/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.constraints.ForeignKeyConstraint;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isExploitConstraints;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.util.Objects.requireNonNull;

/**
 * Carries trusted join-key facts through the plan for native execution.
 *
 * Uniqueness facts are derived from LogicalProperties, which in turn only use
 * connector/table constraints that the planner already trusts. Non-null facts
 * are derived conservatively from enabled or RELY NOT NULL / primary-key
 * constraints through identity projections and filters. FK coverage facts are
 * derived from trusted foreign keys where the source keys are non-null and the
 * referenced side is not filtered. This rule does not change join semantics or
 * shape; it annotates join keys so downstream engines can expose metrics and
 * pick specialized physical implementations.
 */
public class AnnotateJoinNodeWithUniqueKeys
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();
    private final Metadata metadata;
    private final FunctionResolution functionResolution;

    public AnnotateJoinNodeWithUniqueKeys(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionResolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isExploitConstraints(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (!isExploitConstraints(context.getSession())) {
            return Result.empty();
        }

        if (joinNode.getCriteria().isEmpty()) {
            return Result.empty();
        }

        boolean leftKeysUnique = areJoinKeysUnique(joinNode.getLeft(), getLeftJoinKeys(joinNode));
        boolean rightKeysUnique = areJoinKeysUnique(joinNode.getRight(), getRightJoinKeys(joinNode));
        boolean leftKeysNonNull = areJoinKeysNonNull(joinNode.getLeft(), getLeftJoinKeys(joinNode), context);
        boolean rightKeysNonNull = areJoinKeysNonNull(joinNode.getRight(), getRightJoinKeys(joinNode), context);
        boolean leftKeysCoveredByRightKeys = hasTrustedForeignKey(
                joinNode,
                joinNode.getLeft(),
                getLeftJoinKeysList(joinNode),
                joinNode.getRight(),
                getRightJoinKeysList(joinNode),
                context);
        boolean rightKeysCoveredByLeftKeys = hasTrustedForeignKey(
                joinNode,
                joinNode.getRight(),
                getRightJoinKeysList(joinNode),
                joinNode.getLeft(),
                getLeftJoinKeysList(joinNode),
                context);

        if (joinNode.isLeftKeysUnique() == leftKeysUnique &&
                joinNode.isRightKeysUnique() == rightKeysUnique &&
                joinNode.isLeftKeysNonNull() == leftKeysNonNull &&
                joinNode.isRightKeysNonNull() == rightKeysNonNull &&
                joinNode.isLeftKeysCoveredByRightKeys() == leftKeysCoveredByRightKeys &&
                joinNode.isRightKeysCoveredByLeftKeys() == rightKeysCoveredByLeftKeys) {
            return Result.empty();
        }

        return Result.ofPlanNode(joinNode.withKeyProperties(
                leftKeysUnique,
                rightKeysUnique,
                leftKeysNonNull,
                rightKeysNonNull,
                leftKeysCoveredByRightKeys,
                rightKeysCoveredByLeftKeys));
    }

    static boolean areJoinKeysUnique(PlanNode source, Set<VariableReferenceExpression> joinKeys)
    {
        if (joinKeys.isEmpty() || !(source instanceof GroupReference)) {
            return false;
        }

        return ((GroupReference) source).getLogicalProperties()
                .map(logicalProperties -> logicalProperties.isDistinct(joinKeys))
                .orElse(false);
    }

    static boolean areJoinKeysNonNull(PlanNode source, Set<VariableReferenceExpression> joinKeys, Context context)
    {
        if (joinKeys.isEmpty()) {
            return false;
        }

        return joinKeys.stream().allMatch(joinKey -> isKnownNonNull(source, joinKey, context));
    }

    private static boolean isKnownNonNull(PlanNode source, VariableReferenceExpression variable, Context context)
    {
        PlanNode resolved = context.getLookup().resolve(source);
        if (!resolved.getOutputVariables().contains(variable)) {
            return false;
        }

        if (resolved instanceof FilterNode) {
            return isKnownNonNull(((FilterNode) resolved).getSource(), variable, context);
        }

        if (resolved instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) resolved;
            RowExpression assignment = project.getAssignments().get(variable);
            return assignment instanceof VariableReferenceExpression &&
                    isKnownNonNull(project.getSource(), (VariableReferenceExpression) assignment, context);
        }

        if (resolved instanceof ExchangeNode) {
            return isKnownNonNullThroughExchange((ExchangeNode) resolved, variable, context);
        }

        if (resolved instanceof JoinNode) {
            return isKnownNonNullThroughJoin((JoinNode) resolved, variable, context);
        }

        if (resolved instanceof TableScanNode) {
            TableScanNode tableScan = (TableScanNode) resolved;
            ColumnHandle column = tableScan.getAssignments().get(variable);
            if (column == null) {
                return false;
            }

            return tableScan.getTableConstraints().stream()
                    .filter(constraint -> (constraint instanceof NotNullConstraint || constraint instanceof PrimaryKeyConstraint) &&
                            (constraint.isEnabled() || constraint.isRely()))
                    .anyMatch(constraint -> constraint.getColumns().contains(column));
        }

        return false;
    }

    private static boolean isKnownNonNullThroughExchange(ExchangeNode exchange, VariableReferenceExpression variable, Context context)
    {
        int outputIndex = exchange.getOutputVariables().indexOf(variable);
        if (outputIndex < 0) {
            return false;
        }

        return IntStream.range(0, exchange.getSources().size())
                .allMatch(sourceIndex -> isKnownNonNull(
                        exchange.getSources().get(sourceIndex),
                        exchange.getInputs().get(sourceIndex).get(outputIndex),
                        context));
    }

    private static boolean isKnownNonNullThroughJoin(JoinNode join, VariableReferenceExpression variable, Context context)
    {
        if (join.getLeft().getOutputVariables().contains(variable)) {
            switch (join.getType()) {
                case INNER:
                case LEFT:
                    return isKnownNonNull(join.getLeft(), variable, context);
                default:
                    return false;
            }
        }

        if (join.getRight().getOutputVariables().contains(variable)) {
            switch (join.getType()) {
                case INNER:
                case RIGHT:
                    return isKnownNonNull(join.getRight(), variable, context);
                default:
                    return false;
            }
        }

        return false;
    }

    private static Set<VariableReferenceExpression> getLeftJoinKeys(JoinNode joinNode)
    {
        return ImmutableSet.copyOf(getLeftJoinKeysList(joinNode));
    }

    private static Set<VariableReferenceExpression> getRightJoinKeys(JoinNode joinNode)
    {
        return ImmutableSet.copyOf(getRightJoinKeysList(joinNode));
    }

    private static List<VariableReferenceExpression> getLeftJoinKeysList(JoinNode joinNode)
    {
        return joinNode.getCriteria().stream()
                .map(EquiJoinClause::getLeft)
                .collect(ImmutableList.toImmutableList());
    }

    private static List<VariableReferenceExpression> getRightJoinKeysList(JoinNode joinNode)
    {
        return joinNode.getCriteria().stream()
                .map(EquiJoinClause::getRight)
                .collect(ImmutableList.toImmutableList());
    }

    private boolean hasTrustedForeignKey(
            JoinNode joinNode,
            PlanNode source,
            List<VariableReferenceExpression> sourceKeys,
            PlanNode target,
            List<VariableReferenceExpression> targetKeys,
            Context context)
    {
        if (joinNode.getType() != JoinType.INNER || joinNode.getFilter().isPresent()) {
            return false;
        }

        Optional<TableKeys> sourceTableKeys = traceToTableKeys(source, sourceKeys, context, true);
        Optional<TableKeys> targetTableKeys = traceToTableKeys(target, targetKeys, context, false);
        if (!sourceTableKeys.isPresent() || !targetTableKeys.isPresent()) {
            return false;
        }

        if (!areKnownNotNull(sourceTableKeys.get(), context.getSession())) {
            return false;
        }

        for (TableConstraint<ColumnHandle> constraint : sourceTableKeys.get().getScan().getTableConstraints()) {
            if (!(constraint instanceof ForeignKeyConstraint) || !(constraint.isEnabled() || constraint.isRely())) {
                continue;
            }

            ForeignKeyConstraint<ColumnHandle> foreignKey = (ForeignKeyConstraint<ColumnHandle>) constraint;
            if (ImmutableList.copyOf(foreignKey.getColumns()).equals(sourceTableKeys.get().getColumns()) &&
                    foreignKey.getReferencedTable().equals(targetTableKeys.get().getTableName()) &&
                    ImmutableList.copyOf(foreignKey.getReferencedColumns()).equals(targetTableKeys.get().getColumnNames())) {
                return true;
            }
        }
        return false;
    }

    private Optional<TableKeys> traceToTableKeys(
            PlanNode node,
            List<VariableReferenceExpression> keys,
            Context context,
            boolean allowFiltering)
    {
        node = context.getLookup().resolve(node);
        if (keys.isEmpty() || !node.getOutputVariables().containsAll(keys)) {
            return Optional.empty();
        }

        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            ImmutableList.Builder<VariableReferenceExpression> sourceKeys = ImmutableList.builder();
            for (VariableReferenceExpression key : keys) {
                RowExpression assignment = project.getAssignments().get(key);
                if (!(assignment instanceof VariableReferenceExpression)) {
                    return Optional.empty();
                }
                sourceKeys.add((VariableReferenceExpression) assignment);
            }
            return traceToTableKeys(project.getSource(), sourceKeys.build(), context, allowFiltering);
        }

        if (node instanceof FilterNode) {
            FilterNode filter = (FilterNode) node;
            if (!allowFiltering && !isRedundantNotNullFilter(filter, context)) {
                return Optional.empty();
            }
            return traceToTableKeys(filter.getSource(), keys, context, allowFiltering);
        }

        if (node instanceof ExchangeNode) {
            ExchangeNode exchange = (ExchangeNode) node;
            if (exchange.getSources().size() != 1) {
                return Optional.empty();
            }
            ImmutableList.Builder<VariableReferenceExpression> sourceKeys = ImmutableList.builder();
            for (VariableReferenceExpression key : keys) {
                int outputIndex = exchange.getOutputVariables().indexOf(key);
                if (outputIndex < 0) {
                    return Optional.empty();
                }
                sourceKeys.add(exchange.getInputs().get(0).get(outputIndex));
            }
            return traceToTableKeys(exchange.getSources().get(0), sourceKeys.build(), context, allowFiltering);
        }

        if (node instanceof JoinNode) {
            return traceToTableKeysThroughJoin((JoinNode) node, keys, context, allowFiltering);
        }

        if (!(node instanceof TableScanNode)) {
            return Optional.empty();
        }

        TableScanNode scan = (TableScanNode) node;
        if (!allowFiltering && isConstrainedScan(scan)) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> columns = ImmutableList.builder();
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        for (VariableReferenceExpression key : keys) {
            ColumnHandle column = scan.getAssignments().get(key);
            if (column == null) {
                return Optional.empty();
            }
            columns.add(column);
            columnNames.add(metadata.getColumnMetadata(context.getSession(), scan.getTable(), column).getName());
        }

        TableMetadata tableMetadata = metadata.getTableMetadata(context.getSession(), scan.getTable());
        return Optional.of(new TableKeys(scan, tableMetadata.getTable(), columns.build(), columnNames.build()));
    }

    private Optional<TableKeys> traceToTableKeysThroughJoin(
            JoinNode join,
            List<VariableReferenceExpression> keys,
            Context context,
            boolean allowFiltering)
    {
        if (!allowFiltering) {
            return traceToTableKeysThroughCardinalityPreservingJoin(join, keys, context);
        }

        boolean keysFromLeft = join.getLeft().getOutputVariables().containsAll(keys);
        boolean keysFromRight = join.getRight().getOutputVariables().containsAll(keys);
        if (keysFromLeft == keysFromRight) {
            return Optional.empty();
        }

        if (keysFromLeft) {
            switch (join.getType()) {
                case INNER:
                case LEFT:
                    return traceToTableKeys(join.getLeft(), keys, context, true);
                default:
                    return Optional.empty();
            }
        }

        switch (join.getType()) {
            case INNER:
            case RIGHT:
                return traceToTableKeys(join.getRight(), keys, context, true);
            default:
                return Optional.empty();
        }
    }

    private Optional<TableKeys> traceToTableKeysThroughCardinalityPreservingJoin(
            JoinNode join,
            List<VariableReferenceExpression> keys,
            Context context)
    {
        if (join.getType() != JoinType.INNER || join.getFilter().isPresent()) {
            return Optional.empty();
        }

        boolean keysFromLeft = join.getLeft().getOutputVariables().containsAll(keys);
        boolean keysFromRight = join.getRight().getOutputVariables().containsAll(keys);
        if (keysFromLeft == keysFromRight) {
            return Optional.empty();
        }

        if (keysFromLeft) {
            if (!hasTrustedForeignKey(join, join.getLeft(), getLeftJoinKeysList(join), join.getRight(), getRightJoinKeysList(join), context)) {
                return Optional.empty();
            }
            return traceToTableKeys(join.getLeft(), keys, context, false);
        }

        if (!hasTrustedForeignKey(join, join.getRight(), getRightJoinKeysList(join), join.getLeft(), getLeftJoinKeysList(join), context)) {
            return Optional.empty();
        }
        return traceToTableKeys(join.getRight(), keys, context, false);
    }

    private boolean isRedundantNotNullFilter(FilterNode filter, Context context)
    {
        PlanNode source = context.getLookup().resolve(filter.getSource());
        if (!(source instanceof TableScanNode)) {
            return false;
        }

        TableScanNode scan = (TableScanNode) source;
        for (RowExpression conjunct : extractConjuncts(filter.getPredicate())) {
            Optional<VariableReferenceExpression> variable = extractIsNotNullVariable(conjunct);
            if (!variable.isPresent()) {
                return false;
            }

            ColumnHandle column = scan.getAssignments().get(variable.get());
            if (column == null || !isKnownNotNull(scan, column, context.getSession())) {
                return false;
            }
        }
        return true;
    }

    private Optional<VariableReferenceExpression> extractIsNotNullVariable(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression call = (CallExpression) expression;
        if (!functionResolution.isNotFunction(call.getFunctionHandle()) ||
                call.getArguments().size() != 1 ||
                !(call.getArguments().get(0) instanceof SpecialFormExpression)) {
            return Optional.empty();
        }

        SpecialFormExpression isNull = (SpecialFormExpression) call.getArguments().get(0);
        if (isNull.getForm() != IS_NULL ||
                isNull.getArguments().size() != 1 ||
                !(isNull.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }

        return Optional.of((VariableReferenceExpression) isNull.getArguments().get(0));
    }

    private static boolean isConstrainedScan(TableScanNode scan)
    {
        return (scan.getCurrentConstraint() != null && !scan.getCurrentConstraint().isAll()) ||
                !scan.getEnforcedConstraint().isAll();
    }

    private boolean areKnownNotNull(TableKeys tableKeys, Session session)
    {
        for (ColumnHandle column : tableKeys.getColumns()) {
            if (!isKnownNotNull(tableKeys.getScan(), column, session)) {
                return false;
            }
        }
        return true;
    }

    private boolean isKnownNotNull(TableScanNode scan, ColumnHandle column, Session session)
    {
        if (!metadata.getColumnMetadata(session, scan.getTable(), column).isNullable()) {
            return true;
        }

        return scan.getTableConstraints().stream()
                .anyMatch(constraint -> (constraint.isEnabled() || constraint.isRely()) &&
                        (constraint instanceof NotNullConstraint || constraint instanceof PrimaryKeyConstraint) &&
                        constraint.getColumns().contains(column));
    }

    private static class TableKeys
    {
        private final TableScanNode scan;
        private final SchemaTableName tableName;
        private final List<ColumnHandle> columns;
        private final List<String> columnNames;

        private TableKeys(TableScanNode scan, SchemaTableName tableName, List<ColumnHandle> columns, List<String> columnNames)
        {
            this.scan = scan;
            this.tableName = tableName;
            this.columns = ImmutableList.copyOf(columns);
            this.columnNames = ImmutableList.copyOf(columnNames);
        }

        private TableScanNode getScan()
        {
            return scan;
        }

        private SchemaTableName getTableName()
        {
            return tableName;
        }

        private List<ColumnHandle> getColumns()
        {
            return columns;
        }

        private List<String> getColumnNames()
        {
            return columnNames;
        }
    }
}
