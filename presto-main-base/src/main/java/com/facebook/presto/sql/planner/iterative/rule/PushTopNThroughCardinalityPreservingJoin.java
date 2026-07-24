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
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.matching.Pattern.typeOf;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Pushes a logical or partial TopN below an inner lookup join when trusted
 * constraints prove that the lookup is cardinality preserving.
 *
 * The rule targets shapes like:
 *
 * <pre>
 * - TopN(order by A/O columns)
 *   - Join(A.key = O.key)
 *     - A
 *     - Join(O.lookup_key = L.lookup_key)
 *       - O -- trusted FK to L
 *       - L -- trusted unique/PK lookup key
 * </pre>
 *
 * and rewrites them to:
 *
 * <pre>
 * - Join(O.lookup_key = L.lookup_key)
 *   - TopN(order by A/O columns)
 *     - Join(A.key = O.key)
 *       - A
 *       - O
 *   - L
 * </pre>
 *
 * This is only safe if the lookup join cannot drop or duplicate rows. We prove
 * that using a trusted foreign key on the lookup source plus a trusted
 * unique/primary key on the lookup table. The local FK columns must also be
 * known non-null, otherwise an inner lookup join could filter rows after TopN.
 */
public class PushTopNThroughCardinalityPreservingJoin
        implements Rule<TopNNode>
{
    private static final Pattern<TopNNode> PATTERN = typeOf(TopNNode.class);

    private final Metadata metadata;

    public PushTopNThroughCardinalityPreservingJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TopNNode topN, Captures captures, Context context)
    {
        if (topN.getStep() == TopNNode.Step.FINAL) {
            return Result.empty();
        }

        PlanNode source = context.getLookup().resolve(topN.getSource());
        if (source instanceof ProjectNode) {
            return tryPushThroughProject(topN, (ProjectNode) source, context)
                    .map(Result::ofPlanNode)
                    .orElseGet(Result::empty);
        }

        if (!(source instanceof JoinNode)) {
            return Result.empty();
        }

        return tryPushThroughJoin(topN, (JoinNode) source, context)
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }

    private Optional<PlanNode> tryPushThroughProject(TopNNode topN, ProjectNode project, Context context)
    {
        ImmutableList.Builder<Ordering> orderBy = ImmutableList.builder();
        for (Ordering ordering : topN.getOrderingScheme().getOrderBy()) {
            RowExpression assignment = project.getAssignments().get(ordering.getVariable());
            if (!(assignment instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            orderBy.add(new Ordering((VariableReferenceExpression) assignment, ordering.getSortOrder()));
        }

        TopNNode mappedTopN = new TopNNode(
                topN.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                context.getLookup().resolve(project.getSource()),
                topN.getCount(),
                new OrderingScheme(orderBy.build()),
                topN.getStep());

        PlanNode projectSource = context.getLookup().resolve(mappedTopN.getSource());
        if (!(projectSource instanceof JoinNode)) {
            return Optional.empty();
        }

        Optional<PlanNode> rewritten = tryPushThroughJoin(mappedTopN, (JoinNode) projectSource, context);
        if (!rewritten.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new ProjectNode(
                project.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                rewritten.get(),
                project.getAssignments(),
                project.getLocality()));
    }

    private Optional<PlanNode> tryPushThroughJoin(TopNNode topN, JoinNode rootJoin, Context context)
    {
        if (!isPlainInnerJoin(rootJoin)) {
            return Optional.empty();
        }

        Optional<PlanNode> rewritten = tryPushThroughDirectLookup(topN, rootJoin, true, context);
        if (!rewritten.isPresent()) {
            rewritten = tryPushThroughDirectLookup(topN, rootJoin, false, context);
        }
        if (rewritten.isPresent()) {
            return rewritten;
        }

        rewritten = tryPushThroughNestedLookup(topN, rootJoin, true, context);
        if (!rewritten.isPresent()) {
            rewritten = tryPushThroughNestedLookup(topN, rootJoin, false, context);
        }
        return rewritten;
    }

    private Optional<PlanNode> tryPushThroughDirectLookup(
            TopNNode topN,
            JoinNode join,
            boolean lookupOnLeft,
            Context context)
    {
        Optional<LookupSplit> split = trySplitLookupJoin(join, lookupOnLeft, context.getSession(), context);
        if (!split.isPresent()) {
            return Optional.empty();
        }

        PlanNode baseSide = split.get().getBaseSide();
        PlanNode lookupSide = split.get().getLookupSide();
        if (hasSameTopN(baseSide, topN)) {
            return Optional.empty();
        }
        if (!baseSide.getOutputVariables().containsAll(topN.getOrderingScheme().getOrderByVariables())) {
            return Optional.empty();
        }

        Set<VariableReferenceExpression> lookupOutputs = ImmutableSet.copyOf(lookupSide.getOutputVariables());
        for (VariableReferenceExpression orderingVariable : topN.getOrderingScheme().getOrderByVariables()) {
            if (lookupOutputs.contains(orderingVariable)) {
                return Optional.empty();
            }
        }

        TopNNode pushedTopN = new TopNNode(
                topN.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                baseSide,
                topN.getCount(),
                topN.getOrderingScheme(),
                topN.getStep());

        PlanNode finalLeft = lookupOnLeft ? lookupSide : pushedTopN;
        PlanNode finalRight = lookupOnLeft ? pushedTopN : lookupSide;
        Optional<List<EquiJoinClause>> finalCriteria = orientCriteria(join.getCriteria(), finalLeft, finalRight);
        if (!finalCriteria.isPresent()) {
            return Optional.empty();
        }

        if (!isValidJoinOutputOrder(join.getOutputVariables(), finalLeft, finalRight)) {
            return Optional.empty();
        }

        JoinNode finalJoin = new JoinNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.INNER,
                finalLeft,
                finalRight,
                finalCriteria.get(),
                join.getOutputVariables(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                emptyMap());

        return Optional.of(new TopNNode(
                topN.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                finalJoin,
                topN.getCount(),
                topN.getOrderingScheme(),
                topN.getStep()));
    }

    private boolean hasSameTopN(PlanNode node, TopNNode topN)
    {
        if (!(node instanceof TopNNode)) {
            return false;
        }

        TopNNode existing = (TopNNode) node;
        return existing.getCount() == topN.getCount() &&
                existing.getStep() == topN.getStep() &&
                existing.getOrderingScheme().equals(topN.getOrderingScheme());
    }

    private Optional<PlanNode> tryPushThroughNestedLookup(
            TopNNode topN,
            JoinNode rootJoin,
            boolean nestedOnLeft,
            Context context)
    {
        PlanNode rootLeft = context.getLookup().resolve(rootJoin.getLeft());
        PlanNode rootRight = context.getLookup().resolve(rootJoin.getRight());
        PlanNode nestedCandidate = nestedOnLeft ? rootLeft : rootRight;
        PlanNode otherRootSide = nestedOnLeft ? rootRight : rootLeft;

        if (!(nestedCandidate instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode nestedJoin = (JoinNode) nestedCandidate;
        if (!isPlainInnerJoin(nestedJoin)) {
            return Optional.empty();
        }

        Optional<LookupSplit> split = trySplitLookupJoin(nestedJoin, true, context.getSession(), context);
        if (!split.isPresent()) {
            split = trySplitLookupJoin(nestedJoin, false, context.getSession(), context);
        }
        if (!split.isPresent()) {
            return Optional.empty();
        }

        PlanNode baseSide = split.get().getBaseSide();
        PlanNode lookupSide = split.get().getLookupSide();

        PlanNode earlyLeft = nestedOnLeft ? baseSide : otherRootSide;
        PlanNode earlyRight = nestedOnLeft ? otherRootSide : baseSide;

        Optional<List<EquiJoinClause>> earlyCriteria = orientCriteria(rootJoin.getCriteria(), earlyLeft, earlyRight);
        if (!earlyCriteria.isPresent()) {
            return Optional.empty();
        }

        Set<VariableReferenceExpression> earlyAvailable = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(earlyLeft.getOutputVariables())
                .addAll(earlyRight.getOutputVariables())
                .build();
        if (!earlyAvailable.containsAll(topN.getOrderingScheme().getOrderByVariables())) {
            return Optional.empty();
        }

        Set<VariableReferenceExpression> lookupOutputs = ImmutableSet.copyOf(lookupSide.getOutputVariables());
        for (VariableReferenceExpression orderingVariable : topN.getOrderingScheme().getOrderByVariables()) {
            if (lookupOutputs.contains(orderingVariable)) {
                return Optional.empty();
            }
        }

        LinkedHashSet<VariableReferenceExpression> earlyOutputSet = new LinkedHashSet<>();
        for (VariableReferenceExpression variable : rootJoin.getOutputVariables()) {
            if (!lookupOutputs.contains(variable)) {
                earlyOutputSet.add(variable);
            }
        }
        earlyOutputSet.addAll(topN.getOrderingScheme().getOrderByVariables());
        earlyOutputSet.addAll(split.get().getBaseKeys());

        List<VariableReferenceExpression> earlyOutputs = orderJoinOutputs(earlyLeft, earlyRight, earlyOutputSet);
        if (!earlyOutputs.containsAll(earlyOutputSet)) {
            return Optional.empty();
        }

        JoinNode earlyJoin = new JoinNode(
                rootJoin.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.INNER,
                earlyLeft,
                earlyRight,
                earlyCriteria.get(),
                earlyOutputs,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                emptyMap());

        TopNNode pushedTopN = new TopNNode(
                topN.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                earlyJoin,
                topN.getCount(),
                topN.getOrderingScheme(),
                topN.getStep());

        Optional<List<EquiJoinClause>> finalCriteria = orientCriteria(split.get().getLookupCriteria(), pushedTopN, lookupSide);
        if (!finalCriteria.isPresent()) {
            return Optional.empty();
        }

        if (!isValidJoinOutputOrder(rootJoin.getOutputVariables(), pushedTopN, lookupSide)) {
            return Optional.empty();
        }

        JoinNode finalJoin = new JoinNode(
                nestedJoin.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.INNER,
                pushedTopN,
                lookupSide,
                finalCriteria.get(),
                rootJoin.getOutputVariables(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                emptyMap());

        return Optional.of(new TopNNode(
                topN.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                finalJoin,
                topN.getCount(),
                topN.getOrderingScheme(),
                topN.getStep()));
    }

    private Optional<LookupSplit> trySplitLookupJoin(
            JoinNode join,
            boolean lookupOnLeft,
            Session session,
            Context context)
    {
        PlanNode left = context.getLookup().resolve(join.getLeft());
        PlanNode right = context.getLookup().resolve(join.getRight());
        PlanNode lookupSide = lookupOnLeft ? left : right;
        PlanNode baseSide = lookupOnLeft ? right : left;

        Optional<List<VariableReferenceExpression>> lookupKeys = keysForSide(join.getCriteria(), lookupSide);
        Optional<List<VariableReferenceExpression>> baseKeys = keysForSide(join.getCriteria(), baseSide);
        if (!lookupKeys.isPresent() || !baseKeys.isPresent()) {
            return Optional.empty();
        }

        if (!isKnownUnique(lookupSide, lookupKeys.get(), context)) {
            return Optional.empty();
        }

        if (!hasTrustedForeignKey(baseSide, baseKeys.get(), lookupSide, lookupKeys.get(), session, context)) {
            return Optional.empty();
        }

        return Optional.of(new LookupSplit(baseSide, lookupSide, baseKeys.get(), lookupKeys.get(), join.getCriteria()));
    }

    private boolean hasTrustedForeignKey(
            PlanNode source,
            List<VariableReferenceExpression> sourceKeys,
            PlanNode target,
            List<VariableReferenceExpression> targetKeys,
            Session session,
            Context context)
    {
        Optional<TableKeys> sourceTableKeys = traceSourceKeysToTableKeys(source, sourceKeys, session, context);
        Optional<TableKeys> targetTableKeys = traceToTableKeys(target, targetKeys, session, context);
        if (!sourceTableKeys.isPresent() || !targetTableKeys.isPresent()) {
            return false;
        }

        if (!areKnownNotNull(sourceTableKeys.get(), session)) {
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

    private Optional<TableKeys> traceSourceKeysToTableKeys(
            PlanNode node,
            List<VariableReferenceExpression> keys,
            Session session,
            Context context)
    {
        // The FK source may have been filtered, duplicated by joins, or grouped
        // by the FK columns. Those operations preserve key lineage for proving
        // that the next lookup join cannot drop or duplicate rows.
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
            return traceSourceKeysToTableKeys(project.getSource(), sourceKeys.build(), session, context);
        }

        if (node instanceof FilterNode) {
            return traceSourceKeysToTableKeys(((FilterNode) node).getSource(), keys, session, context);
        }

        if (node instanceof AggregationNode) {
            AggregationNode aggregation = (AggregationNode) node;
            if (aggregation.getGroupingSetCount() != 1 ||
                    aggregation.getGroupIdVariable().isPresent() ||
                    !aggregation.getGroupingKeys().containsAll(keys)) {
                return Optional.empty();
            }
            return traceSourceKeysToTableKeys(aggregation.getSource(), keys, session, context);
        }

        if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            if (join.getType() != JoinType.INNER) {
                return Optional.empty();
            }

            PlanNode left = context.getLookup().resolve(join.getLeft());
            PlanNode right = context.getLookup().resolve(join.getRight());
            boolean keysFromLeft = left.getOutputVariables().containsAll(keys);
            boolean keysFromRight = right.getOutputVariables().containsAll(keys);
            if (keysFromLeft == keysFromRight) {
                return Optional.empty();
            }
            return traceSourceKeysToTableKeys(keysFromLeft ? left : right, keys, session, context);
        }

        return traceToTableKeys(node, keys, session, context);
    }

    private Optional<TableKeys> traceToTableKeys(
            PlanNode node,
            List<VariableReferenceExpression> keys,
            Session session,
            Context context)
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
            return traceToTableKeys(project.getSource(), sourceKeys.build(), session, context);
        }

        if (node instanceof FilterNode) {
            return traceToTableKeys(((FilterNode) node).getSource(), keys, session, context);
        }

        if (!(node instanceof TableScanNode)) {
            return Optional.empty();
        }

        TableScanNode scan = (TableScanNode) node;
        ImmutableList.Builder<ColumnHandle> columns = ImmutableList.builder();
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        for (VariableReferenceExpression key : keys) {
            ColumnHandle column = scan.getAssignments().get(key);
            if (column == null) {
                return Optional.empty();
            }
            columns.add(column);
            columnNames.add(metadata.getColumnMetadata(session, scan.getTable(), column).getName());
        }

        TableMetadata tableMetadata = metadata.getTableMetadata(session, scan.getTable());
        return Optional.of(new TableKeys(scan, tableMetadata.getTable(), columns.build(), columnNames.build()));
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

    private boolean isKnownUnique(PlanNode node, List<VariableReferenceExpression> variables, Context context)
    {
        node = context.getLookup().resolve(node);
        if (variables.isEmpty() || !node.getOutputVariables().containsAll(variables)) {
            return false;
        }

        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            ImmutableList.Builder<VariableReferenceExpression> sourceVariables = ImmutableList.builder();
            for (VariableReferenceExpression variable : variables) {
                RowExpression assignment = project.getAssignments().get(variable);
                if (!(assignment instanceof VariableReferenceExpression)) {
                    return false;
                }
                sourceVariables.add((VariableReferenceExpression) assignment);
            }
            return isKnownUnique(project.getSource(), sourceVariables.build(), context);
        }

        if (node instanceof FilterNode) {
            return isKnownUnique(((FilterNode) node).getSource(), variables, context);
        }

        if (node instanceof TableScanNode) {
            TableScanNode scan = (TableScanNode) node;
            LinkedHashSet<ColumnHandle> columns = new LinkedHashSet<>();
            for (VariableReferenceExpression variable : variables) {
                ColumnHandle column = scan.getAssignments().get(variable);
                if (column == null) {
                    return false;
                }
                columns.add(column);
            }
            return scan.getTableConstraints().stream()
                    .anyMatch(constraint -> (constraint.isEnabled() || constraint.isRely()) &&
                            (constraint instanceof PrimaryKeyConstraint || constraint instanceof UniqueConstraint) &&
                            constraint.getColumns().size() == columns.size() &&
                            constraint.getColumns().containsAll(columns));
        }

        return false;
    }

    private Optional<List<VariableReferenceExpression>> keysForSide(List<EquiJoinClause> criteria, PlanNode side)
    {
        ImmutableList.Builder<VariableReferenceExpression> keys = ImmutableList.builder();
        for (EquiJoinClause clause : criteria) {
            if (side.getOutputVariables().contains(clause.getLeft())) {
                keys.add(clause.getLeft());
            }
            else if (side.getOutputVariables().contains(clause.getRight())) {
                keys.add(clause.getRight());
            }
            else {
                return Optional.empty();
            }
        }
        return Optional.of(keys.build());
    }

    private Optional<List<EquiJoinClause>> orientCriteria(List<EquiJoinClause> criteria, PlanNode left, PlanNode right)
    {
        ImmutableList.Builder<EquiJoinClause> oriented = ImmutableList.builder();
        for (EquiJoinClause clause : criteria) {
            if (left.getOutputVariables().contains(clause.getLeft()) && right.getOutputVariables().contains(clause.getRight())) {
                oriented.add(clause);
            }
            else if (left.getOutputVariables().contains(clause.getRight()) && right.getOutputVariables().contains(clause.getLeft())) {
                oriented.add(clause.flip());
            }
            else {
                return Optional.empty();
            }
        }
        return Optional.of(oriented.build());
    }

    private List<VariableReferenceExpression> orderJoinOutputs(
            PlanNode left,
            PlanNode right,
            Set<VariableReferenceExpression> requiredOutputs)
    {
        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        for (VariableReferenceExpression variable : left.getOutputVariables()) {
            if (requiredOutputs.contains(variable)) {
                outputs.add(variable);
            }
        }
        for (VariableReferenceExpression variable : right.getOutputVariables()) {
            if (requiredOutputs.contains(variable)) {
                outputs.add(variable);
            }
        }
        return outputs.build();
    }

    private boolean isValidJoinOutputOrder(List<VariableReferenceExpression> outputs, PlanNode left, PlanNode right)
    {
        Set<VariableReferenceExpression> leftVariables = ImmutableSet.copyOf(left.getOutputVariables());
        Set<VariableReferenceExpression> rightVariables = ImmutableSet.copyOf(right.getOutputVariables());
        boolean sawRight = false;
        for (VariableReferenceExpression output : outputs) {
            if (rightVariables.contains(output)) {
                sawRight = true;
            }
            else if (leftVariables.contains(output) && sawRight) {
                return false;
            }
            else if (!leftVariables.contains(output)) {
                return false;
            }
        }
        return true;
    }

    private boolean isPlainInnerJoin(JoinNode join)
    {
        return join.getType() == JoinType.INNER &&
                !join.getCriteria().isEmpty() &&
                !join.getFilter().isPresent() &&
                !join.getLeftHashVariable().isPresent() &&
                !join.getRightHashVariable().isPresent() &&
                join.getDynamicFilters().isEmpty();
    }

    private static class LookupSplit
    {
        private final PlanNode baseSide;
        private final PlanNode lookupSide;
        private final List<VariableReferenceExpression> baseKeys;
        private final List<VariableReferenceExpression> lookupKeys;
        private final List<EquiJoinClause> lookupCriteria;

        private LookupSplit(
                PlanNode baseSide,
                PlanNode lookupSide,
                List<VariableReferenceExpression> baseKeys,
                List<VariableReferenceExpression> lookupKeys,
                List<EquiJoinClause> lookupCriteria)
        {
            this.baseSide = requireNonNull(baseSide, "baseSide is null");
            this.lookupSide = requireNonNull(lookupSide, "lookupSide is null");
            this.baseKeys = ImmutableList.copyOf(requireNonNull(baseKeys, "baseKeys is null"));
            this.lookupKeys = ImmutableList.copyOf(requireNonNull(lookupKeys, "lookupKeys is null"));
            this.lookupCriteria = ImmutableList.copyOf(requireNonNull(lookupCriteria, "lookupCriteria is null"));
        }

        public PlanNode getBaseSide()
        {
            return baseSide;
        }

        public PlanNode getLookupSide()
        {
            return lookupSide;
        }

        public List<VariableReferenceExpression> getBaseKeys()
        {
            return baseKeys;
        }

        public List<VariableReferenceExpression> getLookupKeys()
        {
            return lookupKeys;
        }

        public List<EquiJoinClause> getLookupCriteria()
        {
            return lookupCriteria;
        }
    }

    private static class TableKeys
    {
        private final TableScanNode scan;
        private final SchemaTableName tableName;
        private final List<ColumnHandle> columns;
        private final List<String> columnNames;

        private TableKeys(
                TableScanNode scan,
                SchemaTableName tableName,
                List<ColumnHandle> columns,
                List<String> columnNames)
        {
            this.scan = requireNonNull(scan, "scan is null");
            this.tableName = requireNonNull(tableName, "tableName is null");
            this.columns = ImmutableList.copyOf(requireNonNull(columns, "columns is null"));
            this.columnNames = ImmutableList.copyOf(requireNonNull(columnNames, "columnNames is null"));
        }

        public TableScanNode getScan()
        {
            return scan;
        }

        public SchemaTableName getTableName()
        {
            return tableName;
        }

        public List<ColumnHandle> getColumns()
        {
            return columns;
        }

        public List<String> getColumnNames()
        {
            return columnNames;
        }
    }
}
