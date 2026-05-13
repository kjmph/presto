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
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.DataOrganizationSpecification;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNRowNumberNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.relational.Expressions.not;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * Replaces a self-join against MAX(grouped_aggregate) with a single grouped
 * aggregate and a rank-1 TopN.
 *
 * <pre>
 * - Join(A.value = M.max_value)
 *   - Aggregation(GROUP BY key; value := agg(expr))
 *     - source
 *   - Aggregation(max_value := max(value2))
 *     - Aggregation(GROUP BY key2; value2 := agg(expr2))
 *       - source2
 * </pre>
 *
 * becomes:
 *
 * <pre>
 * - Project(key, value)
 *   - TopNRowNumber(dense_rank <= 1 ORDER BY value DESC)
 *     - Filter(value IS NOT NULL)
 *       - Aggregation(GROUP BY key; value := agg(expr))
 *         - source
 * </pre>
 *
 * The duplicate grouped aggregate is removed only when the two grouped
 * aggregate inputs canonicalize to the same table, grouping columns, source
 * predicate, aggregation function, and aggregation input expression. The
 * non-null filter preserves SQL MAX/equality semantics: if all grouped values
 * are null, the original scalar MAX is null and the equality join returns no
 * rows.
 */
public class TransformMaxAggregationSelfJoinToTopNRank
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = typeOf(JoinNode.class);

    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public TransformMaxAggregationSelfJoinToTopNRank(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return shouldPushAggregationThroughJoin(session);
    }

    @Override
    public Result apply(JoinNode join, Captures captures, Context context)
    {
        if (join.getType() != JoinType.INNER || join.getFilter().isPresent() || join.getCriteria().size() != 1) {
            return Result.empty();
        }

        EquiJoinClause clause = join.getCriteria().get(0);
        Optional<PlanNode> rewritten = tryCreateRewrite(
                join,
                context.getLookup().resolve(join.getLeft()),
                context.getLookup().resolve(join.getRight()),
                clause.getLeft(),
                clause.getRight(),
                context);
        if (!rewritten.isPresent()) {
            rewritten = tryCreateRewrite(
                    join,
                    context.getLookup().resolve(join.getRight()),
                    context.getLookup().resolve(join.getLeft()),
                    clause.getRight(),
                    clause.getLeft(),
                    context);
        }

        return rewritten.map(Result::ofPlanNode).orElseGet(Result::empty);
    }

    private Optional<PlanNode> tryCreateRewrite(
            JoinNode join,
            PlanNode groupedSide,
            PlanNode maxSide,
            VariableReferenceExpression groupedValue,
            VariableReferenceExpression maxValue,
            Context context)
    {
        Optional<GroupedAggregation> groupedAggregation = extractGroupedAggregation(groupedSide, groupedValue, context);
        Optional<GroupedAggregation> maxInputAggregation = extractScalarMaxInput(maxSide, maxValue, context);
        if (!groupedAggregation.isPresent()
                || !maxInputAggregation.isPresent()
                || !groupedAggregation.get().getSignature().equals(maxInputAggregation.get().getSignature())) {
            return Optional.empty();
        }

        RowExpression notNull = not(
                functionAndTypeManager,
                new SpecialFormExpression(groupedValue.getSourceLocation(), IS_NULL, BOOLEAN, groupedValue));
        FilterNode nonNullGroupedAggregation = new FilterNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                groupedAggregation.get().getRoot(),
                notNull);

        VariableReferenceExpression rankVariable = context.getVariableAllocator().newVariable("max_rank", BIGINT);
        TopNRowNumberNode topRank = new TopNRowNumberNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                nonNullGroupedAggregation,
                new DataOrganizationSpecification(
                        emptyList(),
                        Optional.of(new OrderingScheme(ImmutableList.of(new Ordering(groupedValue, SortOrder.DESC_NULLS_LAST))))),
                TopNRowNumberNode.RankingFunction.DENSE_RANK,
                rankVariable,
                1,
                false,
                Optional.empty());

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression output : join.getOutputVariables()) {
            if (groupedSide.getOutputVariables().contains(output)) {
                assignments.put(output, output);
            }
            else if (maxSide.getOutputVariables().contains(output)) {
                assignments.put(output, groupedValue);
            }
            else {
                return Optional.empty();
            }
        }

        return Optional.of(new ProjectNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                topRank,
                assignments.build(),
                ProjectNode.Locality.LOCAL));
    }

    private Optional<GroupedAggregation> extractScalarMaxInput(
            PlanNode root,
            VariableReferenceExpression maxValue,
            Context context)
    {
        PlanNode node = context.getLookup().resolve(root);
        if (node instanceof ProjectNode) {
            Optional<RowExpression> mapped = assignment((ProjectNode) node, maxValue);
            if (!mapped.isPresent() || !(mapped.get() instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            maxValue = (VariableReferenceExpression) mapped.get();
            node = context.getLookup().resolve(((ProjectNode) node).getSource());
        }

        if (node instanceof FilterNode) {
            if (!isNotNullPredicate(((FilterNode) node).getPredicate(), maxValue)) {
                return Optional.empty();
            }
            node = context.getLookup().resolve(((FilterNode) node).getSource());
        }

        if (node instanceof EnforceSingleRowNode) {
            node = context.getLookup().resolve(((EnforceSingleRowNode) node).getSource());
        }

        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregation = (AggregationNode) node;
        if (aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getGroupingSetCount() != 1
                || !aggregation.getGroupingKeys().isEmpty()
                || aggregation.getAggregations().size() != 1
                || !aggregation.getAggregations().containsKey(maxValue)
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()) {
            return Optional.empty();
        }

        AggregationNode.Aggregation max = aggregation.getAggregations().get(maxValue);
        if (max.getArguments().size() != 1
                || max.isDistinct()
                || max.getFilter().isPresent()
                || max.getOrderBy().isPresent()
                || max.getMask().isPresent()
                || !(max.getArguments().get(0) instanceof VariableReferenceExpression)
                || !functionResolution.isMaxFunction(max.getFunctionHandle())) {
            return Optional.empty();
        }

        VariableReferenceExpression groupedValue = (VariableReferenceExpression) max.getArguments().get(0);
        PlanNode groupedRoot = context.getLookup().resolve(aggregation.getSource());
        if (groupedRoot instanceof ProjectNode) {
            Optional<RowExpression> mapped = assignment((ProjectNode) groupedRoot, groupedValue);
            if (!mapped.isPresent() || !(mapped.get() instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            groupedValue = (VariableReferenceExpression) mapped.get();
            groupedRoot = context.getLookup().resolve(((ProjectNode) groupedRoot).getSource());
        }

        return extractGroupedAggregation(groupedRoot, groupedValue, context);
    }

    private Optional<GroupedAggregation> extractGroupedAggregation(
            PlanNode root,
            VariableReferenceExpression aggregateValue,
            Context context)
    {
        PlanNode node = context.getLookup().resolve(root);
        if (node instanceof ProjectNode) {
            Optional<RowExpression> mapped = assignment((ProjectNode) node, aggregateValue);
            if (!mapped.isPresent() || !(mapped.get() instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            aggregateValue = (VariableReferenceExpression) mapped.get();
            node = context.getLookup().resolve(((ProjectNode) node).getSource());
        }

        if (node instanceof FilterNode) {
            if (!isNotNullPredicate(((FilterNode) node).getPredicate(), aggregateValue)) {
                return Optional.empty();
            }
            node = context.getLookup().resolve(((FilterNode) node).getSource());
        }

        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregation = (AggregationNode) node;
        if (aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getGroupingSetCount() != 1
                || aggregation.getGroupingKeys().isEmpty()
                || aggregation.getAggregations().size() != 1
                || !aggregation.getAggregations().containsKey(aggregateValue)
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()) {
            return Optional.empty();
        }

        Optional<AggregateSourceSignature> signature = extractSignature(aggregation, aggregateValue, context);
        if (!signature.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(new GroupedAggregation(root, aggregateValue, signature.get()));
    }

    private Optional<AggregateSourceSignature> extractSignature(
            AggregationNode aggregation,
            VariableReferenceExpression aggregateValue,
            Context context)
    {
        AggregationNode.Aggregation aggregate = aggregation.getAggregations().get(aggregateValue);
        if (aggregate.getArguments().size() != 1
                || aggregate.isDistinct()
                || aggregate.getFilter().isPresent()
                || aggregate.getOrderBy().isPresent()
                || aggregate.getMask().isPresent()
                || !(aggregate.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }

        VariableReferenceExpression aggregateInput = (VariableReferenceExpression) aggregate.getArguments().get(0);
        List<VariableReferenceExpression> groupingKeys = new ArrayList<>(aggregation.getGroupingKeys());
        PlanNode source = context.getLookup().resolve(aggregation.getSource());
        Optional<RowExpression> predicate = Optional.empty();
        Optional<ProjectNode> project = Optional.empty();

        if (source instanceof ProjectNode) {
            project = Optional.of((ProjectNode) source);
            Optional<RowExpression> mappedInput = assignment(project.get(), aggregateInput);
            if (!mappedInput.isPresent()) {
                return Optional.empty();
            }
            source = context.getLookup().resolve(project.get().getSource());
            return extractSignatureFromSource(
                    context,
                    source,
                    project,
                    groupingKeys,
                    mappedInput.get(),
                    predicate,
                    aggregate,
                    aggregateValue);
        }

        return extractSignatureFromSource(
                context,
                source,
                project,
                groupingKeys,
                aggregateInput,
                predicate,
                aggregate,
                aggregateValue);
    }

    private Optional<AggregateSourceSignature> extractSignatureFromSource(
            Context context,
            PlanNode source,
            Optional<ProjectNode> project,
            List<VariableReferenceExpression> groupingKeys,
            RowExpression aggregateInput,
            Optional<RowExpression> predicate,
            AggregationNode.Aggregation aggregate,
            VariableReferenceExpression aggregateValue)
    {
        if (source instanceof FilterNode) {
            predicate = Optional.of(((FilterNode) source).getPredicate());
            source = context.getLookup().resolve(((FilterNode) source).getSource());
        }

        if (!(source instanceof TableScanNode)) {
            return Optional.empty();
        }

        TableScanNode scan = (TableScanNode) source;
        ImmutableList.Builder<ColumnHandle> groupingColumns = ImmutableList.builder();
        for (VariableReferenceExpression groupingKey : groupingKeys) {
            RowExpression mapped = groupingKey;
            if (project.isPresent()) {
                Optional<RowExpression> mappedGroupingKey = assignment(project.get(), groupingKey);
                if (!mappedGroupingKey.isPresent()) {
                    return Optional.empty();
                }
                mapped = mappedGroupingKey.get();
            }
            if (!(mapped instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            ColumnHandle column = scan.getAssignments().get(mapped);
            if (column == null) {
                return Optional.empty();
            }
            groupingColumns.add(column);
        }

        Optional<CanonicalRowExpressionKey> canonicalInput = CanonicalRowExpressionKey.canonicalize(
                aggregateInput,
                functionAndTypeManager,
                scan.getAssignments());
        Optional<ImmutableMultiset<CanonicalRowExpressionKey>> canonicalPredicate = CanonicalRowExpressionKey.canonicalizePredicate(
                predicate,
                scan,
                functionAndTypeManager,
                functionResolution);
        if (!canonicalInput.isPresent() || !canonicalPredicate.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new AggregateSourceSignature(
                scan.getTable(),
                groupingColumns.build(),
                functionAndTypeManager.getFunctionMetadata(aggregate.getFunctionHandle()).getName(),
                aggregateValue.getType().getTypeSignature(),
                canonicalInput.get(),
                canonicalPredicate.get()));
    }

    private Optional<RowExpression> assignment(ProjectNode project, VariableReferenceExpression variable)
    {
        return Optional.ofNullable(project.getAssignments().get(variable));
    }

    private boolean isNotNullPredicate(RowExpression predicate, VariableReferenceExpression variable)
    {
        if (!(predicate instanceof CallExpression)) {
            return false;
        }

        CallExpression call = (CallExpression) predicate;
        if (!functionResolution.isNotFunction(call.getFunctionHandle())
                || call.getArguments().size() != 1
                || !(call.getArguments().get(0) instanceof SpecialFormExpression)) {
            return false;
        }

        SpecialFormExpression isNull = (SpecialFormExpression) call.getArguments().get(0);
        return isNull.getForm() == IS_NULL
                && isNull.getArguments().size() == 1
                && isNull.getArguments().get(0).equals(variable);
    }

    private static class GroupedAggregation
    {
        private final PlanNode root;
        private final VariableReferenceExpression aggregateValue;
        private final AggregateSourceSignature signature;

        private GroupedAggregation(
                PlanNode root,
                VariableReferenceExpression aggregateValue,
                AggregateSourceSignature signature)
        {
            this.root = root;
            this.aggregateValue = aggregateValue;
            this.signature = signature;
        }

        public PlanNode getRoot()
        {
            return root;
        }

        public VariableReferenceExpression getAggregateValue()
        {
            return aggregateValue;
        }

        public AggregateSourceSignature getSignature()
        {
            return signature;
        }
    }

    private static class AggregateSourceSignature
    {
        private final TableHandle table;
        private final List<ColumnHandle> groupingColumns;
        private final QualifiedObjectName functionName;
        private final TypeSignature outputType;
        private final CanonicalRowExpressionKey aggregateInput;
        private final ImmutableMultiset<CanonicalRowExpressionKey> predicate;

        private AggregateSourceSignature(
                TableHandle table,
                List<ColumnHandle> groupingColumns,
                QualifiedObjectName functionName,
                TypeSignature outputType,
                CanonicalRowExpressionKey aggregateInput,
                ImmutableMultiset<CanonicalRowExpressionKey> predicate)
        {
            this.table = table;
            this.groupingColumns = ImmutableList.copyOf(groupingColumns);
            this.functionName = requireNonNull(functionName, "functionName is null");
            this.outputType = requireNonNull(outputType, "outputType is null");
            this.aggregateInput = requireNonNull(aggregateInput, "aggregateInput is null");
            this.predicate = requireNonNull(predicate, "predicate is null");
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            AggregateSourceSignature that = (AggregateSourceSignature) o;
            return Objects.equals(table, that.table) &&
                    Objects.equals(groupingColumns, that.groupingColumns) &&
                    Objects.equals(functionName, that.functionName) &&
                    Objects.equals(outputType, that.outputType) &&
                    Objects.equals(aggregateInput, that.aggregateInput) &&
                    Objects.equals(predicate, that.predicate);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(table, groupingColumns, functionName, outputType, aggregateInput, predicate);
        }
    }
}
