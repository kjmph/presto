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
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getJoinMaxBroadcastTableSize;
import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.isNaN;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

/**
 * Reduces a grouped aggregation input using a selective join-key source before
 * the aggregation is computed.
 *
 * <pre>
 * - Join(A.key = B.key)
 *   - Aggregation(GROUP BY A.key)
 *     - A
 *   - B
 * </pre>
 *
 * becomes:
 *
 * <pre>
 * - Join(A.key = B.key)
 *   - Aggregation(GROUP BY A.key)
 *     - Filter(marker)
 *       - SemiJoin(A.key = B_copy.key)
 *         - A
 *         - B_copy
 *   - B
 * </pre>
 *
 * The original join is preserved, so duplicate rows from B keep their original
 * multiplicity. B is cloned with fresh symbols before being used by the
 * semi-join; sharing the same symbols across both uses allows later unaliasing
 * and projection pruning passes to construct invalid plans.
 *
 * This rule must not feed duplicate approximate aggregate results into exact
 * equality predicates. Even when the row set is unchanged, a different physical
 * path can change the last bits of REAL/DOUBLE SUM/AVG. Exact numeric aggregate
 * outputs are safe; approximate AVG is allowed only when this join is filtering
 * by the grouping key and not comparing the AVG result itself.
 */
public class PushJoinKeyFilterBelowAggregation
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = typeOf(JoinNode.class);
    private static final double MIN_SELECTIVITY_GAIN = 10.0;

    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;
    private final TaskCountEstimator taskCountEstimator;

    public PushJoinKeyFilterBelowAggregation(FunctionAndTypeManager functionAndTypeManager, TaskCountEstimator taskCountEstimator)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
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

        Optional<Rewrite> rewrite = tryCreateRewrite(
                join,
                context.getLookup().resolve(join.getLeft()),
                context.getLookup().resolve(join.getRight()),
                join.getCriteria().get(0),
                true,
                context);
        if (!rewrite.isPresent()) {
            rewrite = tryCreateRewrite(
                    join,
                    context.getLookup().resolve(join.getRight()),
                    context.getLookup().resolve(join.getLeft()),
                    join.getCriteria().get(0),
                    false,
                    context);
        }
        return rewrite.map(value -> Result.ofPlanNode(value.getJoin())).orElseGet(Result::empty);
    }

    private Optional<Rewrite> tryCreateRewrite(
            JoinNode join,
            PlanNode aggregationSide,
            PlanNode filteringSide,
            EquiJoinClause clause,
            boolean aggregationOnLeft,
            Context context)
    {
        Optional<FilteredAggregation> filteredAggregation = getFilteredAggregation(aggregationSide, context);
        if (!filteredAggregation.isPresent()) {
            return Optional.empty();
        }

        AggregationNode aggregation = filteredAggregation.get().getAggregation();
        VariableReferenceExpression aggregationJoinKey = aggregationOnLeft ? clause.getLeft() : clause.getRight();
        VariableReferenceExpression filteringJoinKey = aggregationOnLeft ? clause.getRight() : clause.getLeft();

        if (!isSupportedAggregation(aggregation, aggregationJoinKey, join)
                || !filteringSide.getOutputVariables().contains(filteringJoinKey)
                || containsSemiJoin(aggregation.getSource(), context)
                || !isFilteringSourceSelective(aggregation.getSource(), filteringSide, context)
                || !isFilteringSourceCheapToClone(filteringSide, context)) {
            return Optional.empty();
        }

        ClonedPlan clonedFilteringSide;
        try {
            clonedFilteringSide = cloneWithFreshSymbols(filteringSide, context);
        }
        catch (UnsupportedOperationException ignored) {
            return Optional.empty();
        }

        Optional<VariableReferenceExpression> clonedFilteringJoinKey = clonedFilteringSide.getMappedVariable(filteringJoinKey);
        if (!clonedFilteringJoinKey.isPresent()) {
            return Optional.empty();
        }

        VariableReferenceExpression semiJoinOutput = context.getVariableAllocator().newVariable("aggregation_key_present", BooleanType.BOOLEAN);
        SemiJoinNode semiJoin = new SemiJoinNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                aggregation.getSource(),
                clonedFilteringSide.getPlan(),
                aggregationJoinKey,
                clonedFilteringJoinKey.get(),
                semiJoinOutput,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        FilterNode filteredAggregationSource = new FilterNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                semiJoin,
                semiJoinOutput);

        AggregationNode rewrittenAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                filteredAggregationSource,
                aggregation.getAggregations(),
                aggregation.getGroupingSets(),
                aggregation.getPreGroupedVariables(),
                aggregation.getStep(),
                aggregation.getHashVariable(),
                aggregation.getGroupIdVariable(),
                aggregation.getAggregationId());

        PlanNode rewrittenAggregationSide = filteredAggregation.get().withAggregation(rewrittenAggregation, context);

        JoinNode rewrittenJoin = new JoinNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                join.getType(),
                aggregationOnLeft ? rewrittenAggregationSide : filteringSide,
                aggregationOnLeft ? filteringSide : rewrittenAggregationSide,
                join.getCriteria(),
                join.getOutputVariables(),
                join.getFilter(),
                join.getLeftHashVariable(),
                join.getRightHashVariable(),
                join.getDistributionType(),
                join.getDynamicFilters());

        return Optional.of(new Rewrite(rewrittenJoin));
    }

    private Optional<FilteredAggregation> getFilteredAggregation(PlanNode node, Context context)
    {
        if (node instanceof AggregationNode) {
            return Optional.of(new FilteredAggregation((AggregationNode) node, Optional.empty()));
        }

        if (node instanceof FilterNode) {
            FilterNode filter = (FilterNode) node;
            PlanNode source = context.getLookup().resolve(filter.getSource());
            if (source instanceof AggregationNode) {
                return Optional.of(new FilteredAggregation((AggregationNode) source, Optional.of(filter)));
            }
        }

        return Optional.empty();
    }

    private boolean isSupportedAggregation(AggregationNode aggregation, VariableReferenceExpression joinKey, JoinNode join)
    {
        Set<VariableReferenceExpression> joinConditionVariables = getJoinConditionVariables(join);
        return aggregation.getStep() == AggregationNode.Step.SINGLE
                && aggregation.getGroupingSetCount() == 1
                && aggregation.getGroupingKeys().contains(joinKey)
                && aggregation.getSource().getOutputVariables().contains(joinKey)
                && !aggregation.getHashVariable().isPresent()
                && !aggregation.getGroupIdVariable().isPresent()
                && !aggregation.getAggregationId().isPresent()
                && aggregation.getAggregations().entrySet().stream()
                        .allMatch(entry -> isSafeAggregation(entry.getKey(), entry.getValue(), joinConditionVariables));
    }

    private Set<VariableReferenceExpression> getJoinConditionVariables(JoinNode join)
    {
        ImmutableSet.Builder<VariableReferenceExpression> variables = ImmutableSet.builder();
        join.getCriteria().forEach(clause -> variables.add(clause.getLeft(), clause.getRight()));
        join.getFilter().ifPresent(filter -> variables.addAll(VariablesExtractor.extractUnique(filter)));
        return variables.build();
    }

    private boolean isSafeAggregation(
            VariableReferenceExpression output,
            AggregationNode.Aggregation aggregation,
            Set<VariableReferenceExpression> joinConditionVariables)
    {
        if (aggregation.isDistinct()
                || aggregation.getFilter().isPresent()
                || aggregation.getOrderBy().isPresent()
                || aggregation.getMask().isPresent()) {
            return false;
        }

        if (functionResolution.isCountFunction(aggregation.getFunctionHandle())
                || functionResolution.isCountIfFunction(aggregation.getFunctionHandle())
                || functionResolution.isMinFunction(aggregation.getFunctionHandle())
                || functionResolution.isMaxFunction(aggregation.getFunctionHandle())) {
            return true;
        }

        if (!isApproximateType(output.getType())) {
            return isFunction(aggregation, "sum") || isFunction(aggregation, "avg");
        }

        return isFunction(aggregation, "avg") && !joinConditionVariables.contains(output);
    }

    private boolean isFunction(AggregationNode.Aggregation aggregation, String name)
    {
        return functionAndTypeManager.getFunctionMetadata(aggregation.getFunctionHandle()).getName().getObjectName().equals(name);
    }

    private static boolean isApproximateType(Type type)
    {
        return type.equals(DOUBLE) || type.equals(REAL);
    }

    private boolean isFilteringSourceSelective(PlanNode aggregationSource, PlanNode filteringSource, Context context)
    {
        PlanNodeStatsEstimate aggregationSourceStats = context.getStatsProvider().getStats(aggregationSource);
        PlanNodeStatsEstimate filteringSourceStats = context.getStatsProvider().getStats(filteringSource);
        double aggregationSourceRows = aggregationSourceStats.getOutputRowCount();
        double filteringSourceRows = filteringSourceStats.getOutputRowCount();
        if (isNaN(aggregationSourceRows) || isNaN(filteringSourceRows) || filteringSourceRows <= 0) {
            return false;
        }
        return aggregationSourceRows / filteringSourceRows >= MIN_SELECTIVITY_GAIN;
    }

    private boolean isFilteringSourceCheapToClone(PlanNode filteringSource, Context context)
    {
        PlanNodeStatsEstimate filteringSourceStats = context.getStatsProvider().getStats(filteringSource);
        double filteringSourceSizeInBytes = filteringSourceStats.getOutputSizeInBytes(filteringSource);
        if (isNaN(filteringSourceSizeInBytes)) {
            return false;
        }

        double replicatedFilteringSourceSizeInBytes = filteringSourceSizeInBytes * max(1, taskCountEstimator.estimateSourceDistributedTaskCount());
        return replicatedFilteringSourceSizeInBytes <= getJoinMaxBroadcastTableSize(context.getSession()).toBytes();
    }

    private boolean containsSemiJoin(PlanNode node, Context context)
    {
        return PlanNodeSearcher.searchFrom(node, context.getLookup())
                .where(SemiJoinNode.class::isInstance)
                .findFirst()
                .isPresent();
    }

    private ClonedPlan cloneWithFreshSymbols(PlanNode node, Context context)
    {
        FreshSymbolCloner cloner = new FreshSymbolCloner(context);
        PlanNode cloned = SimplePlanRewriter.rewriteWith(cloner, node);
        return new ClonedPlan(cloned, cloner.getMappings());
    }

    private static class Rewrite
    {
        private final JoinNode join;

        private Rewrite(JoinNode join)
        {
            this.join = join;
        }

        private JoinNode getJoin()
        {
            return join;
        }
    }

    private static class FilteredAggregation
    {
        private final AggregationNode aggregation;
        private final Optional<FilterNode> filter;

        private FilteredAggregation(AggregationNode aggregation, Optional<FilterNode> filter)
        {
            this.aggregation = aggregation;
            this.filter = filter;
        }

        private AggregationNode getAggregation()
        {
            return aggregation;
        }

        private PlanNode withAggregation(AggregationNode rewrittenAggregation, Context context)
        {
            if (!filter.isPresent()) {
                return rewrittenAggregation;
            }

            FilterNode originalFilter = filter.get();
            return new FilterNode(
                    originalFilter.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    rewrittenAggregation,
                    originalFilter.getPredicate());
        }
    }

    private static class ClonedPlan
    {
        private final PlanNode plan;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> mappings;

        private ClonedPlan(PlanNode plan, Map<VariableReferenceExpression, VariableReferenceExpression> mappings)
        {
            this.plan = plan;
            this.mappings = ImmutableMap.copyOf(mappings);
        }

        private PlanNode getPlan()
        {
            return plan;
        }

        private Optional<VariableReferenceExpression> getMappedVariable(VariableReferenceExpression variable)
        {
            return Optional.ofNullable(mappings.get(variable));
        }
    }

    private static class FreshSymbolCloner
            extends SimplePlanRewriter<Void>
    {
        private final Context ruleContext;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> mappings = new HashMap<>();

        private FreshSymbolCloner(Context ruleContext)
        {
            this.ruleContext = ruleContext;
        }

        private Map<VariableReferenceExpression, VariableReferenceExpression> getMappings()
        {
            return mappings;
        }

        @Override
        public PlanNode visitPlan(PlanNode node, RewriteContext<Void> rewriteContext)
        {
            PlanNode resolved = ruleContext.getLookup().resolve(node);
            if (resolved != node) {
                return resolved.accept(this, rewriteContext);
            }
            throw new UnsupportedOperationException("Unsupported cloned filtering source node: " + node.getClass().getSimpleName());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> rewriteContext)
        {
            ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : node.getAssignments().entrySet()) {
                assignments.put(map(entry.getKey()), entry.getValue());
            }

            return new TableScanNode(
                    node.getSourceLocation(),
                    ruleContext.getIdAllocator().getNextId(),
                    node.getTable(),
                    map(node.getOutputVariables()),
                    assignments.build(),
                    node.getTableConstraints(),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.getCteMaterializationInfo());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Void> rewriteContext)
        {
            PlanNode source = rewriteContext.rewrite(node.getSource());
            return new FilterNode(
                    node.getSourceLocation(),
                    ruleContext.getIdAllocator().getNextId(),
                    source,
                    map(node.getPredicate()));
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> rewriteContext)
        {
            PlanNode source = rewriteContext.rewrite(node.getSource());
            Assignments.Builder assignments = Assignments.builder();
            node.getAssignments().forEach((variable, expression) -> assignments.put(map(variable), map(expression)));
            return new ProjectNode(
                    node.getSourceLocation(),
                    ruleContext.getIdAllocator().getNextId(),
                    node.getStatsEquivalentPlanNode(),
                    source,
                    assignments.build(),
                    node.getLocality());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Void> rewriteContext)
        {
            ImmutableList.Builder<List<RowExpression>> rows = ImmutableList.builder();
            for (List<RowExpression> row : node.getRows()) {
                rows.add(row.stream()
                        .map(this::map)
                        .collect(toImmutableList()));
            }
            return new ValuesNode(
                    node.getSourceLocation(),
                    ruleContext.getIdAllocator().getNextId(),
                    node.getStatsEquivalentPlanNode(),
                    map(node.getOutputVariables()),
                    rows.build(),
                    node.getValuesNodeLabel());
        }

        private List<VariableReferenceExpression> map(List<VariableReferenceExpression> variables)
        {
            return variables.stream()
                    .map(this::map)
                    .collect(toImmutableList());
        }

        private VariableReferenceExpression map(VariableReferenceExpression variable)
        {
            return mappings.computeIfAbsent(variable, value -> ruleContext.getVariableAllocator().newVariable(value.getSourceLocation(), value.getName(), value.getType()));
        }

        private RowExpression map(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression) {
                return map((VariableReferenceExpression) expression);
            }
            return RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Void>()
            {
                @Override
                public RowExpression rewriteVariableReference(VariableReferenceExpression variable, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
                {
                    return map(variable);
                }
            }, expression);
        }
    }
}
