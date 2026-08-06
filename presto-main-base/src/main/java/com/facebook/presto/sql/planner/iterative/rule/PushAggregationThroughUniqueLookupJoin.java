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
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getJoinDistributionType;
import static com.facebook.presto.SystemSessionProperties.getPartialAggregationByteReductionThreshold;
import static com.facebook.presto.SystemSessionProperties.getPartialAggregationStrategy;
import static com.facebook.presto.SystemSessionProperties.getQueryMaxMemoryPerNode;
import static com.facebook.presto.SystemSessionProperties.isExploitConstraints;
import static com.facebook.presto.SystemSessionProperties.isPushAggregationThroughUniqueLookupJoin;
import static com.facebook.presto.SystemSessionProperties.isSingleNodeExecutionEnabled;
import static com.facebook.presto.cost.AggregationStatsRule.groupBy;
import static com.facebook.presto.operator.aggregation.AggregationUtils.isDecomposable;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.BROADCAST;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.AUTOMATIC;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.PartialAggregationStrategy.NEVER;
import static com.facebook.presto.sql.planner.iterative.rule.AnnotateJoinNodeWithUniqueKeys.areJoinKeysUnique;
import static com.facebook.presto.sql.planner.iterative.rule.DetermineJoinDistributionType.isBelowMaxBroadcastSize;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isAtMostScalar;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static java.util.Objects.requireNonNull;

/**
 * Pushes a complete aggregation below an inner join when the other join side
 * is known to be unique.
 *
 * Given grouping keys that include every fact-side join key:
 *
 * <pre>
 * - Aggregation(SINGLE, GROUP BY fact_key, ...)
 *   - Project(fact expressions)
 *     - Join(fact.fact_key = lookup.key)
 *       - fact
 *       - filtered lookup -- planner-proven unique on key
 * </pre>
 *
 * this rule produces:
 *
 * <pre>
 * - Join(fact.fact_key = lookup.key)
 *   - Aggregation(SINGLE, GROUP BY fact_key, ...)
 *     - Project(fact expressions)
 *       - fact
 *   - filtered lookup
 * </pre>
 *
 * The lookup join is retained. It can therefore continue to filter fact keys,
 * but uniqueness proves that it cannot duplicate rows within a fact-key group.
 * A foreign key does not prove coverage after a lookup-side filter and is not
 * required for this transformation. Existing dynamic filters remain valid
 * because all fact-side join keys are grouping keys and aggregation preserves
 * the join-key domains.
 *
 * The rule uses source, grouping, and join cardinality estimates to require a
 * substantial reduction before the lookup. Lookup sides that can be broadcast,
 * and therefore do not require repartitioning the fact side, use a stricter
 * selectivity threshold. It also requires useful partial aggregation and bounds
 * estimated intermediate state per worker for this operator. This is a local
 * safety guard rather than a query-wide memory model. Unknown estimates,
 * variable-width state, insufficient memory headroom, and marginal reductions
 * preserve the original lookup-first plan.
 */
public class PushAggregationThroughUniqueLookupJoin
        implements Rule<AggregationNode>
{
    private static final double MAX_GROUPS_TO_SOURCE_ROWS_RATIO = 0.50;
    private static final double MAX_GROUPS_TO_PARTITIONED_JOIN_ROWS_RATIO = 0.80;
    private static final double MAX_GROUPS_TO_REPLICATED_JOIN_ROWS_RATIO = 0.50;
    private static final double MAX_AGGREGATION_STATE_TO_SOURCE_BYTES_RATIO = 0.50;
    private static final double AGGREGATION_STATE_MEMORY_OVERHEAD = 2.0;
    // Hash-table and row-container metadata impose a floor even when logical
    // grouping keys and aggregation state are only a few bytes wide.
    private static final double MIN_HASH_AGGREGATION_MEMORY_BYTES_PER_GROUP = 64.0;
    private static final double MAX_AGGREGATION_MEMORY_FRACTION = 0.60;

    private static final Capture<JoinNode> JOIN = Capture.newCapture();
    private static final Capture<JoinNode> PROJECTED_JOIN = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushAggregationThroughUniqueLookupJoin::isSupportedAggregation)
            .with(source().matching(join().capturedAs(JOIN)));

    private static final Pattern<AggregationNode> PATTERN_WITH_PROJECTION = aggregation()
            .matching(PushAggregationThroughUniqueLookupJoin::isSupportedAggregation)
            .with(source().matching(project()
                    .with(source().matching(join().capturedAs(PROJECTED_JOIN)))));

    private final FunctionAndTypeManager functionAndTypeManager;
    private final TaskCountEstimator taskCountEstimator;
    private final DeterminismEvaluator determinismEvaluator;

    public PushAggregationThroughUniqueLookupJoin(FunctionAndTypeManager functionAndTypeManager, TaskCountEstimator taskCountEstimator)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.taskCountEstimator = requireNonNull(taskCountEstimator, "taskCountEstimator is null");
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @VisibleForTesting
    public PushAggregationThroughUniqueLookupJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this(functionAndTypeManager, new TaskCountEstimator(() -> 1));
    }

    public Iterable<Rule<?>> rules()
    {
        return ImmutableList.of(this, pushAggregationThroughUniqueLookupJoinWithProjection());
    }

    @VisibleForTesting
    Rule<AggregationNode> pushAggregationThroughUniqueLookupJoinWithProjection()
    {
        return new PushAggregationThroughUniqueLookupJoinWithProjection();
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isExploitConstraints(session) && isPushAggregationThroughUniqueLookupJoin(session);
    }

    @Override
    public boolean isCostBased(Session session)
    {
        return true;
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        JoinNode join = captures.get(JOIN);
        return applyPushdown(aggregation, join, join, context);
    }

    private Result applyPushdown(
            AggregationNode aggregation,
            JoinNode join,
            JoinNode uniquenessSource,
            Context context)
    {
        if (!isSupportedJoin(join)) {
            return Result.empty();
        }

        Optional<PlanNode> pushedToLeft = tryPushToSide(aggregation, join, uniquenessSource, true, context);
        if (pushedToLeft.isPresent()) {
            return Result.ofPlanNode(pushedToLeft.get());
        }

        return tryPushToSide(aggregation, join, uniquenessSource, false, context)
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }

    private Optional<PlanNode> tryPushToSide(
            AggregationNode aggregation,
            JoinNode join,
            JoinNode uniquenessSource,
            boolean aggregationOnLeft,
            Context context)
    {
        PlanNode aggregationSource = aggregationOnLeft ? join.getLeft() : join.getRight();
        Set<VariableReferenceExpression> aggregationSourceOutputs = ImmutableSet.copyOf(aggregationSource.getOutputVariables());
        Set<VariableReferenceExpression> groupingKeys = ImmutableSet.copyOf(aggregation.getGroupingKeys());

        List<VariableReferenceExpression> aggregationJoinKeys = join.getCriteria().stream()
                .map(clause -> aggregationOnLeft ? clause.getLeft() : clause.getRight())
                .collect(ImmutableList.toImmutableList());
        Set<VariableReferenceExpression> lookupJoinKeys = uniquenessSource.getCriteria().stream()
                .map(clause -> aggregationOnLeft ? clause.getRight() : clause.getLeft())
                .collect(ImmutableSet.toImmutableSet());

        if (!groupingKeys.containsAll(aggregationJoinKeys) ||
                !aggregationSourceOutputs.containsAll(groupingKeys) ||
                !allAggregationsOn(aggregation.getAggregations(), aggregationSourceOutputs) ||
                !isLookupUnique(uniquenessSource, lookupJoinKeys, !aggregationOnLeft)) {
            return Optional.empty();
        }

        AggregationNode pushedAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                aggregation.getId(),
                Optional.empty(),
                aggregationSource,
                aggregation.getAggregations(),
                aggregation.getGroupingSets(),
                ImmutableList.of(),
                aggregation.getStep(),
                aggregation.getHashVariable(),
                aggregation.getGroupIdVariable(),
                aggregation.getAggregationId());

        if (!isPushdownBeneficial(aggregationSource, join, pushedAggregation, aggregationOnLeft, context)) {
            return Optional.empty();
        }

        PlanNode left = aggregationOnLeft ? pushedAggregation : join.getLeft();
        PlanNode right = aggregationOnLeft ? join.getRight() : pushedAggregation;

        return Optional.of(new JoinNode(
                join.getSourceLocation(),
                join.getId(),
                Optional.empty(),
                join.getType(),
                left,
                right,
                join.getCriteria(),
                aggregation.getOutputVariables(),
                join.getFilter(),
                join.getLeftHashVariable(),
                join.getRightHashVariable(),
                join.getDistributionType(),
                join.getDynamicFilters(),
                aggregationOnLeft ? join.isLeftKeysUnique() : true,
                aggregationOnLeft ? true : join.isRightKeysUnique(),
                join.isLeftKeysNonNull(),
                join.isRightKeysNonNull(),
                join.isLeftKeysCoveredByRightKeys(),
                join.isRightKeysCoveredByLeftKeys()));
    }

    private boolean isPushdownBeneficial(
            PlanNode aggregationSource,
            JoinNode join,
            AggregationNode pushedAggregation,
            boolean aggregationOnLeft,
            Context context)
    {
        if (getPartialAggregationStrategy(context.getSession()) == NEVER ||
                !isDecomposable(pushedAggregation, functionAndTypeManager) ||
                pushedAggregation.getGroupingKeys().stream().anyMatch(variable -> !(variable.getType() instanceof FixedWidthType))) {
            return false;
        }

        List<Type> intermediateTypes = pushedAggregation.getAggregations().values().stream()
                .map(AggregationNode.Aggregation::getFunctionHandle)
                .map(functionAndTypeManager::getAggregateFunctionImplementation)
                .map(function -> function.getIntermediateType())
                .collect(ImmutableList.toImmutableList());
        if (intermediateTypes.stream().anyMatch(type -> !(type instanceof FixedWidthType))) {
            return false;
        }

        PlanNodeStatsEstimate sourceStats = context.getStatsProvider().getStats(aggregationSource);
        if (aggregationSource.getOutputVariables().stream()
                .filter(variable -> !(variable.getType() instanceof FixedWidthType))
                .map(sourceStats::getVariableStatistics)
                .mapToDouble(variableStats -> variableStats.getAverageRowSize())
                .anyMatch(size -> !Double.isFinite(size) || size < 0)) {
            return false;
        }

        PlanNodeStatsEstimate pushedAggregationStats = groupBy(
                sourceStats,
                pushedAggregation.getGroupingKeys(),
                pushedAggregation.getAggregations());

        double sourceRows = sourceStats.getOutputRowCount();
        double aggregationRows = pushedAggregationStats.getOutputRowCount();
        double joinRows = context.getStatsProvider().getStats(join).getOutputRowCount();
        double sourceBytes = sourceStats.getOutputSizeForVariables(aggregationSource.getOutputVariables());
        double aggregationStateSizePerGroup = aggregationStateSizeInBytes(pushedAggregation.getGroupingKeys(), intermediateTypes);
        double aggregationStateBytes = aggregationRows * aggregationStateSizePerGroup;
        if (!isFinitePositive(sourceRows) ||
                !isFinitePositive(aggregationRows) ||
                !isFinitePositive(joinRows) ||
                !isFinitePositive(sourceBytes) ||
                !isFinitePositive(aggregationStateBytes)) {
            return false;
        }

        double stateReductionRatio = MAX_AGGREGATION_STATE_TO_SOURCE_BYTES_RATIO;
        if (getPartialAggregationStrategy(context.getSession()) == AUTOMATIC) {
            stateReductionRatio = Math.min(stateReductionRatio, getPartialAggregationByteReductionThreshold(context.getSession()));
        }
        if (!isFinitePositive(stateReductionRatio) ||
                aggregationRows > sourceRows * MAX_GROUPS_TO_SOURCE_ROWS_RATIO ||
                aggregationStateBytes > sourceBytes * stateReductionRatio) {
            return false;
        }

        double maxGroupsToJoinRowsRatio = isLookupLikelyReplicated(join, aggregationOnLeft, context) ?
                MAX_GROUPS_TO_REPLICATED_JOIN_ROWS_RATIO :
                MAX_GROUPS_TO_PARTITIONED_JOIN_ROWS_RATIO;
        if (aggregationRows > joinRows * maxGroupsToJoinRowsRatio) {
            return false;
        }

        int hashedTaskCount = isSingleNodeExecutionEnabled(context.getSession()) ?
                1 :
                Math.max(1, taskCountEstimator.estimateHashedTaskCount(context.getSession()));
        double estimatedAggregationMemoryBytes = aggregationRows * Math.max(
                aggregationStateSizePerGroup * AGGREGATION_STATE_MEMORY_OVERHEAD,
                MIN_HASH_AGGREGATION_MEMORY_BYTES_PER_GROUP);
        double estimatedStateBytesPerTask = estimatedAggregationMemoryBytes / hashedTaskCount;
        double maxStateBytesPerTask = getQueryMaxMemoryPerNode(context.getSession()).toBytes() * MAX_AGGREGATION_MEMORY_FRACTION;
        return estimatedStateBytesPerTask <= maxStateBytesPerTask;
    }

    private static double aggregationStateSizeInBytes(
            List<VariableReferenceExpression> groupingKeys,
            List<Type> intermediateTypes)
    {
        return groupingKeys.stream()
                .map(VariableReferenceExpression::getType)
                .map(FixedWidthType.class::cast)
                .mapToDouble(type -> type.getFixedSize() + Byte.BYTES)
                .sum() +
                intermediateTypes.stream()
                        .map(FixedWidthType.class::cast)
                        .mapToDouble(type -> type.getFixedSize() + Byte.BYTES)
                        .sum();
    }

    private static boolean isLookupLikelyReplicated(JoinNode join, boolean aggregationOnLeft, Context context)
    {
        if (join.getDistributionType().isPresent()) {
            // In a replicated join the right side is the replicated build. A
            // fact-side aggregation on the right reduces that build rather than
            // moving work ahead of a replicated lookup.
            return join.getDistributionType().get() == REPLICATED && aggregationOnLeft;
        }
        if (getJoinDistributionType(context.getSession()) == BROADCAST) {
            // BROADCAST preserves syntactic order. The lookup is the build side
            // only when the fact input being aggregated is on the left.
            return aggregationOnLeft;
        }
        if (getJoinDistributionType(context.getSession()) == PARTITIONED) {
            return false;
        }

        // DetermineJoinDistributionType runs after this rule. Treat every
        // broadcast-eligible lookup as replicated so the profitability gate is
        // conservative even if the cost model ultimately chooses partitioning.
        JoinNode lookupOnBuild = aggregationOnLeft ? join : join.flipChildren();
        return isAtMostScalar(lookupOnBuild.getRight(), context.getLookup()) ||
                isBelowMaxBroadcastSize(lookupOnBuild, context);
    }

    private static boolean isFinitePositive(double value)
    {
        return Double.isFinite(value) && value > 0;
    }

    private static boolean isSupportedAggregation(AggregationNode aggregation)
    {
        return aggregation.getStep() == SINGLE
                && aggregation.getGroupingSetCount() == 1
                && !aggregation.getGroupingKeys().isEmpty()
                && aggregation.getGlobalGroupingSets().isEmpty()
                && aggregation.getPreGroupedVariables().isEmpty()
                && !aggregation.getHashVariable().isPresent()
                && !aggregation.getGroupIdVariable().isPresent();
    }

    private static boolean isSupportedJoin(JoinNode join)
    {
        return join.getType() == JoinType.INNER
                && !join.getCriteria().isEmpty()
                && !join.getFilter().isPresent()
                && !join.getLeftHashVariable().isPresent()
                && !join.getRightHashVariable().isPresent();
    }

    private static boolean allAggregationsOn(
            Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations,
            Set<VariableReferenceExpression> variables)
    {
        ImmutableSet.Builder<VariableReferenceExpression> inputs = ImmutableSet.builder();
        aggregations.values().forEach(aggregation -> {
            inputs.addAll(extractAggregationUniqueVariables(aggregation));
            aggregation.getMask().ifPresent(inputs::add);
        });
        return variables.containsAll(inputs.build());
    }

    private static boolean isLookupUnique(
            JoinNode join,
            Set<VariableReferenceExpression> joinKeys,
            boolean lookupOnLeft)
    {
        if (lookupOnLeft ? join.isLeftKeysUnique() : join.isRightKeysUnique()) {
            return true;
        }
        return areJoinKeysUnique(lookupOnLeft ? join.getLeft() : join.getRight(), joinKeys);
    }

    private class PushAggregationThroughUniqueLookupJoinWithProjection
            implements Rule<AggregationNode>
    {
        @Override
        public Pattern<AggregationNode> getPattern()
        {
            return PATTERN_WITH_PROJECTION;
        }

        @Override
        public boolean isEnabled(Session session)
        {
            return PushAggregationThroughUniqueLookupJoin.this.isEnabled(session);
        }

        @Override
        public boolean isCostBased(Session session)
        {
            return PushAggregationThroughUniqueLookupJoin.this.isCostBased(session);
        }

        @Override
        public Result apply(AggregationNode aggregation, Captures captures, Context context)
        {
            ProjectNode project = (ProjectNode) context.getLookup().resolve(aggregation.getSource());
            JoinNode uniquenessSource = captures.get(PROJECTED_JOIN);
            Optional<JoinNode> projectedJoin = PushProjectionThroughJoin.pushProjectionThroughJoin(
                    project,
                    context.getLookup(),
                    context.getIdAllocator(),
                    determinismEvaluator);
            if (!projectedJoin.isPresent()) {
                return Result.empty();
            }

            AggregationNode rewrittenAggregation = (AggregationNode) aggregation.replaceChildren(ImmutableList.of(projectedJoin.get()));
            return applyPushdown(rewrittenAggregation, projectedJoin.get(), uniquenessSource, context);
        }
    }
}
