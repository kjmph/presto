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

import static com.facebook.presto.SystemSessionProperties.isExploitConstraints;
import static com.facebook.presto.SystemSessionProperties.isPushAggregationThroughUniqueLookupJoin;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.iterative.rule.AnnotateJoinNodeWithUniqueKeys.areJoinKeysUnique;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
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
 *       - filtered lookup -- trusted unique/PK on key
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
 * required for this transformation. Profitability depends on how many lookup
 * keys survive that filter, so the rule has a separate default-off gate until
 * aggregation cardinality and memory estimates can support an automatic choice.
 */
public class PushAggregationThroughUniqueLookupJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN = Capture.newCapture();
    private static final Capture<JoinNode> PROJECTED_JOIN = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushAggregationThroughUniqueLookupJoin::isSupportedAggregation)
            .with(source().matching(join().capturedAs(JOIN)));

    private static final Pattern<AggregationNode> PATTERN_WITH_PROJECTION = aggregation()
            .matching(PushAggregationThroughUniqueLookupJoin::isSupportedAggregation)
            .with(source().matching(project()
                    .with(source().matching(join().capturedAs(PROJECTED_JOIN)))));

    private final DeterminismEvaluator determinismEvaluator;

    public PushAggregationThroughUniqueLookupJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(requireNonNull(functionAndTypeManager, "functionAndTypeManager is null"));
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
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        JoinNode join = captures.get(JOIN);
        return applyPushdown(aggregation, join, join);
    }

    private Result applyPushdown(
            AggregationNode aggregation,
            JoinNode join,
            JoinNode uniquenessSource)
    {
        if (!isSupportedJoin(join)) {
            return Result.empty();
        }

        Optional<PlanNode> pushedToLeft = tryPushToSide(aggregation, join, uniquenessSource, true);
        if (pushedToLeft.isPresent()) {
            return Result.ofPlanNode(pushedToLeft.get());
        }

        return tryPushToSide(aggregation, join, uniquenessSource, false)
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }

    private Optional<PlanNode> tryPushToSide(
            AggregationNode aggregation,
            JoinNode join,
            JoinNode uniquenessSource,
            boolean aggregationOnLeft)
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
                && !join.getRightHashVariable().isPresent()
                && join.getDynamicFilters().isEmpty();
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
            return applyPushdown(rewrittenAggregation, projectedJoin.get(), uniquenessSource);
        }
    }
}
