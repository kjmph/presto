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

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.SystemSessionProperties.isPushAggregationThroughJoin;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughJoin.getJoinRequiredVariables;
import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;

public class PushPartialAggregationThroughJoin
        implements Rule<AggregationNode>
{
    private static final Capture<JoinNode> JOIN_NODE = Capture.newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
            .with(source().matching(join().capturedAs(JOIN_NODE)));

    private static final Pattern<AggregationNode> PATTERN_WITH_PROJECTION = aggregation()
            .matching(PushPartialAggregationThroughJoin::isSupportedAggregationNode)
            .with(source().matching(project().with(source().matching(join()))));

    private final DeterminismEvaluator determinismEvaluator;

    public PushPartialAggregationThroughJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(requireNonNull(functionAndTypeManager, "functionAndTypeManager is null"));
    }

    public Iterable<Rule<?>> rules()
    {
        return ImmutableList.of(this, pushPartialAggregationThroughJoinWithProjection());
    }

    @VisibleForTesting
    Rule<AggregationNode> pushPartialAggregationThroughJoinWithProjection()
    {
        return new PushPartialAggregationThroughJoinWithProjection();
    }

    private static boolean isSupportedAggregationNode(AggregationNode aggregationNode)
    {
        // Don't split streaming aggregations or segmented aggregations
        if (aggregationNode.isStreamable() || aggregationNode.isSegmentedAggregationEligible()) {
            return false;
        }

        if (aggregationNode.getHashVariable().isPresent()) {
            // TODO: add support for hash symbol in aggregation node
            return false;
        }
        return aggregationNode.getStep() == PARTIAL && aggregationNode.getGroupingSetCount() == 1;
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushAggregationThroughJoin(session);
    }

    @Override
    public Result apply(AggregationNode aggregationNode, Captures captures, Context context)
    {
        JoinNode joinNode = captures.get(JOIN_NODE);
        return applyPushdown(aggregationNode, joinNode, context);
    }

    private Result applyPushdown(AggregationNode aggregationNode, JoinNode joinNode, Context context)
    {
        if (joinNode.getType() != JoinType.INNER) {
            return Result.empty();
        }

        // TODO: leave partial aggregation above Join?
        if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getLeft().getOutputVariables())) {
            return Result.ofPlanNode(pushPartialToLeftChild(aggregationNode, joinNode, context));
        }
        else {
            if (allAggregationsOn(aggregationNode.getAggregations(), joinNode.getRight().getOutputVariables())) {
                return Result.ofPlanNode(pushPartialToRightChild(aggregationNode, joinNode, context));
            }
        }

        return Result.empty();
    }

    private boolean allAggregationsOn(Map<VariableReferenceExpression, AggregationNode.Aggregation> aggregations, List<VariableReferenceExpression> variables)
    {
        ImmutableSet.Builder<VariableReferenceExpression> inputs = ImmutableSet.builder();
        aggregations.values().forEach(aggregation -> {
            inputs.addAll(extractAggregationUniqueVariables(aggregation));
            aggregation.getMask().ifPresent(inputs::add);
        });
        return variables.containsAll(inputs.build());
    }

    private PlanNode pushPartialToLeftChild(AggregationNode node, JoinNode child, Context context)
    {
        Set<VariableReferenceExpression> joinLeftChildVariables = ImmutableSet.copyOf(child.getLeft().getOutputVariables());
        List<VariableReferenceExpression> groupingSet = getPushedDownGroupingSet(node, joinLeftChildVariables, intersection(getJoinRequiredVariables(child), joinLeftChildVariables));
        AggregationNode pushedAggregation = replaceAggregationSource(node, child.getLeft(), groupingSet);
        return pushPartialToJoin(node, child, pushedAggregation, child.getRight(), context);
    }

    private PlanNode pushPartialToRightChild(AggregationNode node, JoinNode child, Context context)
    {
        Set<VariableReferenceExpression> joinRightChildVariables = ImmutableSet.copyOf(child.getRight().getOutputVariables());
        List<VariableReferenceExpression> groupingSet = getPushedDownGroupingSet(node, joinRightChildVariables, intersection(getJoinRequiredVariables(child), joinRightChildVariables));
        AggregationNode pushedAggregation = replaceAggregationSource(node, child.getRight(), groupingSet);
        return pushPartialToJoin(node, child, child.getLeft(), pushedAggregation, context);
    }

    private List<VariableReferenceExpression> getPushedDownGroupingSet(AggregationNode aggregation, Set<VariableReferenceExpression> availableVariables, Set<VariableReferenceExpression> requiredJoinVariables)
    {
        List<VariableReferenceExpression> groupingSet = aggregation.getGroupingKeys();

        // keep variables that are directly from the join's child (availableVariables)
        List<VariableReferenceExpression> pushedDownGroupingSet = groupingSet.stream()
                .filter(availableVariables::contains)
                .collect(Collectors.toList());

        // add missing required join variables to grouping set
        Set<VariableReferenceExpression> existingVariables = new HashSet<>(pushedDownGroupingSet);
        requiredJoinVariables.stream()
                .filter(existingVariables::add)
                .forEach(pushedDownGroupingSet::add);

        return pushedDownGroupingSet;
    }

    private AggregationNode replaceAggregationSource(
            AggregationNode aggregation,
            PlanNode source,
            List<VariableReferenceExpression> groupingKeys)
    {
        return new AggregationNode(
                aggregation.getSourceLocation(),
                aggregation.getId(),
                source,
                aggregation.getAggregations(),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                aggregation.getStep(),
                aggregation.getHashVariable(),
                aggregation.getGroupIdVariable(),
                aggregation.getAggregationId());
    }

    private PlanNode pushPartialToJoin(
            AggregationNode aggregation,
            JoinNode child,
            PlanNode leftChild,
            PlanNode rightChild,
            Context context)
    {
        JoinNode joinNode = new JoinNode(
                child.getSourceLocation(),
                child.getId(),
                Optional.empty(),
                child.getType(),
                leftChild,
                rightChild,
                child.getCriteria(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(leftChild.getOutputVariables())
                        .addAll(rightChild.getOutputVariables())
                        .build(),
                child.getFilter(),
                child.getLeftHashVariable(),
                child.getRightHashVariable(),
                child.getDistributionType(),
                child.getDynamicFilters(),
                child.isLeftKeysUnique(),
                child.isRightKeysUnique(),
                child.isLeftKeysNonNull(),
                child.isRightKeysNonNull(),
                child.isLeftKeysCoveredByRightKeys(),
                child.isRightKeysCoveredByLeftKeys());
        return restrictOutputs(context.getIdAllocator(), joinNode, ImmutableSet.copyOf(aggregation.getOutputVariables())).orElse(joinNode);
    }

    private class PushPartialAggregationThroughJoinWithProjection
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
            return PushPartialAggregationThroughJoin.this.isEnabled(session);
        }

        @Override
        public Result apply(AggregationNode aggregation, Captures captures, Context context)
        {
            ProjectNode project = (ProjectNode) context.getLookup().resolve(aggregation.getSource());
            Optional<JoinNode> projectedJoin = PushProjectionThroughJoin.pushProjectionThroughJoin(
                    project,
                    context.getLookup(),
                    context.getIdAllocator(),
                    determinismEvaluator);
            if (!projectedJoin.isPresent()) {
                return Result.empty();
            }
            // This variant exposes a partial aggregation to the fact-side repartition exchange.
            // Pushing it below a replicated join cannot reduce that shuffle and can aggregate rows
            // that a selective join would otherwise discard.
            if (!projectedJoin.get().getDistributionType().equals(Optional.of(PARTITIONED))) {
                return Result.empty();
            }

            AggregationNode rewrittenAggregation = (AggregationNode) aggregation.replaceChildren(ImmutableList.of(projectedJoin.get()));
            return applyPushdown(rewrittenAggregation, projectedJoin.get(), context);
        }
    }
}
