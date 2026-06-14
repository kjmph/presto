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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Pushes a global sum below one side of an inner equi-join by grouping on the
 * join keys, then keeps a final sum above the join.
 *
 * This turns:
 *
 * <pre>
 * - Aggregation(sum(left_value))
 *   - Join(left.key = right.key)
 *     - left
 *     - right
 * </pre>
 *
 * into:
 *
 * <pre>
 * - Aggregation(sum(pre_sum))
 *   - Join(left.key = right.key)
 *     - Aggregation(GROUP BY left.key; sum(left_value) AS pre_sum)
 *       - left
 *     - right
 * </pre>
 *
 * The final join is preserved, so unmatched rows and duplicate matches keep the
 * same contribution as the original plan.
 */
public class PushGlobalSumThroughInnerJoin
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = typeOf(AggregationNode.class);

    private final FunctionAndTypeManager functionAndTypeManager;

    public PushGlobalSumThroughInnerJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return shouldPushAggregationThroughJoin(session);
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        if (!isSupportedAggregation(aggregation)) {
            return Result.empty();
        }

        PlanNode source = context.getLookup().resolve(aggregation.getSource());
        Optional<ProjectNode> project = Optional.empty();
        if (source instanceof ProjectNode) {
            project = Optional.of((ProjectNode) source);
            source = context.getLookup().resolve(project.get().getSource());
        }

        if (!(source instanceof JoinNode)) {
            return Result.empty();
        }

        JoinNode join = (JoinNode) source;
        if (!isSupportedJoin(join)) {
            return Result.empty();
        }

        Optional<PlanNode> pushedToLeft = tryPushToSide(aggregation, project, join, true, context);
        if (pushedToLeft.isPresent()) {
            return Result.ofPlanNode(pushedToLeft.get());
        }

        return tryPushToSide(aggregation, project, join, false, context)
                .map(Result::ofPlanNode)
                .orElseGet(Result::empty);
    }

    private boolean isSupportedAggregation(AggregationNode aggregation)
    {
        if (aggregation.getStep() != SINGLE
                || aggregation.getGroupingSetCount() != 1
                || !aggregation.getGroupingKeys().isEmpty()
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()) {
            return false;
        }

        return aggregation.getAggregations().entrySet().stream()
                .allMatch(entry -> isSupportedSum(entry.getKey(), entry.getValue()));
    }

    private boolean isSupportedJoin(JoinNode join)
    {
        return join.getType() == JoinType.INNER
                && !join.getCriteria().isEmpty()
                && !join.getFilter().isPresent()
                && !join.getLeftHashVariable().isPresent()
                && !join.getRightHashVariable().isPresent()
                && join.getDynamicFilters().isEmpty();
    }

    private Optional<PlanNode> tryPushToSide(
            AggregationNode aggregation,
            Optional<ProjectNode> project,
            JoinNode join,
            boolean pushToLeft,
            Context context)
    {
        PlanNode aggregationSide = context.getLookup().resolve(pushToLeft ? join.getLeft() : join.getRight());
        PlanNode otherSide = context.getLookup().resolve(pushToLeft ? join.getRight() : join.getLeft());
        Set<VariableReferenceExpression> aggregationSideOutputs = ImmutableSet.copyOf(aggregationSide.getOutputVariables());

        if (containsAggregation(aggregationSide, context)) {
            return Optional.empty();
        }

        LinkedHashSet<VariableReferenceExpression> pushedGroupingKeys = new LinkedHashSet<>();
        ImmutableList.Builder<EquiJoinClause> rewrittenCriteria = ImmutableList.builder();
        for (EquiJoinClause clause : join.getCriteria()) {
            VariableReferenceExpression aggregationKey = pushToLeft ? clause.getLeft() : clause.getRight();
            VariableReferenceExpression otherKey = pushToLeft ? clause.getRight() : clause.getLeft();
            if (!aggregationSideOutputs.contains(aggregationKey) || !otherSide.getOutputVariables().contains(otherKey)) {
                return Optional.empty();
            }
            pushedGroupingKeys.add(aggregationKey);
            rewrittenCriteria.add(pushToLeft ? new EquiJoinClause(aggregationKey, otherKey) : new EquiJoinClause(otherKey, aggregationKey));
        }

        Assignments.Builder preProjectAssignments = Assignments.builder();
        pushedGroupingKeys.forEach(key -> preProjectAssignments.put(key, key));

        Set<VariableReferenceExpression> projectedInputs = new LinkedHashSet<>();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : aggregation.getAggregations().entrySet()) {
            VariableReferenceExpression input = (VariableReferenceExpression) entry.getValue().getArguments().get(0);
            Optional<RowExpression> mappedInput = mapThroughProject(input, project);
            if (!mappedInput.isPresent() || !aggregationSideOutputs.containsAll(VariablesExtractor.extractUnique(mappedInput.get()))) {
                return Optional.empty();
            }
            if (pushedGroupingKeys.contains(input)) {
                if (!mappedInput.get().equals(input)) {
                    return Optional.empty();
                }
                continue;
            }
            if (projectedInputs.add(input)) {
                preProjectAssignments.put(input, mappedInput.get());
            }
        }

        ProjectNode preProject = new ProjectNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                aggregationSide,
                preProjectAssignments.build(),
                LOCAL);

        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> preAggregations = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregations = ImmutableMap.builder();
        ImmutableList.Builder<VariableReferenceExpression> preAggregationOutputs = ImmutableList.builder();
        pushedGroupingKeys.forEach(preAggregationOutputs::add);
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : aggregation.getAggregations().entrySet()) {
            VariableReferenceExpression preOutput = context.getVariableAllocator().newVariable(entry.getKey(), "pre");
            preAggregations.put(preOutput, entry.getValue());
            finalAggregations.put(entry.getKey(), sum(preOutput));
            preAggregationOutputs.add(preOutput);
        }

        AggregationNode preAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                preProject,
                preAggregations.build(),
                singleGroupingSet(ImmutableList.copyOf(pushedGroupingKeys)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        PlanNode left = pushToLeft ? preAggregation : otherSide;
        PlanNode right = pushToLeft ? otherSide : preAggregation;
        ImmutableList<VariableReferenceExpression> joinOutputVariables = preAggregationOutputs.build();

        JoinNode rewrittenJoin = new JoinNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.INNER,
                left,
                right,
                rewrittenCriteria.build(),
                joinOutputVariables,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                emptyMap());

        return Optional.of(new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                rewrittenJoin,
                finalAggregations.build(),
                aggregation.getGroupingSets(),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
    }

    private boolean isSupportedSum(VariableReferenceExpression output, AggregationNode.Aggregation aggregation)
    {
        if (aggregation.getArguments().size() != 1
                || aggregation.isDistinct()
                || aggregation.getFilter().isPresent()
                || aggregation.getMask().isPresent()
                || aggregation.getOrderBy().isPresent()
                || !(aggregation.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return false;
        }

        VariableReferenceExpression input = (VariableReferenceExpression) aggregation.getArguments().get(0);
        if (!input.getType().equals(output.getType()) || !aggregation.getCall().getType().equals(output.getType())) {
            return false;
        }
        try {
            return functionAndTypeManager.lookupFunction("sum", fromTypes(input.getType())).equals(aggregation.getFunctionHandle());
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode())) {
                return false;
            }
            throw e;
        }
    }

    private AggregationNode.Aggregation sum(VariableReferenceExpression variable)
    {
        return new AggregationNode.Aggregation(
                new CallExpression(
                        variable.getSourceLocation(),
                        "sum",
                        functionAndTypeManager.lookupFunction("sum", fromTypes(variable.getType())),
                        variable.getType(),
                        ImmutableList.of(variable)),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    private Optional<RowExpression> mapThroughProject(VariableReferenceExpression variable, Optional<ProjectNode> project)
    {
        if (!project.isPresent()) {
            return Optional.of(variable);
        }
        return Optional.ofNullable(project.get().getAssignments().get(variable));
    }

    private boolean containsAggregation(PlanNode node, Context context)
    {
        node = context.getLookup().resolve(node);
        if (node instanceof AggregationNode) {
            return true;
        }
        for (PlanNode source : node.getSources()) {
            if (containsAggregation(source, context)) {
                return true;
            }
        }
        return false;
    }
}
