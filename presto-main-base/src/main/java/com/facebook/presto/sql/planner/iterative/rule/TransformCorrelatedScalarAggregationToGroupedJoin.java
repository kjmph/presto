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

import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
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
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.matching.Pattern.nonEmpty;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.optimizations.QueryCardinalityUtil.isScalar;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.Patterns.LateralJoin.correlation;
import static com.facebook.presto.sql.planner.plan.Patterns.lateralJoin;
import static com.facebook.presto.util.MorePredicates.isInstanceOfAny;
import static java.util.Objects.requireNonNull;

/**
 * Rewrites correlated scalar aggregations whose correlation is a simple equality
 * into a grouped aggregation joined back to the input.
 *
 * <pre>
 * - LateralJoin(correlation: C)
 *   - input
 *   - Project(f(agg))
 *     - Aggregation(GROUP BY (); agg(x))
 *       - Filter(K = C)
 *         - source
 * </pre>
 *
 * becomes:
 *
 * <pre>
 * - Project(input, f(agg))
 *   - Join(LEFT, C = K)
 *     - input
 *     - Aggregation(GROUP BY K; agg(x))
 *       - source
 * </pre>
 *
 * This avoids the more general scalar-subquery rewrite that joins every input
 * row to the raw subquery source and then re-aggregates by a synthetic unique id.
 */
public class TransformCorrelatedScalarAggregationToGroupedJoin
        implements Rule<LateralJoinNode>
{
    private static final Pattern<LateralJoinNode> PATTERN = lateralJoin()
            .with(nonEmpty(correlation()));

    private final LogicalRowExpressions logicalRowExpressions;
    private final FunctionResolution functionResolution;

    public TransformCorrelatedScalarAggregationToGroupedJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                functionResolution,
                functionAndTypeManager);
    }

    @Override
    public Pattern<LateralJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LateralJoinNode lateralJoinNode, Captures captures, Context context)
    {
        PlanNode subquery = context.getLookup().resolve(lateralJoinNode.getSubquery());
        if (!isScalar(subquery, context.getLookup())) {
            return Result.empty();
        }

        Optional<AggregationNode> aggregation = findAggregation(subquery, context);
        if (!aggregation.isPresent() || !isSupportedScalarAggregation(aggregation.get())) {
            return Result.empty();
        }

        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(
                context.getIdAllocator(),
                context.getVariableAllocator(),
                context.getLookup(),
                logicalRowExpressions);
        Optional<DecorrelatedNode> decorrelatedSource = decorrelator.decorrelateFilters(
                context.getLookup().resolve(aggregation.get().getSource()),
                lateralJoinNode.getCorrelation());
        if (!decorrelatedSource.isPresent() || !decorrelatedSource.get().getCorrelatedPredicates().isPresent()) {
            return Result.empty();
        }

        Optional<JoinKeys> joinKeys = extractJoinKeys(
                decorrelatedSource.get().getCorrelatedPredicates().get(),
                lateralJoinNode.getCorrelation(),
                decorrelatedSource.get().getNode().getOutputVariables());
        if (!joinKeys.isPresent()) {
            return Result.empty();
        }

        AggregationNode groupedAggregation = new AggregationNode(
                aggregation.get().getSourceLocation(),
                context.getIdAllocator().getNextId(),
                decorrelatedSource.get().getNode(),
                aggregation.get().getAggregations(),
                singleGroupingSet(joinKeys.get().getSubqueryKeys()),
                ImmutableList.of(),
                aggregation.get().getStep(),
                aggregation.get().getHashVariable(),
                Optional.empty(),
                Optional.empty());

        JoinNode join = new JoinNode(
                lateralJoinNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.LEFT,
                lateralJoinNode.getInput(),
                groupedAggregation,
                joinKeys.get().getClauses(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(lateralJoinNode.getInput().getOutputVariables())
                        .addAll(groupedAggregation.getAggregations().keySet())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        Optional<ProjectNode> subqueryProjection = searchFrom(subquery, context.getLookup())
                .where(ProjectNode.class::isInstance)
                .recurseOnlyWhen(EnforceSingleRowNode.class::isInstance)
                .findFirst();

        if (subqueryProjection.isPresent()) {
            return Result.ofPlanNode(new ProjectNode(
                    context.getIdAllocator().getNextId(),
                    join,
                    Assignments.builder()
                            .putAll(identityAssignments(lateralJoinNode.getInput().getOutputVariables()))
                            .putAll(subqueryProjection.get().getAssignments())
                            .build()));
        }

        return Result.ofPlanNode(new ProjectNode(
                context.getIdAllocator().getNextId(),
                join,
                identityAssignments(lateralJoinNode.getOutputVariables())));
    }

    private static Optional<AggregationNode> findAggregation(PlanNode rootNode, Context context)
    {
        return searchFrom(rootNode, context.getLookup())
                .where(AggregationNode.class::isInstance)
                .recurseOnlyWhen(isInstanceOfAny(ProjectNode.class, EnforceSingleRowNode.class))
                .findFirst();
    }

    private boolean isSupportedScalarAggregation(AggregationNode aggregation)
    {
        return aggregation.getGroupingKeys().isEmpty()
                && aggregation.getGroupingSetCount() == 1
                && aggregation.getStep() == AggregationNode.Step.SINGLE
                && !aggregation.getAggregations().isEmpty()
                && !aggregation.hasOrderings()
                && !aggregation.getHashVariable().isPresent()
                && !aggregation.getGroupIdVariable().isPresent()
                && !aggregation.getAggregationId().isPresent()
                && aggregation.getAggregations().values().stream()
                .noneMatch(aggregationFunction -> aggregationFunction.isDistinct()
                        || functionResolution.isCountFunction(aggregationFunction.getFunctionHandle())
                        || functionResolution.isCountIfFunction(aggregationFunction.getFunctionHandle())
                        || aggregationFunction.getFilter().isPresent()
                        || aggregationFunction.getMask().isPresent());
    }

    private Optional<JoinKeys> extractJoinKeys(
            RowExpression correlatedPredicate,
            List<VariableReferenceExpression> correlation,
            List<VariableReferenceExpression> subqueryOutputs)
    {
        ImmutableList.Builder<EquiJoinClause> clauses = ImmutableList.builder();
        ImmutableList.Builder<VariableReferenceExpression> subqueryKeys = ImmutableList.builder();

        for (RowExpression conjunct : LogicalRowExpressions.extractConjuncts(correlatedPredicate)) {
            if (!(conjunct instanceof CallExpression)) {
                return Optional.empty();
            }
            CallExpression call = (CallExpression) conjunct;
            if (!functionResolution.isEqualsFunction(call.getFunctionHandle()) || call.getArguments().size() != 2) {
                return Optional.empty();
            }

            Optional<VariableReferenceExpression> left = getOnlyVariable(call.getArguments().get(0));
            Optional<VariableReferenceExpression> right = getOnlyVariable(call.getArguments().get(1));
            if (!left.isPresent() || !right.isPresent()) {
                return Optional.empty();
            }

            Optional<EquiJoinClause> clause = toJoinClause(left.get(), right.get(), correlation, subqueryOutputs);
            if (!clause.isPresent()) {
                return Optional.empty();
            }
            clauses.add(clause.get());
            subqueryKeys.add(clause.get().getRight());
        }

        ImmutableList<EquiJoinClause> builtClauses = clauses.build();
        if (builtClauses.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new JoinKeys(builtClauses, subqueryKeys.build()));
    }

    private static Optional<VariableReferenceExpression> getOnlyVariable(RowExpression expression)
    {
        List<VariableReferenceExpression> variables = ImmutableList.copyOf(VariablesExtractor.extractUnique(expression));
        if (variables.size() != 1 || !(expression instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }
        return Optional.of(variables.get(0));
    }

    private static Optional<EquiJoinClause> toJoinClause(
            VariableReferenceExpression left,
            VariableReferenceExpression right,
            List<VariableReferenceExpression> correlation,
            List<VariableReferenceExpression> subqueryOutputs)
    {
        boolean leftIsCorrelation = correlation.contains(left);
        boolean rightIsCorrelation = correlation.contains(right);
        boolean leftIsSubquery = subqueryOutputs.contains(left);
        boolean rightIsSubquery = subqueryOutputs.contains(right);

        if (leftIsCorrelation && rightIsSubquery && !rightIsCorrelation) {
            return Optional.of(new EquiJoinClause(left, right));
        }
        if (rightIsCorrelation && leftIsSubquery && !leftIsCorrelation) {
            return Optional.of(new EquiJoinClause(right, left));
        }
        return Optional.empty();
    }

    private static class JoinKeys
    {
        private final List<EquiJoinClause> clauses;
        private final List<VariableReferenceExpression> subqueryKeys;

        private JoinKeys(List<EquiJoinClause> clauses, List<VariableReferenceExpression> subqueryKeys)
        {
            this.clauses = ImmutableList.copyOf(clauses);
            this.subqueryKeys = ImmutableList.copyOf(subqueryKeys);
        }

        public List<EquiJoinClause> getClauses()
        {
            return clauses;
        }

        public List<VariableReferenceExpression> getSubqueryKeys()
        {
            return subqueryKeys;
        }
    }
}
