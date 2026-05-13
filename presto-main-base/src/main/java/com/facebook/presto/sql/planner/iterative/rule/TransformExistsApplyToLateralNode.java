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
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.ExistsExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator;
import com.facebook.presto.sql.planner.optimizations.PlanNodeDecorrelator.DecorrelatedNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.LateralJoinNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.expressions.LogicalRowExpressions.FALSE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.TRUE_CONSTANT;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.spi.plan.AggregationNode.globalAggregation;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.LimitNode.Step.FINAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static com.facebook.presto.sql.planner.plan.LateralJoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.LateralJoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.Patterns.applyNode;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static com.facebook.presto.sql.relational.Expressions.specialForm;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * EXISTS is modeled as (if correlated predicates are equality comparisons):
 * <pre>
 *     - Project(exists := COALESCE(subqueryTrue, false))
 *       - LateralJoin(LEFT)
 *         - input
 *         - Project(subqueryTrue := true)
 *           - Limit(count=1)
 *             - subquery
 * </pre>
 * or:
 * <pre>
 *     - LateralJoin(LEFT)
 *       - input
 *       - Project($0 > 0)
 *         - Aggregation(COUNT(*))
 *           - subquery
 * </pre>
 * otherwise
 */
public class TransformExistsApplyToLateralNode
        implements Rule<ApplyNode>
{
    private static final Pattern<ApplyNode> PATTERN = applyNode();

    private final StandardFunctionResolution functionResolution;
    private final LogicalRowExpressions logicalRowExpressions;

    public TransformExistsApplyToLateralNode(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.logicalRowExpressions = new LogicalRowExpressions(
                new RowExpressionDeterminismEvaluator(functionAndTypeManager),
                new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver()),
                functionAndTypeManager);
    }

    @Override
    public Pattern<ApplyNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ApplyNode parent, Captures captures, Context context)
    {
        if (parent.getSubqueryAssignments().size() != 1) {
            return Result.empty();
        }

        RowExpression expression = getOnlyElement(parent.getSubqueryAssignments().getExpressions());
        if (!(expression instanceof ExistsExpression)) {
            return Result.empty();
        }

        Optional<PlanNode> nonDefaultAggregation = rewriteToNonDefaultAggregation(parent, context);
        if (nonDefaultAggregation.isPresent()) {
            return Result.ofPlanNode(nonDefaultAggregation.get());
        }

        Optional<PlanNode> groupedNotEqualAggregation = rewriteToGroupedNotEqualAggregation(parent, context);
        if (groupedNotEqualAggregation.isPresent()) {
            return Result.ofPlanNode(groupedNotEqualAggregation.get());
        }

        return Result.ofPlanNode(rewriteToDefaultAggregation(parent, context));
    }

    private Optional<PlanNode> rewriteToNonDefaultAggregation(ApplyNode applyNode, Context context)
    {
        checkState(applyNode.getSubquery().getOutputVariables().isEmpty(), "Expected subquery output variables to be pruned");

        VariableReferenceExpression exists = getOnlyElement(applyNode.getSubqueryAssignments().getVariables());
        VariableReferenceExpression subqueryTrue = context.getVariableAllocator().newVariable(exists.getSourceLocation(), "subqueryTrue", BOOLEAN);

        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(identityAssignments(applyNode.getInput().getOutputVariables()));
        assignments.put(exists, specialForm(COALESCE, BOOLEAN, ImmutableList.of(subqueryTrue, FALSE_CONSTANT)));

        PlanNode subquery = new ProjectNode(
                context.getIdAllocator().getNextId(),
                new LimitNode(
                        applyNode.getSourceLocation(),
                        context.getIdAllocator().getNextId(),
                        applyNode.getSubquery(),
                        1L,
                        FINAL),
                Assignments.of(subqueryTrue, TRUE_CONSTANT));

        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getVariableAllocator(), context.getLookup(), logicalRowExpressions);
        if (!decorrelator.decorrelateFilters(subquery, applyNode.getCorrelation()).isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new ProjectNode(context.getIdAllocator().getNextId(),
                new LateralJoinNode(
                        applyNode.getSourceLocation(),
                        applyNode.getId(),
                        applyNode.getInput(),
                        subquery,
                        applyNode.getCorrelation(),
                        LEFT,
                        applyNode.getOriginSubqueryError()),
                assignments.build()));
    }

    /**
     * Rewrites a correlated EXISTS whose predicate has equality join keys plus
     * one correlated non-equality test:
     *
     * <pre>
     * EXISTS (SELECT * FROM R WHERE R.k = C.k AND R.v <> C.v AND inner_predicate)
     * </pre>
     *
     * into a left join against one grouped row per inner key:
     *
     * <pre>
     * LEFT JOIN (
     *   SELECT R.k, min(R.v), max(R.v)
     *   FROM R
     *   WHERE inner_predicate
     *   GROUP BY R.k
     *)
     * exists := COALESCE(min_v <> C.v OR max_v <> C.v, false)
     * </pre>
     *
     * The rule is deliberately narrow. It only fires for a single correlated
     * {@code <>} predicate over an orderable inner expression. SQL aggregates
     * ignore nulls, and the final COALESCE preserves EXISTS semantics when no
     * non-null inner value can satisfy the non-equality predicate.
     */
    private Optional<PlanNode> rewriteToGroupedNotEqualAggregation(ApplyNode applyNode, Context context)
    {
        checkState(applyNode.getSubquery().getOutputVariables().isEmpty(), "Expected subquery output variables to be pruned");

        PlanNodeDecorrelator decorrelator = new PlanNodeDecorrelator(context.getIdAllocator(), context.getVariableAllocator(), context.getLookup(), logicalRowExpressions);
        Optional<DecorrelatedNode> decorrelated = decorrelator.decorrelateFilters(stripEmptyProject(applyNode.getSubquery(), context), applyNode.getCorrelation());
        if (!decorrelated.isPresent() || !decorrelated.get().getCorrelatedPredicates().isPresent()) {
            return Optional.empty();
        }
        PlanNode decorrelatedNode = decorrelated.get().getNode();
        if (containsUnsupportedGroupedNotEqualSource(decorrelatedNode, context)) {
            return Optional.empty();
        }

        Optional<GroupedNotEqualExists> shape = extractGroupedNotEqualExists(
                decorrelated.get().getCorrelatedPredicates().get(),
                applyNode.getCorrelation(),
                decorrelatedNode.getOutputVariables());
        if (!shape.isPresent()) {
            return Optional.empty();
        }

        VariableReferenceExpression minValue = context.getVariableAllocator().newVariable(
                shape.get().getSubqueryValue().getSourceLocation(),
                "exists_min",
                shape.get().getSubqueryValue().getType());
        VariableReferenceExpression maxValue = context.getVariableAllocator().newVariable(
                shape.get().getSubqueryValue().getSourceLocation(),
                "exists_max",
                shape.get().getSubqueryValue().getType());

        AggregationNode groupedInner = new AggregationNode(
                applyNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                decorrelatedNode,
                ImmutableMap.of(
                        minValue, new Aggregation(
                                new CallExpression(
                                        shape.get().getSubqueryValue().getSourceLocation(),
                                        "min",
                                        functionResolution.minFunction(shape.get().getSubqueryValue().getType()),
                                        shape.get().getSubqueryValue().getType(),
                                        ImmutableList.of(shape.get().getSubqueryValue())),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                Optional.empty()),
                        maxValue, new Aggregation(
                                new CallExpression(
                                        shape.get().getSubqueryValue().getSourceLocation(),
                                        "max",
                                        functionResolution.maxFunction(shape.get().getSubqueryValue().getType()),
                                        shape.get().getSubqueryValue().getType(),
                                        ImmutableList.of(shape.get().getSubqueryValue())),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                Optional.empty())),
                singleGroupingSet(shape.get().getSubqueryKeys()),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        JoinNode join = new JoinNode(
                applyNode.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.LEFT,
                applyNode.getInput(),
                groupedInner,
                shape.get().getClauses(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(applyNode.getInput().getOutputVariables())
                        .add(minValue)
                        .add(maxValue)
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());

        RowExpression minNotEqual = comparisonExpression(functionResolution, NOT_EQUAL, minValue, shape.get().getCorrelationValue());
        RowExpression maxNotEqual = comparisonExpression(functionResolution, NOT_EQUAL, maxValue, shape.get().getCorrelationValue());
        RowExpression existsCondition = specialForm(
                COALESCE,
                BOOLEAN,
                ImmutableList.of(logicalRowExpressions.combineDisjuncts(minNotEqual, maxNotEqual), FALSE_CONSTANT));

        VariableReferenceExpression exists = getOnlyElement(applyNode.getSubqueryAssignments().getVariables());
        Assignments.Builder assignments = Assignments.builder();
        assignments.putAll(identityAssignments(applyNode.getInput().getOutputVariables()));
        assignments.put(exists, existsCondition);

        return Optional.of(new ProjectNode(context.getIdAllocator().getNextId(), join, assignments.build()));
    }

    private boolean containsUnsupportedGroupedNotEqualSource(PlanNode node, Context context)
    {
        return searchFrom(node, context.getLookup())
                .where(planNode -> planNode instanceof LimitNode || planNode instanceof AggregationNode)
                .findFirst()
                .isPresent();
    }

    private PlanNode rewriteToDefaultAggregation(ApplyNode parent, Context context)
    {
        VariableReferenceExpression count = context.getVariableAllocator().newVariable("count", BIGINT);
        VariableReferenceExpression exists = getOnlyElement(parent.getSubqueryAssignments().getVariables());

        return new LateralJoinNode(
                parent.getSourceLocation(),
                parent.getId(),
                parent.getInput(),
                new ProjectNode(
                        context.getIdAllocator().getNextId(),
                        new AggregationNode(
                                parent.getSourceLocation(),
                                context.getIdAllocator().getNextId(),
                                parent.getSubquery(),
                                ImmutableMap.of(count, new Aggregation(
                                        new CallExpression(
                                                exists.getSourceLocation(),
                                                "count",
                                                functionResolution.countFunction(),
                                                BIGINT,
                                                ImmutableList.of()),
                                        Optional.empty(),
                                        Optional.empty(),
                                        false,
                                        Optional.empty())),
                                globalAggregation(),
                                ImmutableList.of(),
                                AggregationNode.Step.SINGLE,
                                Optional.empty(),
                                Optional.empty(),
                                Optional.empty()),
                        Assignments.of(exists, comparisonExpression(functionResolution, GREATER_THAN, count, new ConstantExpression(0L, BIGINT)))),
                parent.getCorrelation(),
                INNER,
                parent.getOriginSubqueryError());
    }

    private PlanNode stripEmptyProject(PlanNode node, Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);
        if (resolved instanceof ProjectNode && ((ProjectNode) resolved).getAssignments().isEmpty()) {
            return stripEmptyProject(((ProjectNode) resolved).getSource(), context);
        }
        return resolved;
    }

    private Optional<GroupedNotEqualExists> extractGroupedNotEqualExists(
            RowExpression correlatedPredicate,
            List<VariableReferenceExpression> correlation,
            List<VariableReferenceExpression> subqueryOutputs)
    {
        ImmutableList.Builder<EquiJoinClause> clauses = ImmutableList.builder();
        ImmutableList.Builder<VariableReferenceExpression> subqueryKeys = ImmutableList.builder();
        Optional<NotEqualPredicate> notEqual = Optional.empty();

        for (RowExpression conjunct : extractConjuncts(correlatedPredicate)) {
            if (!(conjunct instanceof CallExpression) || ((CallExpression) conjunct).getArguments().size() != 2) {
                return Optional.empty();
            }

            CallExpression call = (CallExpression) conjunct;
            Optional<VariableReferenceExpression> left = getVariable(call.getArguments().get(0));
            Optional<VariableReferenceExpression> right = getVariable(call.getArguments().get(1));
            if (!left.isPresent() || !right.isPresent()) {
                return Optional.empty();
            }

            if (functionResolution.isEqualsFunction(call.getFunctionHandle())) {
                Optional<EquiJoinClause> clause = toJoinClause(left.get(), right.get(), correlation, subqueryOutputs);
                if (!clause.isPresent()) {
                    return Optional.empty();
                }
                clauses.add(clause.get());
                subqueryKeys.add(clause.get().getRight());
                continue;
            }

            if (isNotEqualFunction(call)) {
                if (notEqual.isPresent()) {
                    return Optional.empty();
                }
                notEqual = toNotEqualPredicate(left.get(), right.get(), correlation, subqueryOutputs);
                if (!notEqual.isPresent()) {
                    return Optional.empty();
                }
                continue;
            }

            return Optional.empty();
        }

        ImmutableList<EquiJoinClause> builtClauses = clauses.build();
        if (builtClauses.isEmpty() || !notEqual.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new GroupedNotEqualExists(
                builtClauses,
                subqueryKeys.build(),
                notEqual.get().getSubqueryValue(),
                notEqual.get().getCorrelationValue()));
    }

    private boolean isNotEqualFunction(CallExpression call)
    {
        RowExpression left = call.getArguments().get(0);
        RowExpression right = call.getArguments().get(1);
        return left.getType().isComparable()
                && right.getType().isComparable()
                && functionResolution.comparisonFunction(NOT_EQUAL, left.getType(), right.getType()).equals(call.getFunctionHandle());
    }

    private static Optional<VariableReferenceExpression> getVariable(RowExpression expression)
    {
        if (expression instanceof VariableReferenceExpression) {
            return Optional.of((VariableReferenceExpression) expression);
        }
        return Optional.empty();
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

    private static Optional<NotEqualPredicate> toNotEqualPredicate(
            VariableReferenceExpression left,
            VariableReferenceExpression right,
            List<VariableReferenceExpression> correlation,
            List<VariableReferenceExpression> subqueryOutputs)
    {
        boolean leftIsCorrelation = correlation.contains(left);
        boolean rightIsCorrelation = correlation.contains(right);
        boolean leftIsSubquery = subqueryOutputs.contains(left);
        boolean rightIsSubquery = subqueryOutputs.contains(right);

        if (leftIsCorrelation && rightIsSubquery && !rightIsCorrelation && right.getType().isOrderable()) {
            return Optional.of(new NotEqualPredicate(right, left));
        }
        if (rightIsCorrelation && leftIsSubquery && !leftIsCorrelation && left.getType().isOrderable()) {
            return Optional.of(new NotEqualPredicate(left, right));
        }
        return Optional.empty();
    }

    private static class GroupedNotEqualExists
    {
        private final List<EquiJoinClause> clauses;
        private final List<VariableReferenceExpression> subqueryKeys;
        private final VariableReferenceExpression subqueryValue;
        private final VariableReferenceExpression correlationValue;

        private GroupedNotEqualExists(
                List<EquiJoinClause> clauses,
                List<VariableReferenceExpression> subqueryKeys,
                VariableReferenceExpression subqueryValue,
                VariableReferenceExpression correlationValue)
        {
            this.clauses = ImmutableList.copyOf(clauses);
            this.subqueryKeys = ImmutableList.copyOf(subqueryKeys);
            this.subqueryValue = subqueryValue;
            this.correlationValue = correlationValue;
        }

        public List<EquiJoinClause> getClauses()
        {
            return clauses;
        }

        public List<VariableReferenceExpression> getSubqueryKeys()
        {
            return subqueryKeys;
        }

        public VariableReferenceExpression getSubqueryValue()
        {
            return subqueryValue;
        }

        public VariableReferenceExpression getCorrelationValue()
        {
            return correlationValue;
        }
    }

    private static class NotEqualPredicate
    {
        private final VariableReferenceExpression subqueryValue;
        private final VariableReferenceExpression correlationValue;

        private NotEqualPredicate(VariableReferenceExpression subqueryValue, VariableReferenceExpression correlationValue)
        {
            this.subqueryValue = subqueryValue;
            this.correlationValue = correlationValue;
        }

        public VariableReferenceExpression getSubqueryValue()
        {
            return subqueryValue;
        }

        public VariableReferenceExpression getCorrelationValue()
        {
            return correlationValue;
        }
    }
}
