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
import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.SystemSessionProperties.useDefaultsForCorrelatedAggregationPushdownThroughOuterJoins;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.plan.AggregationNode.globalAggregation;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.RowExpressionVariableInliner.inlineVariables;
import static com.facebook.presto.sql.planner.optimizations.AggregationNodeUtils.extractAggregationUniqueVariables;
import static com.facebook.presto.sql.planner.optimizations.DistinctOutputQueryUtil.isDistinct;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.constantNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * This optimizer pushes aggregations below outer joins when: the aggregation
 * is on top of the outer join, it groups by all columns in the outer table, and
 * the outer rows are guaranteed to be distinct.
 * <p>
 * When the aggregation is pushed down, we still need to perform aggregations
 * on the null values that come out of the absent values in an outer
 * join. We add a cross join with a row of aggregations on null literals,
 * and select the aggregation over nulls when a marker shows that the
 * pushed aggregation produced no inner group.
 * <p>
 * Example:
 * <pre>
 * - Filter ("nationkey" > "avg")
 *  - Aggregate(Group by: all columns from the left table, aggregation:
 *    avg("n2.nationkey"))
 *      - LeftJoin("regionkey" = "regionkey")
 *          - AssignUniqueId (nation)
 *              - Tablescan (nation)
 *          - Tablescan (nation)
 * </pre>
 * </p>
 * Is rewritten to:
 * <pre>
 * - Filter ("nationkey" > "avg")
 *  - project(regionkey, if(inner_regionkey is null, "avg_over_null", "avg"))
 *      - CrossJoin
 *          - LeftJoin("regionkey" = "regionkey")
 *              - AssignUniqueId (nation)
 *                  - Tablescan (nation)
 *              - Aggregate(Group by: regionkey, aggregation:
 *                avg(nationkey))
 *                  - Tablescan (nation)
 *          - Aggregate
 *            avg(null_literal)
 *              - Values (null_literal)
 * </pre>
 */
public class PushAggregationThroughOuterJoin
        implements Rule<AggregationNode>
{
    private static final Set<QualifiedObjectName> NULL_FOR_SINGLE_NULL_ROW = ImmutableSet.of(
            QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "avg"),
            QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "max"),
            QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "min"),
            QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "sum"));

    private static final Capture<JoinNode> JOIN = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(join().capturedAs(JOIN)));
    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public PushAggregationThroughOuterJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
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
        JoinNode join = captures.get(JOIN);

        if (aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getGroupingSetCount() != 1
                || !aggregation.getPreGroupedVariables().isEmpty()
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()
                || aggregation.getAggregations().isEmpty()
                || join.getFilter().isPresent()
                || join.getLeftHashVariable().isPresent()
                || join.getRightHashVariable().isPresent()
                || !(join.getType() == JoinType.LEFT || join.getType() == JoinType.RIGHT)
                || join.getCriteria().isEmpty()
                || !groupsOnAllColumns(aggregation, getOuterTable(join).getOutputVariables())
                || !isDistinct(context.getLookup().resolve(getOuterTable(join)), context.getLookup()::resolve)) {
            return Result.empty();
        }

        List<VariableReferenceExpression> groupingKeys = join.getCriteria().stream()
                .map(join.getType() == JoinType.RIGHT ? EquiJoinClause::getLeft : EquiJoinClause::getRight)
                .distinct()
                .collect(toImmutableList());
        Set<VariableReferenceExpression> innerVariables = ImmutableSet.copyOf(getInnerTable(join).getOutputVariables());
        if (aggregation.getAggregations().values().stream().anyMatch(aggregate -> !isSupportedAggregation(aggregate, innerVariables))) {
            return Result.empty();
        }

        // Dynamic filters are built from the right input. For a LEFT join, the right input is replaced by the
        // aggregation, so every dynamic-filter build variable must be retained as one of its grouping keys.
        if (join.getType() == JoinType.LEFT && !ImmutableSet.copyOf(groupingKeys).containsAll(join.getDynamicFilters().values())) {
            return Result.empty();
        }

        // A matching equijoin key cannot be null. Keep one inner key in the join output as a marker that
        // distinguishes a matched null aggregate result from a null-extended unmatched row.
        VariableReferenceExpression innerGroupPresent = groupingKeys.get(0);
        AggregationNode rewrittenAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                aggregation.getId(),
                getInnerTable(join),
                aggregation.getAggregations(),
                singleGroupingSet(groupingKeys),
                ImmutableList.of(),
                aggregation.getStep(),
                aggregation.getHashVariable(),
                aggregation.getGroupIdVariable(),
                aggregation.getAggregationId());

        JoinNode rewrittenJoin;
        if (join.getType() == JoinType.LEFT) {
            rewrittenJoin = new JoinNode(
                    join.getSourceLocation(),
                    join.getId(),
                    join.getType(),
                    join.getLeft(),
                    rewrittenAggregation,
                    join.getCriteria(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(join.getLeft().getOutputVariables())
                            .add(innerGroupPresent)
                            .addAll(rewrittenAggregation.getAggregations().keySet())
                            .build(),
                    join.getFilter(),
                    join.getLeftHashVariable(),
                    join.getRightHashVariable(),
                    join.getDistributionType(),
                    join.getDynamicFilters());
        }
        else {
            rewrittenJoin = new JoinNode(
                    join.getSourceLocation(),
                    join.getId(),
                    join.getType(),
                    rewrittenAggregation,
                    join.getRight(),
                    join.getCriteria(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(rewrittenAggregation.getAggregations().keySet())
                            .add(innerGroupPresent)
                            .addAll(join.getRight().getOutputVariables())
                            .build(),
                    join.getFilter(),
                    join.getLeftHashVariable(),
                    join.getRightHashVariable(),
                    join.getDistributionType(),
                    join.getDynamicFilters());
        }

        Optional<PlanNode> resultNode = restoreAggregationOverNull(
                aggregation,
                rewrittenJoin,
                innerGroupPresent,
                context.getVariableAllocator(),
                context.getIdAllocator(),
                context.getLookup(),
                useDefaultsForCorrelatedAggregationPushdownThroughOuterJoins(context.getSession()));
        if (!resultNode.isPresent()) {
            return Result.empty();
        }

        return Result.ofPlanNode(resultNode.get());
    }

    private boolean isSupportedAggregation(AggregationNode.Aggregation aggregation, Set<VariableReferenceExpression> innerVariables)
    {
        if (aggregation.getArguments().isEmpty()
                || aggregation.getArguments().stream().anyMatch(argument -> !(argument instanceof VariableReferenceExpression))
                || !determinismEvaluator.isDeterministic(aggregation.getCall())
                || aggregation.getFilter().filter(filter -> !determinismEvaluator.isDeterministic(filter)).isPresent()) {
            return false;
        }

        ImmutableSet.Builder<VariableReferenceExpression> dependencies = ImmutableSet.builder();
        dependencies.addAll(extractAggregationUniqueVariables(aggregation));
        aggregation.getMask().ifPresent(dependencies::add);
        return innerVariables.containsAll(dependencies.build());
    }

    private static PlanNode getInnerTable(JoinNode join)
    {
        checkState(join.getType() == JoinType.LEFT || join.getType() == JoinType.RIGHT, "expected LEFT or RIGHT JOIN");
        PlanNode innerNode;
        if (join.getType().equals(JoinType.LEFT)) {
            innerNode = join.getRight();
        }
        else {
            innerNode = join.getLeft();
        }
        return innerNode;
    }

    private static PlanNode getOuterTable(JoinNode join)
    {
        checkState(join.getType() == JoinType.LEFT || join.getType() == JoinType.RIGHT, "expected LEFT or RIGHT JOIN");
        PlanNode outerNode;
        if (join.getType().equals(JoinType.LEFT)) {
            outerNode = join.getLeft();
        }
        else {
            outerNode = join.getRight();
        }
        return outerNode;
    }

    private static boolean groupsOnAllColumns(AggregationNode node, List<VariableReferenceExpression> columns)
    {
        return new HashSet<>(node.getGroupingKeys()).equals(new HashSet<>(columns));
    }

    // An unmatched outer-join row contributes one null-extended row to the original aggregation. Reproduce the
    // aggregate result over that row, and select it only when the pushed inner aggregation produced no group.
    // Testing the marker is necessary because a matched group can itself produce a null aggregate result.
    private Optional<PlanNode> restoreAggregationOverNull(
            AggregationNode aggregationNode,
            PlanNode outerJoin,
            VariableReferenceExpression innerGroupPresent,
            VariableAllocator variableAllocator,
            PlanNodeIdAllocator idAllocator,
            Lookup lookup,
            boolean useDefaultsForCorrelatedAggregations)
    {
        // Create an aggregation node over a row of nulls.
        Optional<MappedAggregationInfo> aggregationOverNullInfoResultNode = createAggregationOverNull(
                aggregationNode,
                variableAllocator,
                idAllocator,
                lookup);

        if (!aggregationOverNullInfoResultNode.isPresent()) {
            return Optional.empty();
        }

        MappedAggregationInfo aggregationOverNullInfo = aggregationOverNullInfoResultNode.get();

        AggregationNode aggregationOverNull = aggregationOverNullInfo.getAggregation();
        Map<VariableReferenceExpression, VariableReferenceExpression> sourceAggregationToOverNullMapping = aggregationOverNullInfo.getVariableMapping();

        Map<VariableReferenceExpression, RowExpression> literalMap = new HashMap<>();
        Set<VariableReferenceExpression> nullResultAggregations = new HashSet<>();

        if (useDefaultsForCorrelatedAggregations) {
            for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> aggregation : aggregationNode.getAggregations().entrySet()) {
                FunctionHandle functionHandle = aggregation.getValue().getFunctionHandle();

                Optional<RowExpression> defaultLiteral = Optional.empty();
                if (functionResolution.isCountFunction(functionHandle)
                        && aggregation.getValue().getArguments().size() == 1
                        && aggregation.getValue().getArguments().get(0) instanceof VariableReferenceExpression) {
                    defaultLiteral = Optional.of(constant(Long.valueOf(0), aggregation.getKey().getType()));
                }
                else if (returnsNullForSingleNullRow(aggregation.getValue())) {
                    nullResultAggregations.add(aggregation.getKey());
                }

                if (defaultLiteral.isPresent()) {
                    literalMap.put(aggregation.getKey(), defaultLiteral.get());
                }
            }
        }

        PlanNode finalJoinNode = outerJoin;
        if (literalMap.size() + nullResultAggregations.size() < aggregationNode.getAggregations().size()) {
            // Do a cross join with the aggregation over null
            finalJoinNode = new JoinNode(
                    outerJoin.getSourceLocation(),
                    idAllocator.getNextId(),
                    JoinType.INNER,
                    outerJoin,
                    aggregationOverNull,
                    ImmutableList.of(),
                    ImmutableList.<VariableReferenceExpression>builder()
                            .addAll(outerJoin.getOutputVariables())
                            .addAll(aggregationOverNull.getOutputVariables())
                            .build(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ImmutableMap.of());
        }

        // Select the aggregate over one null row only for unmatched outer rows.
        Assignments.Builder assignmentsBuilder = Assignments.builder();
        for (VariableReferenceExpression variable : aggregationNode.getOutputVariables()) {
            if (aggregationNode.getAggregations().keySet().contains(variable)) {
                // An unmatched outer row already supplies the correct null result for these aggregates. Keeping
                // the pushed variable unchanged also lets null-rejecting predicates simplify the outer join.
                if (nullResultAggregations.contains(variable)) {
                    assignmentsBuilder.put(variable, variable);
                    continue;
                }
                RowExpression unmatchedValue = literalMap.containsKey(variable) ? literalMap.get(variable) : sourceAggregationToOverNullMapping.get(variable);
                assignmentsBuilder.put(variable, new SpecialFormExpression(
                        IF,
                        variable.getType(),
                        ImmutableList.of(
                                new SpecialFormExpression(IS_NULL, BOOLEAN, ImmutableList.of(innerGroupPresent)),
                                unmatchedValue,
                                variable)));
            }
            else {
                assignmentsBuilder.put(variable, variable);
            }
        }
        return Optional.of(new ProjectNode(idAllocator.getNextId(), finalJoinNode, assignmentsBuilder.build()));
    }

    private boolean returnsNullForSingleNullRow(AggregationNode.Aggregation aggregation)
    {
        return aggregation.getArguments().size() == 1 &&
                NULL_FOR_SINGLE_NULL_ROW.contains(functionAndTypeManager.getFunctionMetadata(aggregation.getFunctionHandle()).getName());
    }

    private Optional<MappedAggregationInfo> createAggregationOverNull(AggregationNode referenceAggregation, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator, Lookup lookup)
    {
        // Create a values node that consists of a single row of nulls.
        // Map the output symbols from the referenceAggregation's source
        // to symbol references for the new values node.
        ImmutableList.Builder<VariableReferenceExpression> nullVariables = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> nullLiterals = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> sourcesVariableMappingBuilder = ImmutableMap.builder();
        for (VariableReferenceExpression sourceVariable : referenceAggregation.getSource().getOutputVariables()) {
            RowExpression nullLiteral = constantNull(sourceVariable.getSourceLocation(), sourceVariable.getType());
            nullLiterals.add(nullLiteral);
            VariableReferenceExpression nullVariable = variableAllocator.newVariable(nullLiteral);
            nullVariables.add(nullVariable);
            // TODO The type should be from sourceVariable.getType
            sourcesVariableMappingBuilder.put(sourceVariable, nullVariable);
        }
        ValuesNode nullRow = new ValuesNode(
                referenceAggregation.getSourceLocation(),
                idAllocator.getNextId(),
                nullVariables.build(),
                ImmutableList.of(nullLiterals.build()),
                Optional.empty());
        Map<VariableReferenceExpression, VariableReferenceExpression> sourcesVariableMapping = sourcesVariableMappingBuilder.build();

        // For each aggregation function in the reference node, create a corresponding aggregation function
        // that points to the nullRow. Map the symbols from the aggregations in referenceAggregation to the
        // symbols in these new aggregations.
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> aggregationsVariableMappingBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> aggregationsOverNullBuilder = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : referenceAggregation.getAggregations().entrySet()) {
            VariableReferenceExpression aggregationVariable = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();

            if (!usesOnlyVariablesFrom(aggregation, sourcesVariableMapping.keySet())) {
                return Optional.empty();
            }

            AggregationNode.Aggregation overNullAggregation = new AggregationNode.Aggregation(
                    new CallExpression(
                            aggregation.getCall().getSourceLocation(),
                            aggregation.getCall().getDisplayName(),
                            aggregation.getCall().getFunctionHandle(),
                            aggregation.getCall().getType(),
                            aggregation.getArguments()
                                    .stream()
                                    .map(argument -> inlineVariables(sourcesVariableMapping, argument))
                                    .collect(toImmutableList())),
                    aggregation.getFilter().map(filter -> inlineVariables(sourcesVariableMapping, filter)),
                    aggregation.getOrderBy().map(orderBy -> inlineOrderByVariables(sourcesVariableMapping, orderBy)),
                    aggregation.isDistinct(),
                    aggregation.getMask().map(x -> new VariableReferenceExpression(sourcesVariableMapping.get(x).getSourceLocation(), sourcesVariableMapping.get(x).getName(), x.getType())));
            QualifiedObjectName functionName = functionAndTypeManager.getFunctionMetadata(overNullAggregation.getFunctionHandle()).getName();
            VariableReferenceExpression overNull = variableAllocator.newVariable(aggregation.getCall().getSourceLocation(), functionName.getObjectName(), aggregationVariable.getType());
            aggregationsOverNullBuilder.put(overNull, overNullAggregation);
            aggregationsVariableMappingBuilder.put(aggregationVariable, overNull);
        }
        Map<VariableReferenceExpression, VariableReferenceExpression> aggregationsSymbolMapping = aggregationsVariableMappingBuilder.build();

        // create an aggregation node whose source is the null row.
        AggregationNode aggregationOverNullRow = new AggregationNode(
                referenceAggregation.getSourceLocation(),
                idAllocator.getNextId(),
                nullRow,
                aggregationsOverNullBuilder.build(),
                globalAggregation(),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        return Optional.of(new MappedAggregationInfo(aggregationOverNullRow, aggregationsSymbolMapping));
    }

    private static OrderingScheme inlineOrderByVariables(Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping, OrderingScheme orderingScheme)
    {
        // This is a logic expanded from ExpressionTreeRewriter::rewriteSortItems
        ImmutableList.Builder<VariableReferenceExpression> orderBy = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, SortOrder> ordering = new ImmutableMap.Builder<>();
        for (VariableReferenceExpression variable : orderingScheme.getOrderByVariables()) {
            VariableReferenceExpression translated = variableMapping.get(variable);
            orderBy.add(translated);
            ordering.put(translated, orderingScheme.getOrdering(variable));
        }

        ImmutableMap<VariableReferenceExpression, SortOrder> orderingMap = ordering.build();
        return new OrderingScheme(orderBy.build().stream().map(variable -> new Ordering(variable, orderingMap.get(variable))).collect(toImmutableList()));
    }

    private static boolean usesOnlyVariablesFrom(AggregationNode.Aggregation aggregation, Set<VariableReferenceExpression> sourceVariables)
    {
        ImmutableSet.Builder<VariableReferenceExpression> inputVariables = ImmutableSet.builder();
        inputVariables.addAll(extractAggregationUniqueVariables(aggregation));
        aggregation.getMask().ifPresent(inputVariables::add);
        return sourceVariables.containsAll(inputVariables.build());
    }

    private static class MappedAggregationInfo
    {
        private final AggregationNode aggregationNode;
        private final Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping;

        public MappedAggregationInfo(AggregationNode aggregationNode, Map<VariableReferenceExpression, VariableReferenceExpression> variableMapping)
        {
            this.aggregationNode = aggregationNode;
            this.variableMapping = variableMapping;
        }

        public Map<VariableReferenceExpression, VariableReferenceExpression> getVariableMapping()
        {
            return variableMapping;
        }

        public AggregationNode getAggregation()
        {
            return aggregationNode;
        }
    }
}
