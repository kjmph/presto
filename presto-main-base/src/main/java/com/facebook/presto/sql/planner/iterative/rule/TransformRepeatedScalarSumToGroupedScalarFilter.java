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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.GroupedScalarFilterNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isNativeExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static java.util.Objects.requireNonNull;

/**
 * Replaces a cross join against a scalar SUM over the same row source with a
 * grouped aggregation that also carries the scalar SUM row.
 *
 * This targets shapes like:
 *
 * <pre>
 * - Project(group_key, grouped_sum)
 *   - Filter(grouped_sum > scalar_sum * factor)
 *     - CrossJoin
 *       - Aggregation(GROUP BY group_key; sum(value) AS grouped_sum)
 *         - source
 *       - Project(threshold := scalar_sum * factor)
 *         - Aggregation(sum(value) AS scalar_sum)
 *           - source
 * </pre>
 *
 * and rewrites them to:
 *
 * <pre>
 * - Project(group_key, grouped_sum)
 *   - GroupedScalarFilter(grouped_sum > scalar_sum * factor)
 *     - Aggregation(GROUP BY group_key; sum(value) AS grouped_sum,
 *         max(group_id) AS output_group_id)
 *       - GroupId((), (group_key))
 *         - source
 * </pre>
 *
 * A later exchange-splitting rule places a nulls-only replicated exchange
 * between partial and final aggregation, so the null-key scalar row is
 * available on every partition without gathering the grouped result on the
 * coordinator or duplicating an arbitrary grouped row.
 */
public class TransformRepeatedScalarSumToGroupedScalarFilter
        implements Rule<PlanNode>
{
    private static final Pattern<PlanNode> PATTERN = typeOf(PlanNode.class);

    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;
    private final RowExpressionDeterminismEvaluator determinismEvaluator;

    public TransformRepeatedScalarSumToGroupedScalarFilter(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
    }

    @Override
    public Pattern<PlanNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isNativeExecutionEnabled(session) && shouldPushAggregationThroughJoin(session);
    }

    @Override
    public Result apply(PlanNode node, Captures captures, Context context)
    {
        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            PlanNode source = context.getLookup().resolve(project.getSource());
            if (!(source instanceof FilterNode)) {
                return Result.empty();
            }

            return tryRewrite((FilterNode) source, context)
                    .flatMap(rewrite -> withProject(project, rewrite, context))
                    .map(Result::ofPlanNode)
                    .orElseGet(Result::empty);
        }

        if (node instanceof FilterNode) {
            return tryRewrite((FilterNode) node, context)
                    .map(Rewrite::getFilter)
                    .map(Result::ofPlanNode)
                    .orElseGet(Result::empty);
        }

        return Result.empty();
    }

    private Optional<Rewrite> tryRewrite(FilterNode filter, Context context)
    {
        PlanNode filterSource = context.getLookup().resolve(filter.getSource());
        if (!(filterSource instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode join = (JoinNode) filterSource;
        if (join.getType() != JoinType.INNER
                || !join.getCriteria().isEmpty()
                || join.getFilter().isPresent()) {
            return Optional.empty();
        }

        Optional<Rewrite> rewritten = tryRewrite(
                filter,
                context.getLookup().resolve(join.getLeft()),
                context.getLookup().resolve(join.getRight()),
                context);
        if (!rewritten.isPresent()) {
            rewritten = tryRewrite(
                    filter,
                    context.getLookup().resolve(join.getRight()),
                    context.getLookup().resolve(join.getLeft()),
                    context);
        }

        return rewritten;
    }

    private Optional<Rewrite> tryRewrite(
            FilterNode filter,
            PlanNode groupedSide,
            PlanNode scalarSide,
            Context context)
    {
        Optional<GroupedSum> grouped = extractGroupedSum(groupedSide);
        Optional<ScalarSumSide> scalar = extractScalarSumSide(scalarSide, context);
        if (!grouped.isPresent() || !scalar.isPresent()) {
            return Optional.empty();
        }
        if (!grouped.get().getSum().getType().equals(scalar.get().getSum().getType())) {
            return Optional.empty();
        }
        if (!sameSourceShape(
                grouped.get().getAggregation().getSource(),
                scalar.get().getAggregation().getSource(),
                grouped.get().getInput(),
                scalar.get().getInput(),
                context)) {
            return Optional.empty();
        }
        if (grouped.get().getAggregation().getGroupingKeys().size() != 1) {
            return Optional.empty();
        }

        VariableReferenceExpression scalarSum = context.getVariableAllocator().newVariable(
                scalar.get().getSum(),
                "reused_scalar_sum");
        VariableReferenceExpression groupId = context.getVariableAllocator().newVariable("groupId", BIGINT);
        VariableReferenceExpression outputGroupId = context.getVariableAllocator().newVariable("group_id", BIGINT);
        VariableReferenceExpression groupingKey = grouped.get().getAggregation().getGroupingKeys().get(0);
        if (!isKnownNonNull(grouped.get().getAggregation().getSource(), groupingKey, context)) {
            return Optional.empty();
        }

        GroupIdNode groupIdNode = new GroupIdNode(
                filter.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                grouped.get().getAggregation().getSource(),
                ImmutableList.of(
                        ImmutableList.of(),
                        ImmutableList.of(groupingKey)),
                ImmutableMap.of(groupingKey, groupingKey),
                ImmutableList.of(grouped.get().getInput()),
                groupId);

        AggregationNode groupedAndScalarAggregation = new AggregationNode(
                filter.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                groupIdNode,
                ImmutableMap.<VariableReferenceExpression, AggregationNode.Aggregation>builder()
                        .put(grouped.get().getSum(), grouped.get().getAggregation().getAggregations().get(grouped.get().getSum()))
                        .put(outputGroupId, new AggregationNode.Aggregation(
                                new CallExpression(
                                        groupId.getSourceLocation(),
                                        "max",
                                        functionAndTypeManager.lookupFunction("max", fromTypes(BIGINT)),
                                        BIGINT,
                                        ImmutableList.of(groupId)),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                Optional.empty()))
                        .build(),
                AggregationNode.singleGroupingSet(ImmutableList.of(groupingKey)),
                ImmutableList.of(),
                SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        Map<VariableReferenceExpression, RowExpression> scalarReplacements = new LinkedHashMap<>();
        scalarReplacements.put(scalar.get().getSum(), scalarSum);
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : scalar.get().getTopAssignments().entrySet()) {
            scalarReplacements.put(
                    entry.getKey(),
                    RowExpressionVariableInliner.inlineVariables(
                            variable -> variable.equals(scalar.get().getSum()) ? scalarSum : variable,
                            entry.getValue()));
        }
        Map<VariableReferenceExpression, RowExpression> replacements = ImmutableMap.copyOf(scalarReplacements);

        RowExpression rewrittenPredicate = and(
                comparisonExpression(functionResolution, EQUAL, outputGroupId, new ConstantExpression(1L, BIGINT)),
                inlineScalarVariables(filter.getPredicate(), replacements));
        if (!availableFrom(rewrittenPredicate, groupedAndScalarAggregation, scalarSum)) {
            return Optional.empty();
        }

        GroupedScalarFilterNode rewrittenFilter = new GroupedScalarFilterNode(
                filter.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                groupedAndScalarAggregation,
                outputGroupId,
                1L,
                0L,
                grouped.get().getSum(),
                scalarSum,
                rewrittenPredicate);

        return Optional.of(new Rewrite(rewrittenFilter, replacements));
    }

    private Optional<PlanNode> withProject(
            ProjectNode project,
            Rewrite rewrite,
            Context context)
    {
        Assignments.Builder assignments = Assignments.builder();
        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : project.getAssignments().entrySet()) {
            RowExpression assignment = inlineScalarVariables(entry.getValue(), rewrite.getScalarReplacements());
            if (!availableFrom(assignment, rewrite.getFilter())) {
                return Optional.empty();
            }
            assignments.put(entry.getKey(), assignment);
        }

        return Optional.of(new ProjectNode(
                project.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                rewrite.getFilter(),
                assignments.build(),
                project.getLocality()));
    }

    private Optional<GroupedSum> extractGroupedSum(PlanNode node)
    {
        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregation = (AggregationNode) node;
        if (aggregation.getGroupingKeys().isEmpty()) {
            return Optional.empty();
        }

        return extractSingleSumAggregation(aggregation)
                .map(sum -> new GroupedSum(aggregation, sum.getOutput(), sum.getInput()));
    }

    private Optional<ScalarSumSide> extractScalarSumSide(PlanNode root, Context context)
    {
        List<ProjectNode> projects = new ArrayList<>();
        while (true) {
            root = context.getLookup().resolve(root);
            if (root instanceof ProjectNode) {
                ProjectNode project = (ProjectNode) root;
                projects.add(project);
                root = project.getSource();
                continue;
            }
            if (root instanceof EnforceSingleRowNode) {
                root = ((EnforceSingleRowNode) root).getSource();
                continue;
            }
            break;
        }

        if (!(root instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregation = (AggregationNode) root;
        if (!aggregation.getGroupingKeys().isEmpty()) {
            return Optional.empty();
        }

        Optional<SumAggregation> sum = extractSingleSumAggregation(aggregation);
        if (!sum.isPresent()) {
            return Optional.empty();
        }

        Map<VariableReferenceExpression, RowExpression> topAssignments = new LinkedHashMap<>();
        topAssignments.put(sum.get().getOutput(), sum.get().getOutput());
        for (int i = projects.size() - 1; i >= 0; i--) {
            Map<VariableReferenceExpression, RowExpression> projectAssignments = projects.get(i).getAssignments().getMap();
            Map<VariableReferenceExpression, RowExpression> currentAssignments = topAssignments;
            Map<VariableReferenceExpression, RowExpression> rewrittenAssignments = new LinkedHashMap<>(currentAssignments);
            for (Map.Entry<VariableReferenceExpression, RowExpression> entry : projectAssignments.entrySet()) {
                rewrittenAssignments.put(
                        entry.getKey(),
                        RowExpressionVariableInliner.inlineVariables(
                                variable -> currentAssignments.getOrDefault(variable, variable),
                                entry.getValue()));
            }
            topAssignments = rewrittenAssignments;
        }
        return Optional.of(new ScalarSumSide(aggregation, sum.get().getOutput(), sum.get().getInput(), topAssignments));
    }

    private Optional<SumAggregation> extractSingleSumAggregation(AggregationNode aggregation)
    {
        if (aggregation.getAggregations().size() != 1
                || aggregation.getGroupingSetCount() != 1
                || aggregation.getStep() != SINGLE
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()) {
            return Optional.empty();
        }

        Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry =
                aggregation.getAggregations().entrySet().iterator().next();
        AggregationNode.Aggregation sum = entry.getValue();
        if (sum.getArguments().size() != 1
                || sum.isDistinct()
                || sum.getFilter().isPresent()
                || sum.getMask().isPresent()
                || sum.getOrderBy().isPresent()
                || !(sum.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }

        VariableReferenceExpression input = (VariableReferenceExpression) sum.getArguments().get(0);
        try {
            if (!functionAndTypeManager.lookupFunction("sum", fromTypes(input.getType())).equals(sum.getFunctionHandle())) {
                return Optional.empty();
            }
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode())) {
                return Optional.empty();
            }
            throw e;
        }

        return Optional.of(new SumAggregation(entry.getKey(), input));
    }

    private RowExpression inlineScalarVariables(
            RowExpression expression,
            Map<VariableReferenceExpression, RowExpression> replacements)
    {
        return RowExpressionVariableInliner.inlineVariables(
                variable -> replacements.getOrDefault(variable, variable),
                expression);
    }

    private boolean availableFrom(RowExpression expression, PlanNode source)
    {
        Set<VariableReferenceExpression> available = ImmutableSet.copyOf(source.getOutputVariables());
        return available.containsAll(VariablesExtractor.extractUnique(expression));
    }

    private boolean availableFrom(RowExpression expression, PlanNode source, VariableReferenceExpression extraVariable)
    {
        Set<VariableReferenceExpression> available = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(source.getOutputVariables())
                .add(extraVariable)
                .build();
        return available.containsAll(VariablesExtractor.extractUnique(expression));
    }

    private boolean isKnownNonNull(PlanNode source, VariableReferenceExpression variable, Context context)
    {
        source = context.getLookup().resolve(source);
        if (!source.getOutputVariables().contains(variable)) {
            return false;
        }

        double nullsFraction = context.getStatsProvider().getStats(source).getVariableStatistics(variable).getNullsFraction();
        if (!Double.isNaN(nullsFraction) && nullsFraction == 0.0) {
            return true;
        }

        if (source instanceof ProjectNode) {
            RowExpression assignment = ((ProjectNode) source).getAssignments().get(variable);
            return assignment instanceof VariableReferenceExpression &&
                    isKnownNonNull(((ProjectNode) source).getSource(), (VariableReferenceExpression) assignment, context);
        }

        if (source instanceof FilterNode) {
            FilterNode filter = (FilterNode) source;
            return isNotNullPredicate(filter.getPredicate(), variable) ||
                    isKnownNonNull(filter.getSource(), variable, context);
        }

        if (source instanceof JoinNode) {
            JoinNode join = (JoinNode) source;
            boolean fromLeft = join.getLeft().getOutputVariables().contains(variable);
            boolean fromRight = join.getRight().getOutputVariables().contains(variable);
            if (join.getType() == JoinType.INNER) {
                return (fromLeft && isKnownNonNull(join.getLeft(), variable, context)) ||
                        (fromRight && isKnownNonNull(join.getRight(), variable, context));
            }
            if (join.getType() == JoinType.LEFT && fromLeft) {
                return isKnownNonNull(join.getLeft(), variable, context);
            }
            if (join.getType() == JoinType.RIGHT && fromRight) {
                return isKnownNonNull(join.getRight(), variable, context);
            }
            return false;
        }

        if (source instanceof TableScanNode) {
            TableScanNode scan = (TableScanNode) source;
            ColumnHandle column = scan.getAssignments().get(variable);
            return column != null && scan.getTableConstraints().stream()
                    .anyMatch(constraint -> (constraint.isEnabled() || constraint.isRely()) &&
                            (constraint instanceof NotNullConstraint || constraint instanceof PrimaryKeyConstraint) &&
                            constraint.getColumns().contains(column));
        }

        return false;
    }

    private boolean isNotNullPredicate(RowExpression predicate, VariableReferenceExpression variable)
    {
        List<RowExpression> conjuncts = new ArrayList<>();
        collectConjuncts(predicate, conjuncts);
        return conjuncts.stream().anyMatch(conjunct -> isNotNullConjunct(conjunct, variable));
    }

    private void collectConjuncts(RowExpression expression, List<RowExpression> conjuncts)
    {
        if (expression instanceof SpecialFormExpression
                && ((SpecialFormExpression) expression).getForm() == SpecialFormExpression.Form.AND) {
            for (RowExpression argument : ((SpecialFormExpression) expression).getArguments()) {
                collectConjuncts(argument, conjuncts);
            }
            return;
        }
        conjuncts.add(expression);
    }

    private boolean isNotNullConjunct(RowExpression expression, VariableReferenceExpression variable)
    {
        if (!(expression instanceof CallExpression)) {
            return false;
        }
        CallExpression call = (CallExpression) expression;
        if (!functionResolution.isNotFunction(call.getFunctionHandle())
                || call.getArguments().size() != 1
                || !(call.getArguments().get(0) instanceof SpecialFormExpression)) {
            return false;
        }

        SpecialFormExpression isNull = (SpecialFormExpression) call.getArguments().get(0);
        return isNull.getForm() == SpecialFormExpression.Form.IS_NULL
                && isNull.getArguments().size() == 1
                && isNull.getArguments().get(0).equals(variable);
    }

    private static class Rewrite
    {
        private final PlanNode filter;
        private final Map<VariableReferenceExpression, RowExpression> scalarReplacements;

        private Rewrite(
                PlanNode filter,
                Map<VariableReferenceExpression, RowExpression> scalarReplacements)
        {
            this.filter = requireNonNull(filter, "filter is null");
            this.scalarReplacements = ImmutableMap.copyOf(requireNonNull(scalarReplacements, "scalarReplacements is null"));
        }

        public PlanNode getFilter()
        {
            return filter;
        }

        public Map<VariableReferenceExpression, RowExpression> getScalarReplacements()
        {
            return scalarReplacements;
        }
    }

    private boolean sameSourceShape(
            PlanNode groupedSource,
            PlanNode scalarSource,
            VariableReferenceExpression groupedInput,
            VariableReferenceExpression scalarInput,
            Context context)
    {
        Optional<SourceSignature> grouped = sourceSignature(groupedSource, context);
        Optional<SumMeasureSignature> scalar = sumMeasureSignature(scalarSource, scalarInput, context);
        if (!grouped.isPresent() || !scalar.isPresent() || !grouped.get().getRows().equals(scalar.get().getRows())) {
            return false;
        }

        ExpressionKey groupedExpression = grouped.get().getOutputs().get(groupedInput);
        ExpressionKey scalarExpression = scalar.get().getExpression();
        return groupedExpression != null && groupedExpression.equals(scalarExpression);
    }

    private Optional<SumMeasureSignature> sumMeasureSignature(
            PlanNode node,
            VariableReferenceExpression input,
            Context context)
    {
        node = context.getLookup().resolve(node);

        if (node instanceof ProjectNode) {
            return sumMeasureSignatureForProject((ProjectNode) node, input, context);
        }

        if (node instanceof FilterNode) {
            return sumMeasureSignatureForFilter((FilterNode) node, input, context);
        }

        if (node instanceof AggregationNode) {
            return sumMeasureSignatureForAggregation((AggregationNode) node, input, context);
        }

        if (node instanceof JoinNode) {
            return sumMeasureSignatureForJoin((JoinNode) node, input, context);
        }

        Optional<SourceSignature> source = sourceSignature(node, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        ExpressionKey expression = source.get().getOutputs().get(input);
        if (expression == null) {
            return Optional.empty();
        }
        return Optional.of(new SumMeasureSignature(
                source.get().getRows(),
                expression,
                source.get().getOutputs()));
    }

    private Optional<SumMeasureSignature> sumMeasureSignatureForProject(
            ProjectNode project,
            VariableReferenceExpression input,
            Context context)
    {
        RowExpression assignment = project.getAssignments().get(input);
        if (assignment == null) {
            return Optional.empty();
        }

        Optional<SourceSignature> source = sourceSignature(project.getSource(), context);
        if (source.isPresent()) {
            Optional<ExpressionKey> expression = expressionKey(assignment, source.get().getOutputs());
            if (!expression.isPresent()) {
                return Optional.empty();
            }

            Optional<SourceSignature> projected = sourceSignature(project, context);
            if (!projected.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(new SumMeasureSignature(
                    projected.get().getRows(),
                    expression.get(),
                    projected.get().getOutputs()));
        }

        if (!(assignment instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }

        Optional<SumMeasureSignature> measure = sumMeasureSignature(
                project.getSource(),
                (VariableReferenceExpression) assignment,
                context);
        if (!measure.isPresent()) {
            return Optional.empty();
        }

        Optional<Map<VariableReferenceExpression, ExpressionKey>> outputs = projectOutputs(project, measure.get().getOutputs());
        if (!outputs.isPresent()) {
            return Optional.empty();
        }
        return Optional.of(new SumMeasureSignature(
                measure.get().getRows(),
                measure.get().getExpression(),
                outputs.get()));
    }

    private Optional<SumMeasureSignature> sumMeasureSignatureForFilter(
            FilterNode filter,
            VariableReferenceExpression input,
            Context context)
    {
        Optional<SumMeasureSignature> source = sumMeasureSignature(filter.getSource(), input, context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        Optional<ExpressionKey> predicate = expressionKey(filter.getPredicate(), source.get().getOutputs());
        if (!predicate.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new SumMeasureSignature(
                SignatureKey.of("Filter", source.get().getRows(), predicate.get()),
                source.get().getExpression(),
                outputsFor(filter, source.get().getOutputs())));
    }

    private Optional<SumMeasureSignature> sumMeasureSignatureForAggregation(
            AggregationNode aggregation,
            VariableReferenceExpression input,
            Context context)
    {
        if (aggregation.getGroupingSetCount() != 1
                || aggregation.getStep() != SINGLE
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()) {
            return Optional.empty();
        }

        AggregationNode.Aggregation sum = aggregation.getAggregations().get(input);
        if (sum == null) {
            return Optional.empty();
        }

        Optional<VariableReferenceExpression> sumInput = extractSumInput(sum);
        if (!sumInput.isPresent()) {
            return Optional.empty();
        }

        Optional<SumMeasureSignature> source = sumMeasureSignature(aggregation.getSource(), sumInput.get(), context);
        if (!source.isPresent()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<VariableReferenceExpression, ExpressionKey> outputs = ImmutableMap.builder();
        for (VariableReferenceExpression groupingKey : aggregation.getGroupingKeys()) {
            ExpressionKey groupingKeyExpression = source.get().getOutputs().get(groupingKey);
            if (groupingKeyExpression == null) {
                return Optional.empty();
            }
            outputs.put(groupingKey, groupingKeyExpression);
        }
        outputs.put(input, source.get().getExpression());

        return Optional.of(new SumMeasureSignature(
                source.get().getRows(),
                source.get().getExpression(),
                outputs.build()));
    }

    private Optional<SumMeasureSignature> sumMeasureSignatureForJoin(
            JoinNode join,
            VariableReferenceExpression input,
            Context context)
    {
        if (join.getType() != JoinType.INNER) {
            return Optional.empty();
        }

        PlanNode left = context.getLookup().resolve(join.getLeft());
        PlanNode right = context.getLookup().resolve(join.getRight());
        boolean fromLeft = left.getOutputVariables().contains(input);
        boolean fromRight = right.getOutputVariables().contains(input);
        if (fromLeft == fromRight) {
            return Optional.empty();
        }

        if (fromLeft) {
            Optional<SumMeasureSignature> measure = sumMeasureSignature(left, input, context);
            Optional<SourceSignature> other = sourceSignature(right, context);
            if (!measure.isPresent() || !other.isPresent()) {
                return Optional.empty();
            }
            return joinMeasureSignature(join, measure.get(), other.get(), true);
        }

        Optional<SourceSignature> other = sourceSignature(left, context);
        Optional<SumMeasureSignature> measure = sumMeasureSignature(right, input, context);
        if (!measure.isPresent() || !other.isPresent()) {
            return Optional.empty();
        }
        return joinMeasureSignature(join, measure.get(), other.get(), false);
    }

    private Optional<SumMeasureSignature> joinMeasureSignature(
            JoinNode join,
            SumMeasureSignature measure,
            SourceSignature other,
            boolean measureOnLeft)
    {
        ImmutableMap.Builder<VariableReferenceExpression, ExpressionKey> inputsBuilder = ImmutableMap.builder();
        if (measureOnLeft) {
            inputsBuilder.putAll(measure.getOutputs());
            inputsBuilder.putAll(other.getOutputs());
        }
        else {
            inputsBuilder.putAll(other.getOutputs());
            inputsBuilder.putAll(measure.getOutputs());
        }
        Map<VariableReferenceExpression, ExpressionKey> inputs = inputsBuilder.build();

        List<SignatureKey> criteria = new ArrayList<>();
        for (EquiJoinClause clause : join.getCriteria()) {
            ExpressionKey leftKey = inputs.get(clause.getLeft());
            ExpressionKey rightKey = inputs.get(clause.getRight());
            if (leftKey == null || rightKey == null) {
                return Optional.empty();
            }
            criteria.add(SignatureKey.of("EquiJoinClause", unorderedList(leftKey, rightKey)));
        }

        Optional<ExpressionKey> filter = Optional.empty();
        if (join.getFilter().isPresent()) {
            filter = expressionKey(join.getFilter().get(), inputs);
            if (!filter.isPresent()) {
                return Optional.empty();
            }
        }

        return Optional.of(new SumMeasureSignature(
                joinSignature(
                        join.getType(),
                        measureOnLeft ? measure.getRows() : other.getRows(),
                        measureOnLeft ? other.getRows() : measure.getRows(),
                        criteria,
                        filter),
                measure.getExpression(),
                outputsFor(join, inputs)));
    }

    private Optional<Map<VariableReferenceExpression, ExpressionKey>> projectOutputs(
            ProjectNode project,
            Map<VariableReferenceExpression, ExpressionKey> inputs)
    {
        ImmutableMap.Builder<VariableReferenceExpression, ExpressionKey> outputs = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : project.getAssignments().entrySet()) {
            Optional<ExpressionKey> expression = expressionKey(assignment.getValue(), inputs);
            if (!expression.isPresent()) {
                return Optional.empty();
            }
            outputs.put(assignment.getKey(), expression.get());
        }
        return Optional.of(outputs.build());
    }

    private Optional<VariableReferenceExpression> extractSumInput(AggregationNode.Aggregation sum)
    {
        if (sum.getArguments().size() != 1
                || sum.isDistinct()
                || sum.getFilter().isPresent()
                || sum.getMask().isPresent()
                || sum.getOrderBy().isPresent()
                || !(sum.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }

        VariableReferenceExpression input = (VariableReferenceExpression) sum.getArguments().get(0);
        try {
            if (!functionAndTypeManager.lookupFunction("sum", fromTypes(input.getType())).equals(sum.getFunctionHandle())) {
                return Optional.empty();
            }
        }
        catch (PrestoException e) {
            if (e.getErrorCode().equals(StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode())) {
                return Optional.empty();
            }
            throw e;
        }
        return Optional.of(input);
    }

    private Optional<SourceSignature> sourceSignature(PlanNode node, Context context)
    {
        node = context.getLookup().resolve(node);
        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            Optional<SourceSignature> source = sourceSignature(project.getSource(), context);
            if (!source.isPresent()) {
                return Optional.empty();
            }

            ImmutableMap.Builder<VariableReferenceExpression, ExpressionKey> outputs = ImmutableMap.builder();
            for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : project.getAssignments().entrySet()) {
                Optional<ExpressionKey> expression = expressionKey(assignment.getValue(), source.get().getOutputs());
                if (!expression.isPresent()) {
                    return Optional.empty();
                }
                outputs.put(assignment.getKey(), expression.get());
            }
            return Optional.of(new SourceSignature(source.get().getRows(), outputs.build()));
        }

        if (node instanceof FilterNode) {
            FilterNode filter = (FilterNode) node;
            Optional<SourceSignature> source = sourceSignature(filter.getSource(), context);
            if (!source.isPresent()) {
                return Optional.empty();
            }
            Optional<ExpressionKey> predicate = expressionKey(filter.getPredicate(), source.get().getOutputs());
            if (!predicate.isPresent()) {
                return Optional.empty();
            }
            return Optional.of(new SourceSignature(
                    SignatureKey.of("Filter", source.get().getRows(), predicate.get()),
                    outputsFor(node, source.get().getOutputs())));
        }

        if (node instanceof TableScanNode) {
            TableScanNode scan = (TableScanNode) node;
            ImmutableMap.Builder<VariableReferenceExpression, ExpressionKey> outputs = ImmutableMap.builder();
            for (VariableReferenceExpression output : scan.getOutputVariables()) {
                ColumnHandle column = scan.getAssignments().get(output);
                if (column == null) {
                    return Optional.empty();
                }
                outputs.put(output, ExpressionKey.of(
                        "Column",
                        output.getType().getTypeSignature(),
                        scan.getTable(),
                        column));
            }
            return Optional.of(new SourceSignature(
                    SignatureKey.of(
                            "TableScan",
                            scan.getTable(),
                            Optional.ofNullable(scan.getCurrentConstraint()),
                            Optional.ofNullable(scan.getEnforcedConstraint())),
                    outputs.build()));
        }

        if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            Optional<SourceSignature> left = sourceSignature(join.getLeft(), context);
            Optional<SourceSignature> right = sourceSignature(join.getRight(), context);
            if (!left.isPresent() || !right.isPresent()) {
                return Optional.empty();
            }

            ImmutableMap<VariableReferenceExpression, ExpressionKey> inputs = ImmutableMap.<VariableReferenceExpression, ExpressionKey>builder()
                    .putAll(left.get().getOutputs())
                    .putAll(right.get().getOutputs())
                    .build();

            List<SignatureKey> criteria = new ArrayList<>();
            for (EquiJoinClause clause : join.getCriteria()) {
                ExpressionKey leftKey = inputs.get(clause.getLeft());
                ExpressionKey rightKey = inputs.get(clause.getRight());
                if (leftKey == null || rightKey == null) {
                    return Optional.empty();
                }
                criteria.add(SignatureKey.of("EquiJoinClause", unorderedList(leftKey, rightKey)));
            }

            Optional<ExpressionKey> filter = Optional.empty();
            if (join.getFilter().isPresent()) {
                filter = expressionKey(join.getFilter().get(), inputs);
                if (!filter.isPresent()) {
                    return Optional.empty();
                }
            }

            return Optional.of(new SourceSignature(
                    joinSignature(join.getType(), left.get().getRows(), right.get().getRows(), criteria, filter),
                    outputsFor(node, inputs)));
        }

        return Optional.empty();
    }

    private ImmutableMap<VariableReferenceExpression, ExpressionKey> outputsFor(
            PlanNode node,
            Map<VariableReferenceExpression, ExpressionKey> inputs)
    {
        ImmutableMap.Builder<VariableReferenceExpression, ExpressionKey> outputs = ImmutableMap.builder();
        for (VariableReferenceExpression output : node.getOutputVariables()) {
            ExpressionKey expression = inputs.get(output);
            if (expression != null) {
                outputs.put(output, expression);
            }
        }
        return outputs.build();
    }

    private static UnorderedList unorderedList(Object... elements)
    {
        return new UnorderedList(ImmutableList.copyOf(elements));
    }

    private static UnorderedList unorderedList(List<?> elements)
    {
        return new UnorderedList(elements);
    }

    private static SignatureKey joinSignature(
            JoinType joinType,
            SignatureKey leftRows,
            SignatureKey rightRows,
            List<SignatureKey> criteria,
            Optional<ExpressionKey> filter)
    {
        if (joinType != JoinType.INNER) {
            return SignatureKey.of(
                    "Join",
                    joinType,
                    leftRows,
                    rightRows,
                    unorderedList(criteria),
                    filter);
        }

        List<SignatureKey> sources = new ArrayList<>();
        List<SignatureKey> allCriteria = new ArrayList<>();
        List<ExpressionKey> filters = new ArrayList<>();
        collectInnerJoinParts(leftRows, sources, allCriteria, filters);
        collectInnerJoinParts(rightRows, sources, allCriteria, filters);
        allCriteria.addAll(criteria);
        filter.ifPresent(filters::add);

        return SignatureKey.of(
                "InnerJoin",
                unorderedList(sources),
                unorderedList(allCriteria),
                unorderedList(filters));
    }

    private static void collectInnerJoinParts(
            SignatureKey rows,
            List<SignatureKey> sources,
            List<SignatureKey> criteria,
            List<ExpressionKey> filters)
    {
        if (!rows.kind.equals("InnerJoin")) {
            sources.add(rows);
            return;
        }

        ((UnorderedList) rows.parts.get(0)).addElementsTo(sources);
        ((UnorderedList) rows.parts.get(1)).addElementsTo(criteria);
        ((UnorderedList) rows.parts.get(2)).addElementsTo(filters);
    }

    private Optional<ExpressionKey> expressionKey(
            RowExpression expression,
            Map<VariableReferenceExpression, ExpressionKey> inputs)
    {
        if (!determinismEvaluator.isDeterministic(expression)) {
            return Optional.empty();
        }
        try {
            return Optional.of(expression.accept(new ExpressionKeyVisitor(functionAndTypeManager), inputs));
        }
        catch (UnsupportedOperationException ignored) {
            return Optional.empty();
        }
    }

    private static class UnorderedList
    {
        private final List<Object> elements;

        private UnorderedList(List<?> elements)
        {
            this.elements = ImmutableList.copyOf(requireNonNull(elements, "elements is null"));
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

            UnorderedList that = (UnorderedList) o;
            if (elements.size() != that.elements.size()) {
                return false;
            }

            List<Object> unmatched = new ArrayList<>(that.elements);
            for (Object element : elements) {
                int index = firstMatchingIndex(unmatched, element);
                if (index < 0) {
                    return false;
                }
                unmatched.remove(index);
            }
            return unmatched.isEmpty();
        }

        @Override
        public int hashCode()
        {
            long sum = 0;
            long sumOfSquares = 0;
            int xor = 0;
            for (Object element : elements) {
                int hash = Objects.hashCode(element);
                sum += hash;
                sumOfSquares += (long) hash * hash;
                xor ^= hash;
            }
            return Objects.hash(elements.size(), sum, sumOfSquares, xor);
        }

        private static int firstMatchingIndex(List<Object> elements, Object expected)
        {
            for (int i = 0; i < elements.size(); i++) {
                if (Objects.equals(elements.get(i), expected)) {
                    return i;
                }
            }
            return -1;
        }

        @SuppressWarnings("unchecked")
        private <T> void addElementsTo(List<T> target)
        {
            target.addAll((List<? extends T>) elements);
        }
    }

    private static class SourceSignature
    {
        private final SignatureKey rows;
        private final Map<VariableReferenceExpression, ExpressionKey> outputs;

        private SourceSignature(SignatureKey rows, Map<VariableReferenceExpression, ExpressionKey> outputs)
        {
            this.rows = requireNonNull(rows, "rows is null");
            this.outputs = ImmutableMap.copyOf(requireNonNull(outputs, "outputs is null"));
        }

        public SignatureKey getRows()
        {
            return rows;
        }

        public Map<VariableReferenceExpression, ExpressionKey> getOutputs()
        {
            return outputs;
        }
    }

    private static class SumMeasureSignature
    {
        private final SignatureKey rows;
        private final ExpressionKey expression;
        private final Map<VariableReferenceExpression, ExpressionKey> outputs;

        private SumMeasureSignature(
                SignatureKey rows,
                ExpressionKey expression,
                Map<VariableReferenceExpression, ExpressionKey> outputs)
        {
            this.rows = requireNonNull(rows, "rows is null");
            this.expression = requireNonNull(expression, "expression is null");
            this.outputs = ImmutableMap.copyOf(requireNonNull(outputs, "outputs is null"));
        }

        public SignatureKey getRows()
        {
            return rows;
        }

        public ExpressionKey getExpression()
        {
            return expression;
        }

        public Map<VariableReferenceExpression, ExpressionKey> getOutputs()
        {
            return outputs;
        }
    }

    private static class SignatureKey
    {
        private final String kind;
        private final List<Object> parts;

        private SignatureKey(String kind, Object... parts)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.parts = ImmutableList.copyOf(requireNonNull(parts, "parts is null"));
        }

        public static SignatureKey of(String kind, Object... parts)
        {
            return new SignatureKey(kind, parts);
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
            SignatureKey that = (SignatureKey) o;
            return Objects.equals(kind, that.kind) &&
                    Objects.equals(parts, that.parts);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, parts);
        }
    }

    private static class ExpressionKey
    {
        private final String kind;
        private final TypeSignature type;
        private final List<Object> parts;

        private ExpressionKey(String kind, TypeSignature type, Object... parts)
        {
            this.kind = requireNonNull(kind, "kind is null");
            this.type = requireNonNull(type, "type is null");
            this.parts = ImmutableList.copyOf(requireNonNull(parts, "parts is null"));
        }

        public static ExpressionKey of(String kind, TypeSignature type, Object... parts)
        {
            return new ExpressionKey(kind, type, parts);
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
            ExpressionKey that = (ExpressionKey) o;
            return Objects.equals(kind, that.kind) &&
                    Objects.equals(type, that.type) &&
                    Objects.equals(parts, that.parts);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(kind, type, parts);
        }
    }

    private static class ExpressionKeyVisitor
            implements RowExpressionVisitor<ExpressionKey, Map<VariableReferenceExpression, ExpressionKey>>
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        private ExpressionKeyVisitor(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        }

        @Override
        public ExpressionKey visitVariableReference(
                VariableReferenceExpression reference,
                Map<VariableReferenceExpression, ExpressionKey> inputs)
        {
            ExpressionKey expression = inputs.get(reference);
            if (expression == null) {
                throw new UnsupportedOperationException("Unknown variable: " + reference);
            }
            return expression;
        }

        @Override
        public ExpressionKey visitConstant(
                ConstantExpression literal,
                Map<VariableReferenceExpression, ExpressionKey> inputs)
        {
            Object value = literal.getValue();
            if (value instanceof Block || (value != null && value.getClass().isArray())) {
                throw new UnsupportedOperationException("Unsupported constant value: " + value.getClass().getName());
            }
            return ExpressionKey.of(
                    "Constant",
                    literal.getType().getTypeSignature(),
                    Optional.ofNullable(value));
        }

        @Override
        public ExpressionKey visitCall(
                CallExpression call,
                Map<VariableReferenceExpression, ExpressionKey> inputs)
        {
            QualifiedObjectName functionName = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getName();
            List<ExpressionKey> arguments = call.getArguments().stream()
                    .map(argument -> argument.accept(this, inputs))
                    .collect(ImmutableList.toImmutableList());
            if (functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getOperatorType().map(EQUAL::equals).orElse(false)
                    && arguments.size() == 2) {
                return ExpressionKey.of(
                        "Call",
                        call.getType().getTypeSignature(),
                        functionName,
                        unorderedList(arguments));
            }
            return ExpressionKey.of(
                    "Call",
                    call.getType().getTypeSignature(),
                    functionName,
                    ImmutableList.copyOf(arguments));
        }

        @Override
        public ExpressionKey visitSpecialForm(
                SpecialFormExpression specialForm,
                Map<VariableReferenceExpression, ExpressionKey> inputs)
        {
            List<ExpressionKey> arguments = specialForm.getArguments().stream()
                    .map(argument -> argument.accept(this, inputs))
                    .collect(ImmutableList.toImmutableList());
            if (specialForm.getForm() == SpecialFormExpression.Form.AND) {
                return ExpressionKey.of(
                        "SpecialForm",
                        specialForm.getType().getTypeSignature(),
                        specialForm.getForm(),
                        unorderedList(arguments));
            }
            return ExpressionKey.of(
                    "SpecialForm",
                    specialForm.getType().getTypeSignature(),
                    specialForm.getForm(),
                    ImmutableList.copyOf(arguments));
        }
    }

    private static class SumAggregation
    {
        private final VariableReferenceExpression output;
        private final VariableReferenceExpression input;

        private SumAggregation(VariableReferenceExpression output, VariableReferenceExpression input)
        {
            this.output = output;
            this.input = input;
        }

        public VariableReferenceExpression getOutput()
        {
            return output;
        }

        public VariableReferenceExpression getInput()
        {
            return input;
        }
    }

    private static class GroupedSum
    {
        private final AggregationNode aggregation;
        private final VariableReferenceExpression sum;
        private final VariableReferenceExpression input;

        private GroupedSum(AggregationNode aggregation, VariableReferenceExpression sum, VariableReferenceExpression input)
        {
            this.aggregation = aggregation;
            this.sum = sum;
            this.input = input;
        }

        public AggregationNode getAggregation()
        {
            return aggregation;
        }

        public VariableReferenceExpression getSum()
        {
            return sum;
        }

        public VariableReferenceExpression getInput()
        {
            return input;
        }
    }

    private static class ScalarSumSide
    {
        private final AggregationNode aggregation;
        private final VariableReferenceExpression sum;
        private final VariableReferenceExpression input;
        private final Map<VariableReferenceExpression, RowExpression> topAssignments;

        private ScalarSumSide(
                AggregationNode aggregation,
                VariableReferenceExpression sum,
                VariableReferenceExpression input,
                Map<VariableReferenceExpression, RowExpression> topAssignments)
        {
            this.aggregation = aggregation;
            this.sum = sum;
            this.input = input;
            this.topAssignments = topAssignments;
        }

        public AggregationNode getAggregation()
        {
            return aggregation;
        }

        public VariableReferenceExpression getSum()
        {
            return sum;
        }

        public VariableReferenceExpression getInput()
        {
            return input;
        }

        public Map<VariableReferenceExpression, RowExpression> getTopAssignments()
        {
            return topAssignments;
        }
    }
}
