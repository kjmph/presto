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
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.RowExpressionVariableInliner;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.spi.plan.WindowNode.Frame.WindowType.ROWS;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Replaces a join against a grouped MIN over the same row source with a window
 * MIN over the already-filtered row stream.
 *
 * This targets shapes produced by decorrelating predicates like:
 *
 * <pre>
 *   row.value = (
 *       SELECT min(value)
 *       FROM same_row_source
 *       WHERE same_row_source.key = row.key
 *         AND lookup filters...
 * </pre>
 *
 * after the planner has materialized the subquery as a grouped aggregation and
 * joined raw rows back to the grouped MIN:
 *
 * <pre>
 * - Join(raw.key = dim.key AND raw.value = min_value)
 *   - raw
 *   - Join(grouped_min.key = dim.key)
 *     - Aggregation(GROUP BY key; min(value))
 *       - filtered raw source
 *     - dim
 * </pre>
 *
 * The rewrite uses the filtered raw source from the MIN side, joins it to the
 * dimension side, computes min(value) over key, then filters value = min(value).
 * It fires only when the raw scan and the MIN input scan are the same table and
 * columns.
 */
public class TransformMinAggregationJoinToWindow
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = typeOf(JoinNode.class);
    private static final WindowNode.Frame FULL_ROWS_FRAME = new WindowNode.Frame(
            ROWS,
            UNBOUNDED_PRECEDING,
            Optional.empty(),
            Optional.empty(),
            UNBOUNDED_FOLLOWING,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            Optional.empty());

    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public TransformMinAggregationJoinToWindow(FunctionAndTypeManager functionAndTypeManager)
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
        if (join.getType() != JoinType.INNER || join.getFilter().isPresent() || join.getCriteria().size() != 2) {
            return Result.empty();
        }

        Optional<PlanNode> rewritten = tryCreateRewrite(
                join,
                context.getLookup().resolve(join.getLeft()),
                context.getLookup().resolve(join.getRight()),
                true,
                context);
        if (!rewritten.isPresent()) {
            rewritten = tryCreateRewrite(
                    join,
                    context.getLookup().resolve(join.getRight()),
                    context.getLookup().resolve(join.getLeft()),
                    false,
                    context);
        }

        return rewritten.map(Result::ofPlanNode).orElseGet(Result::empty);
    }

    private Optional<PlanNode> tryCreateRewrite(
            JoinNode join,
            PlanNode rawSide,
            PlanNode minDimensionSide,
            boolean rawOnLeft,
            Context context)
    {
        Optional<MinDimensionJoin> minDimensionJoin = extractMinDimensionJoin(minDimensionSide, context);
        if (!minDimensionJoin.isPresent()) {
            return Optional.empty();
        }

        Optional<JoinKeys> joinKeys = extractJoinKeys(join, minDimensionJoin.get(), rawOnLeft);
        if (!joinKeys.isPresent()) {
            return Optional.empty();
        }

        Optional<RawSide> raw = extractRawSide(rawSide, context);
        if (!raw.isPresent()) {
            return Optional.empty();
        }

        LinkedHashSet<VariableReferenceExpression> rawVariablesToMap = new LinkedHashSet<>(rawSide.getOutputVariables());
        rawVariablesToMap.add(joinKeys.get().getRawKey());
        rawVariablesToMap.add(joinKeys.get().getRawValue());
        raw.get().getPredicate()
                .ifPresent(predicate -> rawVariablesToMap.addAll(VariablesExtractor.extractUnique(predicate)));

        Optional<ScanMapping> sourceScan = findMatchingScan(
                minDimensionJoin.get().getMinAggregation().getSource(),
                raw.get().getScan(),
                rawVariablesToMap,
                context);
        if (!sourceScan.isPresent()) {
            return Optional.empty();
        }

        VariableReferenceExpression sourceKey = sourceScan.get().getMapped(joinKeys.get().getRawKey()).orElse(null);
        VariableReferenceExpression sourceValue = sourceScan.get().getMapped(joinKeys.get().getRawValue()).orElse(null);
        if (sourceKey == null || sourceValue == null
                || !sourceKey.equals(minDimensionJoin.get().getMinAggregation().getGroupKey())
                || !sourceValue.equals(minDimensionJoin.get().getMinAggregation().getInput())) {
            return Optional.empty();
        }

        LinkedHashSet<VariableReferenceExpression> requiredSourceOutputs = new LinkedHashSet<>();
        for (VariableReferenceExpression rawOutput : rawSide.getOutputVariables()) {
            Optional<VariableReferenceExpression> mapped = sourceScan.get().getMapped(rawOutput);
            if (!mapped.isPresent()) {
                return Optional.empty();
            }
            requiredSourceOutputs.add(mapped.get());
        }
        raw.get().getPredicate().ifPresent(predicate -> {
            for (VariableReferenceExpression variable : VariablesExtractor.extractUnique(predicate)) {
                sourceScan.get().getMapped(variable).ifPresent(requiredSourceOutputs::add);
            }
        });
        requiredSourceOutputs.add(sourceKey);
        requiredSourceOutputs.add(sourceValue);

        Optional<PlanNode> rewrittenMinSource = ensureOutputVariables(
                minDimensionJoin.get().getMinAggregation().getSource(),
                requiredSourceOutputs);
        if (!rewrittenMinSource.isPresent()) {
            return Optional.empty();
        }

        PlanNode windowSource = new JoinNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.INNER,
                rewrittenMinSource.get(),
                minDimensionJoin.get().getDimensionSide(),
                ImmutableList.of(new EquiJoinClause(sourceKey, minDimensionJoin.get().getDimensionKey())),
                appendOutputs(
                        rewrittenMinSource.get().getOutputVariables(),
                        minDimensionJoin.get().getDimensionSide().getOutputVariables()),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                emptyMap());

        VariableReferenceExpression windowMin = context.getVariableAllocator().newVariable("min", sourceValue.getType());
        WindowNode window = new WindowNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                windowSource,
                new DataOrganizationSpecification(ImmutableList.of(sourceKey), Optional.empty()),
                ImmutableMap.of(windowMin, new WindowNode.Function(
                        new CallExpression(
                                sourceValue.getSourceLocation(),
                                "min",
                                functionAndTypeManager.lookupFunction("min", fromTypes(sourceValue.getType())),
                                sourceValue.getType(),
                                ImmutableList.of(sourceValue)),
                        FULL_ROWS_FRAME,
                        false)),
                Optional.empty(),
                ImmutableSet.of(),
                0);

        RowExpression minPredicate = new CallExpression(
                sourceValue.getSourceLocation(),
                EQUAL.name(),
                functionResolution.comparisonFunction(EQUAL, sourceValue.getType(), windowMin.getType()),
                BooleanType.BOOLEAN,
                ImmutableList.of(sourceValue, windowMin));
        Optional<RowExpression> rawPredicate = translatePredicate(raw.get().getPredicate(), sourceScan.get());
        if (raw.get().getPredicate().isPresent() && !rawPredicate.isPresent()) {
            return Optional.empty();
        }

        FilterNode filtered = new FilterNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                window,
                rawPredicate.map(predicate -> and(predicate, minPredicate)).orElse(minPredicate));

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression output : join.getOutputVariables()) {
            if (rawSide.getOutputVariables().contains(output)) {
                Optional<VariableReferenceExpression> mapped = sourceScan.get().getMapped(output);
                if (!mapped.isPresent()) {
                    return Optional.empty();
                }
                assignments.put(output, mapped.get());
            }
            else if (minDimensionJoin.get().getDimensionSide().getOutputVariables().contains(output)) {
                assignments.put(output, output);
            }
            else if (output.equals(minDimensionJoin.get().getMinAggregation().getOutput())) {
                assignments.put(output, windowMin);
            }
            else {
                return Optional.empty();
            }
        }

        return Optional.of(new ProjectNode(
                join.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                filtered,
                assignments.build(),
                ProjectNode.Locality.LOCAL));
    }

    private Optional<MinDimensionJoin> extractMinDimensionJoin(PlanNode node, Context context)
    {
        node = context.getLookup().resolve(node);
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode join = (JoinNode) node;
        if (join.getType() != JoinType.INNER || join.getFilter().isPresent() || join.getCriteria().size() != 1) {
            return Optional.empty();
        }

        PlanNode left = context.getLookup().resolve(join.getLeft());
        PlanNode right = context.getLookup().resolve(join.getRight());
        EquiJoinClause clause = join.getCriteria().get(0);

        Optional<MinAggregation> leftMin = extractMinAggregation(left, context);
        if (leftMin.isPresent()
                && leftMin.get().getGroupKey().equals(clause.getLeft())
                && right.getOutputVariables().contains(clause.getRight())) {
            return Optional.of(new MinDimensionJoin(leftMin.get(), right, clause.getRight()));
        }

        Optional<MinAggregation> rightMin = extractMinAggregation(right, context);
        if (rightMin.isPresent()
                && rightMin.get().getGroupKey().equals(clause.getRight())
                && left.getOutputVariables().contains(clause.getLeft())) {
            return Optional.of(new MinDimensionJoin(rightMin.get(), left, clause.getLeft()));
        }

        return Optional.empty();
    }

    private Optional<MinAggregation> extractMinAggregation(PlanNode root, Context context)
    {
        root = context.getLookup().resolve(root);
        VariableReferenceExpression minOutput = null;

        if (root instanceof FilterNode) {
            FilterNode filter = (FilterNode) root;
            Optional<VariableReferenceExpression> notNullVariable = extractNotNullVariable(filter.getPredicate());
            if (!notNullVariable.isPresent()) {
                return Optional.empty();
            }
            minOutput = notNullVariable.get();
            root = context.getLookup().resolve(filter.getSource());
        }

        if (!(root instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregation = (AggregationNode) root;
        if (aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getGroupingSetCount() != 1
                || aggregation.getGroupingKeys().size() != 1
                || aggregation.getAggregations().size() != 1
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()) {
            return Optional.empty();
        }

        Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry = aggregation.getAggregations().entrySet().iterator().next();
        if (minOutput != null && !minOutput.equals(entry.getKey())) {
            return Optional.empty();
        }

        AggregationNode.Aggregation min = entry.getValue();
        if (min.getArguments().size() != 1
                || min.isDistinct()
                || min.getFilter().isPresent()
                || min.getMask().isPresent()
                || min.getOrderBy().isPresent()
                || !(min.getArguments().get(0) instanceof VariableReferenceExpression)
                || !functionResolution.isMinFunction(min.getFunctionHandle())) {
            return Optional.empty();
        }

        return Optional.of(new MinAggregation(
                context.getLookup().resolve(aggregation.getSource()),
                aggregation.getGroupingKeys().get(0),
                entry.getKey(),
                (VariableReferenceExpression) min.getArguments().get(0)));
    }

    private Optional<JoinKeys> extractJoinKeys(JoinNode join, MinDimensionJoin minDimensionJoin, boolean rawOnLeft)
    {
        VariableReferenceExpression rawKey = null;
        VariableReferenceExpression rawValue = null;

        for (EquiJoinClause clause : join.getCriteria()) {
            VariableReferenceExpression raw = rawOnLeft ? clause.getLeft() : clause.getRight();
            VariableReferenceExpression other = rawOnLeft ? clause.getRight() : clause.getLeft();

            if (other.equals(minDimensionJoin.getDimensionKey())) {
                rawKey = raw;
            }
            else if (other.equals(minDimensionJoin.getMinAggregation().getOutput())) {
                rawValue = raw;
            }
            else {
                return Optional.empty();
            }
        }

        if (rawKey == null || rawValue == null) {
            return Optional.empty();
        }
        return Optional.of(new JoinKeys(rawKey, rawValue));
    }

    private Optional<PlanNode> ensureOutputVariables(
            PlanNode node,
            Set<VariableReferenceExpression> requiredOutputs)
    {
        if (node.getOutputVariables().containsAll(requiredOutputs)) {
            return Optional.of(node);
        }

        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode join = (JoinNode) node;
        LinkedHashSet<VariableReferenceExpression> inputs = new LinkedHashSet<>();
        inputs.addAll(join.getLeft().getOutputVariables());
        inputs.addAll(join.getRight().getOutputVariables());
        if (!inputs.containsAll(requiredOutputs)) {
            return Optional.empty();
        }

        LinkedHashSet<VariableReferenceExpression> outputs = new LinkedHashSet<>();
        for (VariableReferenceExpression output : join.getOutputVariables()) {
            if (join.getLeft().getOutputVariables().contains(output)) {
                outputs.add(output);
            }
        }
        for (VariableReferenceExpression output : requiredOutputs) {
            if (join.getLeft().getOutputVariables().contains(output)) {
                outputs.add(output);
            }
        }
        for (VariableReferenceExpression output : join.getOutputVariables()) {
            if (join.getRight().getOutputVariables().contains(output)) {
                outputs.add(output);
            }
        }
        for (VariableReferenceExpression output : requiredOutputs) {
            if (join.getRight().getOutputVariables().contains(output)) {
                outputs.add(output);
            }
        }

        return Optional.of(new JoinNode(
                join.getSourceLocation(),
                join.getId(),
                join.getStatsEquivalentPlanNode(),
                join.getType(),
                join.getLeft(),
                join.getRight(),
                join.getCriteria(),
                ImmutableList.copyOf(outputs),
                join.getFilter(),
                join.getLeftHashVariable(),
                join.getRightHashVariable(),
                join.getDistributionType(),
                join.getDynamicFilters(),
                join.isLeftKeysUnique(),
                join.isRightKeysUnique(),
                join.isLeftKeysNonNull(),
                join.isRightKeysNonNull(),
                join.isLeftKeysCoveredByRightKeys(),
                join.isRightKeysCoveredByLeftKeys()));
    }

    private Optional<RawSide> extractRawSide(PlanNode node, Context context)
    {
        node = context.getLookup().resolve(node);
        Optional<RowExpression> predicate = Optional.empty();
        while (node instanceof FilterNode) {
            FilterNode filter = (FilterNode) node;
            predicate = Optional.of(predicate.map(existing -> and(existing, filter.getPredicate())).orElse(filter.getPredicate()));
            node = context.getLookup().resolve(filter.getSource());
        }

        if (node instanceof TableScanNode) {
            return Optional.of(new RawSide((TableScanNode) node, predicate));
        }
        return Optional.empty();
    }

    private Optional<RowExpression> translatePredicate(Optional<RowExpression> predicate, ScanMapping mapping)
    {
        if (!predicate.isPresent()) {
            return Optional.empty();
        }

        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> translations = ImmutableMap.builder();
        for (VariableReferenceExpression variable : VariablesExtractor.extractUnique(predicate.get())) {
            Optional<VariableReferenceExpression> mapped = mapping.getMapped(variable);
            if (!mapped.isPresent()) {
                return Optional.empty();
            }
            translations.put(variable, mapped.get());
        }
        return Optional.of(RowExpressionVariableInliner.inlineVariables(translations.build(), predicate.get()));
    }

    private Optional<ScanMapping> findMatchingScan(
            PlanNode node,
            TableScanNode rawScan,
            Iterable<VariableReferenceExpression> rawVariables,
            Context context)
    {
        node = context.getLookup().resolve(node);
        if (node instanceof TableScanNode) {
            return matchScan((TableScanNode) node, rawScan, rawVariables);
        }

        for (PlanNode source : node.getSources()) {
            Optional<ScanMapping> result = findMatchingScan(source, rawScan, rawVariables, context);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    private Optional<ScanMapping> matchScan(
            TableScanNode candidate,
            TableScanNode rawScan,
            Iterable<VariableReferenceExpression> rawVariables)
    {
        if (!sameTable(candidate.getTable(), rawScan.getTable())) {
            return Optional.empty();
        }

        Map<ColumnHandle, VariableReferenceExpression> candidateByColumn = new LinkedHashMap<>();
        candidate.getAssignments().forEach((variable, column) -> candidateByColumn.put(column, variable));

        ScanMapping mapping = new ScanMapping();
        for (VariableReferenceExpression rawVariable : rawVariables) {
            ColumnHandle rawColumn = rawScan.getAssignments().get(rawVariable);
            if (rawColumn == null) {
                return Optional.empty();
            }
            VariableReferenceExpression candidateVariable = candidateByColumn.get(rawColumn);
            if (candidateVariable == null) {
                return Optional.empty();
            }
            mapping.put(rawVariable, candidateVariable);
        }
        return Optional.of(mapping);
    }

    private Optional<VariableReferenceExpression> extractNotNullVariable(RowExpression predicate)
    {
        if (!(predicate instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression call = (CallExpression) predicate;
        if (!functionResolution.isNotFunction(call.getFunctionHandle())
                || call.getArguments().size() != 1
                || !(call.getArguments().get(0) instanceof SpecialFormExpression)) {
            return Optional.empty();
        }

        SpecialFormExpression isNull = (SpecialFormExpression) call.getArguments().get(0);
        if (isNull.getForm() != IS_NULL
                || isNull.getArguments().size() != 1
                || !(isNull.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }
        return Optional.of((VariableReferenceExpression) isNull.getArguments().get(0));
    }

    private boolean sameTable(TableHandle left, TableHandle right)
    {
        return left.equals(right);
    }

    private ImmutableList<VariableReferenceExpression> appendOutputs(
            Iterable<VariableReferenceExpression> left,
            Iterable<VariableReferenceExpression> right)
    {
        LinkedHashSet<VariableReferenceExpression> outputs = new LinkedHashSet<>();
        left.forEach(outputs::add);
        right.forEach(outputs::add);
        return ImmutableList.copyOf(outputs);
    }

    private static class MinDimensionJoin
    {
        private final MinAggregation minAggregation;
        private final PlanNode dimensionSide;
        private final VariableReferenceExpression dimensionKey;

        private MinDimensionJoin(
                MinAggregation minAggregation,
                PlanNode dimensionSide,
                VariableReferenceExpression dimensionKey)
        {
            this.minAggregation = minAggregation;
            this.dimensionSide = dimensionSide;
            this.dimensionKey = dimensionKey;
        }

        public MinAggregation getMinAggregation()
        {
            return minAggregation;
        }

        public PlanNode getDimensionSide()
        {
            return dimensionSide;
        }

        public VariableReferenceExpression getDimensionKey()
        {
            return dimensionKey;
        }
    }

    private static class RawSide
    {
        private final TableScanNode scan;
        private final Optional<RowExpression> predicate;

        private RawSide(TableScanNode scan, Optional<RowExpression> predicate)
        {
            this.scan = scan;
            this.predicate = predicate;
        }

        public TableScanNode getScan()
        {
            return scan;
        }

        public Optional<RowExpression> getPredicate()
        {
            return predicate;
        }
    }

    private static class MinAggregation
    {
        private final PlanNode source;
        private final VariableReferenceExpression groupKey;
        private final VariableReferenceExpression output;
        private final VariableReferenceExpression input;

        private MinAggregation(
                PlanNode source,
                VariableReferenceExpression groupKey,
                VariableReferenceExpression output,
                VariableReferenceExpression input)
        {
            this.source = source;
            this.groupKey = groupKey;
            this.output = output;
            this.input = input;
        }

        public PlanNode getSource()
        {
            return source;
        }

        public VariableReferenceExpression getGroupKey()
        {
            return groupKey;
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

    private static class JoinKeys
    {
        private final VariableReferenceExpression rawKey;
        private final VariableReferenceExpression rawValue;

        private JoinKeys(VariableReferenceExpression rawKey, VariableReferenceExpression rawValue)
        {
            this.rawKey = rawKey;
            this.rawValue = rawValue;
        }

        public VariableReferenceExpression getRawKey()
        {
            return rawKey;
        }

        public VariableReferenceExpression getRawValue()
        {
            return rawValue;
        }
    }

    private static class ScanMapping
    {
        private final Map<VariableReferenceExpression, VariableReferenceExpression> mapping = new LinkedHashMap<>();

        public void put(VariableReferenceExpression from, VariableReferenceExpression to)
        {
            mapping.put(from, to);
        }

        public Optional<VariableReferenceExpression> getMapped(VariableReferenceExpression variable)
        {
            return Optional.ofNullable(mapping.get(variable));
        }
    }
}
