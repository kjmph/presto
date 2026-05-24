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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Reuses a filtered grouped fact aggregation when the outer query computes the
 * same sum by scanning and joining the fact table again.
 *
 * The rule targets shapes like:
 *
 * <pre>
 * - Aggregation(GROUP BY dimension columns; sum(F.value))
 *   - Join(D.key = A.key)
 *     - Join(F.key = D.key)
 *       - F
 *       - D  -- unique on D.key
 *     - Filter(predicate(sum_value))
 *       - Aggregation(GROUP BY A.key; sum(A.value))
 *         - A
 * </pre>
 *
 * and rewrites them to:
 *
 * <pre>
 * - Project(dimension columns, sum_value)
 *   - Join(A.key = D.key)
 *     - Filter(predicate(sum_value))
 *       - Aggregation(GROUP BY A.key; sum(A.value))
 *         - A
 *     - D
 * </pre>
 *
 * This is safe only when the dimension side is unique on the join key: then the
 * dimension columns are functionally determined by the fact key and the outer
 * sum over raw fact rows is equal to the already-computed per-key fact sum.
 */
public class TransformDuplicateFactSumToFilteredAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = typeOf(AggregationNode.class);

    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public TransformDuplicateFactSumToFilteredAggregation(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
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
        Optional<SumAggregation> outerSum = extractSingleSumAggregation(aggregation);
        if (!outerSum.isPresent()) {
            return Result.empty();
        }

        PlanNode source = context.getLookup().resolve(aggregation.getSource());
        if (!(source instanceof JoinNode)) {
            return Result.empty();
        }

        JoinNode filterJoin = (JoinNode) source;
        if (filterJoin.getType() != JoinType.INNER || filterJoin.getFilter().isPresent() || filterJoin.getCriteria().size() != 1) {
            return Result.empty();
        }

        Optional<RewriteParts> parts = tryExtract(
                filterJoin,
                context.getLookup().resolve(filterJoin.getLeft()),
                context.getLookup().resolve(filterJoin.getRight()),
                filterJoin.getCriteria().get(0).getLeft(),
                filterJoin.getCriteria().get(0).getRight(),
                aggregation,
                outerSum.get(),
                context);
        if (!parts.isPresent()) {
            parts = tryExtract(
                    filterJoin,
                    context.getLookup().resolve(filterJoin.getRight()),
                    context.getLookup().resolve(filterJoin.getLeft()),
                    filterJoin.getCriteria().get(0).getRight(),
                    filterJoin.getCriteria().get(0).getLeft(),
                    aggregation,
                    outerSum.get(),
                    context);
        }
        if (!parts.isPresent()) {
            return Result.empty();
        }

        JoinNode rewrittenJoin = new JoinNode(
                filterJoin.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                JoinType.INNER,
                parts.get().getFilteredAggregation().getRootWithSum(),
                parts.get().getFactJoin().getDimensionSide(),
                ImmutableList.of(new EquiJoinClause(
                        parts.get().getFilteredAggregation().getRootKey(),
                        parts.get().getFactJoin().getDimensionKey())),
                ImmutableList.<VariableReferenceExpression>builder()
                        .add(parts.get().getFilteredAggregation().getSum())
                        .addAll(parts.get().getFactJoin().getDimensionSide().getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                emptyMap());

        Assignments.Builder assignments = Assignments.builder();
        aggregation.getGroupingKeys().forEach(variable -> assignments.put(variable, variable));
        assignments.put(outerSum.get().getOutput(), parts.get().getFilteredAggregation().getSum());

        return Result.ofPlanNode(new ProjectNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                rewrittenJoin,
                assignments.build(),
                LOCAL));
    }

    private Optional<RewriteParts> tryExtract(
            JoinNode filterJoin,
            PlanNode factJoinSide,
            PlanNode filteredAggregationSide,
            VariableReferenceExpression factJoinSideKey,
            VariableReferenceExpression filteredAggregationJoinKey,
            AggregationNode aggregation,
            SumAggregation outerSum,
            Context context)
    {
        if (!factJoinSide.getOutputVariables().contains(factJoinSideKey)
                || !filteredAggregationSide.getOutputVariables().contains(filteredAggregationJoinKey)) {
            return Optional.empty();
        }

        Optional<FactDimensionJoin> factJoin = extractFactDimensionJoin(
                factJoinSide,
                factJoinSideKey,
                outerSum.getInput(),
                aggregation.getGroupingKeys(),
                context);
        Optional<FilteredAggregation> filteredAggregation = extractFilteredAggregation(
                filteredAggregationSide,
                filteredAggregationJoinKey,
                context);
        if (!factJoin.isPresent() || !filteredAggregation.isPresent()) {
            return Optional.empty();
        }

        if (!sameFactSources(factJoin.get().getFact(), filteredAggregation.get().getFact())) {
            return Optional.empty();
        }

        if (!isKnownUnique(factJoin.get().getDimensionSide(), ImmutableList.of(factJoin.get().getDimensionKey()), context)) {
            return Optional.empty();
        }

        return Optional.of(new RewriteParts(factJoin.get(), filteredAggregation.get()));
    }

    private Optional<FactDimensionJoin> extractFactDimensionJoin(
            PlanNode node,
            VariableReferenceExpression dimensionJoinKey,
            VariableReferenceExpression factValue,
            List<VariableReferenceExpression> groupingKeys,
            Context context)
    {
        node = context.getLookup().resolve(node);
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode join = (JoinNode) node;
        if (join.getType() != JoinType.INNER || join.getFilter().isPresent() || join.getCriteria().size() != 1) {
            return Optional.empty();
        }

        Optional<FactDimensionJoin> leftFact = tryExtractFactDimensionJoin(
                join,
                context.getLookup().resolve(join.getLeft()),
                context.getLookup().resolve(join.getRight()),
                join.getCriteria().get(0).getLeft(),
                join.getCriteria().get(0).getRight(),
                dimensionJoinKey,
                factValue,
                groupingKeys,
                context);
        if (leftFact.isPresent()) {
            return leftFact;
        }

        return tryExtractFactDimensionJoin(
                join,
                context.getLookup().resolve(join.getRight()),
                context.getLookup().resolve(join.getLeft()),
                join.getCriteria().get(0).getRight(),
                join.getCriteria().get(0).getLeft(),
                dimensionJoinKey,
                factValue,
                groupingKeys,
                context);
    }

    private Optional<FactDimensionJoin> tryExtractFactDimensionJoin(
            JoinNode join,
            PlanNode factSide,
            PlanNode dimensionSide,
            VariableReferenceExpression factKey,
            VariableReferenceExpression dimensionKey,
            VariableReferenceExpression expectedDimensionKey,
            VariableReferenceExpression factValue,
            List<VariableReferenceExpression> groupingKeys,
            Context context)
    {
        if (!dimensionKey.equals(expectedDimensionKey)
                || !factSide.getOutputVariables().contains(factKey)
                || !factSide.getOutputVariables().contains(factValue)
                || !dimensionSide.getOutputVariables().contains(dimensionKey)
                || !dimensionSide.getOutputVariables().containsAll(groupingKeys)) {
            return Optional.empty();
        }

        Optional<FactSource> fact = extractFactSource(factSide, factKey, factValue, context);
        if (!fact.isPresent()) {
            return Optional.empty();
        }

        if (!isKnownUnique(dimensionSide, ImmutableList.of(dimensionKey), context)) {
            return Optional.empty();
        }

        return Optional.of(new FactDimensionJoin(join, fact.get(), dimensionSide, dimensionKey));
    }

    private Optional<FilteredAggregation> extractFilteredAggregation(
            PlanNode root,
            VariableReferenceExpression rootKey,
            Context context)
    {
        PlanNode node = context.getLookup().resolve(root);
        VariableReferenceExpression key = rootKey;
        Optional<ProjectNode> project = Optional.empty();

        if (node instanceof ProjectNode) {
            project = Optional.of((ProjectNode) node);
            RowExpression assignment = project.get().getAssignments().get(key);
            if (!(assignment instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            key = (VariableReferenceExpression) assignment;
            node = context.getLookup().resolve(project.get().getSource());
        }

        if (node instanceof FilterNode) {
            node = context.getLookup().resolve(((FilterNode) node).getSource());
        }

        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregation = (AggregationNode) node;
        Optional<SumAggregation> sum = extractSingleSumAggregation(aggregation);
        if (!sum.isPresent()
                || aggregation.getGroupingKeys().size() != 1
                || !aggregation.getGroupingKeys().get(0).equals(key)) {
            return Optional.empty();
        }

        Optional<FactSource> fact = extractFactSource(aggregation.getSource(), key, sum.get().getInput(), context);
        if (!fact.isPresent()) {
            return Optional.empty();
        }

        PlanNode rootWithSum = root;
        if (!rootWithSum.getOutputVariables().contains(sum.get().getOutput())) {
            if (!project.isPresent()
                    || !context.getLookup().resolve(project.get().getSource()).getOutputVariables().contains(sum.get().getOutput())) {
                return Optional.empty();
            }
            rootWithSum = new ProjectNode(
                    project.get().getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    project.get().getSource(),
                    Assignments.builder()
                            .putAll(project.get().getAssignments())
                            .put(sum.get().getOutput(), sum.get().getOutput())
                            .build(),
                    project.get().getLocality());
        }

        return Optional.of(new FilteredAggregation(rootWithSum, rootKey, fact.get(), sum.get().getOutput()));
    }

    private Optional<FactSource> extractFactSource(
            PlanNode root,
            VariableReferenceExpression key,
            VariableReferenceExpression value,
            Context context)
    {
        root = context.getLookup().resolve(root);
        Optional<RowExpression> predicate = Optional.empty();
        PlanNode source = root;

        if (source instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) source;
            if (!(project.getAssignments().get(key) instanceof VariableReferenceExpression)
                    || !(project.getAssignments().get(value) instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            key = (VariableReferenceExpression) project.getAssignments().get(key);
            value = (VariableReferenceExpression) project.getAssignments().get(value);
            source = context.getLookup().resolve(project.getSource());
        }

        if (source instanceof FilterNode) {
            predicate = Optional.of(((FilterNode) source).getPredicate());
            source = context.getLookup().resolve(((FilterNode) source).getSource());
        }

        if (!(source instanceof TableScanNode)) {
            return Optional.empty();
        }

        TableScanNode scan = (TableScanNode) source;
        if (!scan.getAssignments().containsKey(key) || !scan.getAssignments().containsKey(value)) {
            return Optional.empty();
        }
        return Optional.of(new FactSource(scan, key, value, predicate));
    }

    private Optional<SumAggregation> extractSingleSumAggregation(AggregationNode aggregation)
    {
        if (aggregation.getAggregations().size() != 1
                || aggregation.getGroupingSetCount() != 1
                || aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()) {
            return Optional.empty();
        }

        Map.Entry<VariableReferenceExpression, Aggregation> entry = aggregation.getAggregations().entrySet().iterator().next();
        Aggregation sum = entry.getValue();
        if (sum.getArguments().size() != 1
                || sum.isDistinct()
                || sum.getFilter().isPresent()
                || sum.getMask().isPresent()
                || sum.getOrderBy().isPresent()
                || !(sum.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }

        VariableReferenceExpression input = (VariableReferenceExpression) sum.getArguments().get(0);
        if (!functionAndTypeManager.lookupFunction("sum", fromTypes(input.getType())).equals(sum.getFunctionHandle())) {
            return Optional.empty();
        }

        return Optional.of(new SumAggregation(entry.getKey(), input));
    }

    private boolean sameFactSources(FactSource left, FactSource right)
    {
        return sameTable(left.getScan().getTable(), right.getScan().getTable())
                && sameColumn(left.getScan(), left.getKey(), right.getScan(), right.getKey())
                && sameColumn(left.getScan(), left.getValue(), right.getScan(), right.getValue())
                && samePredicate(left.getPredicate(), left.getScan(), right.getPredicate(), right.getScan());
    }

    private boolean sameTable(TableHandle left, TableHandle right)
    {
        return left.equals(right);
    }

    private boolean sameColumn(
            TableScanNode leftScan,
            VariableReferenceExpression left,
            TableScanNode rightScan,
            VariableReferenceExpression right)
    {
        ColumnHandle leftColumn = leftScan.getAssignments().get(left);
        ColumnHandle rightColumn = rightScan.getAssignments().get(right);
        return leftColumn != null && leftColumn.equals(rightColumn);
    }

    private boolean samePredicate(
            Optional<RowExpression> leftPredicate,
            TableScanNode leftScan,
            Optional<RowExpression> rightPredicate,
            TableScanNode rightScan)
    {
        Optional<ImmutableMultiset<CanonicalRowExpressionKey>> left = CanonicalRowExpressionKey.canonicalizePredicate(
                leftPredicate,
                leftScan,
                functionAndTypeManager,
                functionResolution);
        Optional<ImmutableMultiset<CanonicalRowExpressionKey>> right = CanonicalRowExpressionKey.canonicalizePredicate(
                rightPredicate,
                rightScan,
                functionAndTypeManager,
                functionResolution);
        return left.isPresent() && left.equals(right);
    }

    private boolean isKnownUnique(PlanNode node, List<VariableReferenceExpression> variables, Context context)
    {
        node = context.getLookup().resolve(node);
        if (variables.isEmpty() || !node.getOutputVariables().containsAll(variables)) {
            return false;
        }

        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            ImmutableList.Builder<VariableReferenceExpression> sourceVariables = ImmutableList.builder();
            for (VariableReferenceExpression variable : variables) {
                RowExpression assignment = project.getAssignments().get(variable);
                if (!(assignment instanceof VariableReferenceExpression)) {
                    return false;
                }
                sourceVariables.add((VariableReferenceExpression) assignment);
            }
            return isKnownUnique(project.getSource(), sourceVariables.build(), context);
        }

        if (node instanceof FilterNode) {
            return isKnownUnique(((FilterNode) node).getSource(), variables, context);
        }

        if (node instanceof TableScanNode) {
            TableScanNode scan = (TableScanNode) node;
            LinkedHashSet<ColumnHandle> columns = new LinkedHashSet<>();
            for (VariableReferenceExpression variable : variables) {
                ColumnHandle column = scan.getAssignments().get(variable);
                if (column == null) {
                    return false;
                }
                columns.add(column);
            }
            return scan.getTableConstraints().stream()
                    .anyMatch(constraint -> (constraint.isEnabled() || constraint.isRely()) &&
                            (constraint instanceof PrimaryKeyConstraint || constraint instanceof UniqueConstraint) &&
                            constraint.getColumns().size() == columns.size() &&
                            constraint.getColumns().containsAll(columns));
        }

        if (node instanceof JoinNode) {
            return isKnownUniqueThroughJoin((JoinNode) node, variables, context);
        }

        return false;
    }

    private boolean isKnownUniqueThroughJoin(JoinNode join, List<VariableReferenceExpression> variables, Context context)
    {
        if (join.getType() != JoinType.INNER || join.getFilter().isPresent() || join.getCriteria().isEmpty()) {
            return false;
        }

        PlanNode left = context.getLookup().resolve(join.getLeft());
        PlanNode right = context.getLookup().resolve(join.getRight());
        if (left.getOutputVariables().containsAll(variables)) {
            Optional<List<VariableReferenceExpression>> rightJoinKeys = joinKeysForOtherSide(join, true, left, right);
            return rightJoinKeys.isPresent()
                    && isKnownUnique(left, variables, context)
                    && isKnownUnique(right, rightJoinKeys.get(), context);
        }
        if (right.getOutputVariables().containsAll(variables)) {
            Optional<List<VariableReferenceExpression>> leftJoinKeys = joinKeysForOtherSide(join, false, left, right);
            return leftJoinKeys.isPresent()
                    && isKnownUnique(right, variables, context)
                    && isKnownUnique(left, leftJoinKeys.get(), context);
        }
        return false;
    }

    private Optional<List<VariableReferenceExpression>> joinKeysForOtherSide(
            JoinNode join,
            boolean variablesOnLeft,
            PlanNode left,
            PlanNode right)
    {
        List<VariableReferenceExpression> otherKeys = new ArrayList<>();
        for (EquiJoinClause clause : join.getCriteria()) {
            if (left.getOutputVariables().contains(clause.getLeft()) && right.getOutputVariables().contains(clause.getRight())) {
                otherKeys.add(variablesOnLeft ? clause.getRight() : clause.getLeft());
            }
            else if (left.getOutputVariables().contains(clause.getRight()) && right.getOutputVariables().contains(clause.getLeft())) {
                otherKeys.add(variablesOnLeft ? clause.getLeft() : clause.getRight());
            }
            else {
                return Optional.empty();
            }
        }
        return otherKeys.isEmpty() ? Optional.empty() : Optional.of(otherKeys);
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

    private static class RewriteParts
    {
        private final FactDimensionJoin factJoin;
        private final FilteredAggregation filteredAggregation;

        private RewriteParts(FactDimensionJoin factJoin, FilteredAggregation filteredAggregation)
        {
            this.factJoin = factJoin;
            this.filteredAggregation = filteredAggregation;
        }

        public FactDimensionJoin getFactJoin()
        {
            return factJoin;
        }

        public FilteredAggregation getFilteredAggregation()
        {
            return filteredAggregation;
        }
    }

    private static class FactDimensionJoin
    {
        private final JoinNode join;
        private final FactSource fact;
        private final PlanNode dimensionSide;
        private final VariableReferenceExpression dimensionKey;

        private FactDimensionJoin(
                JoinNode join,
                FactSource fact,
                PlanNode dimensionSide,
                VariableReferenceExpression dimensionKey)
        {
            this.join = join;
            this.fact = fact;
            this.dimensionSide = dimensionSide;
            this.dimensionKey = dimensionKey;
        }

        public JoinNode getJoin()
        {
            return join;
        }

        public FactSource getFact()
        {
            return fact;
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

    private static class FilteredAggregation
    {
        private final PlanNode rootWithSum;
        private final VariableReferenceExpression rootKey;
        private final FactSource fact;
        private final VariableReferenceExpression sum;

        private FilteredAggregation(
                PlanNode rootWithSum,
                VariableReferenceExpression rootKey,
                FactSource fact,
                VariableReferenceExpression sum)
        {
            this.rootWithSum = rootWithSum;
            this.rootKey = rootKey;
            this.fact = fact;
            this.sum = sum;
        }

        public PlanNode getRootWithSum()
        {
            return rootWithSum;
        }

        public VariableReferenceExpression getRootKey()
        {
            return rootKey;
        }

        public FactSource getFact()
        {
            return fact;
        }

        public VariableReferenceExpression getSum()
        {
            return sum;
        }
    }

    private static class FactSource
    {
        private final TableScanNode scan;
        private final VariableReferenceExpression key;
        private final VariableReferenceExpression value;
        private final Optional<RowExpression> predicate;

        private FactSource(
                TableScanNode scan,
                VariableReferenceExpression key,
                VariableReferenceExpression value,
                Optional<RowExpression> predicate)
        {
            this.scan = scan;
            this.key = key;
            this.value = value;
            this.predicate = predicate;
        }

        public TableScanNode getScan()
        {
            return scan;
        }

        public VariableReferenceExpression getKey()
        {
            return key;
        }

        public VariableReferenceExpression getValue()
        {
            return value;
        }

        public Optional<RowExpression> getPredicate()
        {
            return predicate;
        }
    }
}
