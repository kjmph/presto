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
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.constraints.NotNullConstraint;
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
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IF;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.OR;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Collapses a counted fact stream guarded by paired same-key EXISTS and
 * NOT EXISTS non-equality predicates into one grouped fact aggregation.
 *
 * For each key K and value V, the original shape counts rows from R satisfying
 * predicate P when:
 *
 * <pre>
 * EXISTS     (R2.K = R.K AND R2.V <> R.V)
 * NOT EXISTS (R3.K = R.K AND R3.V <> R.V AND P(R3))
 * </pre>
 *
 * If K and V are known non-null by the connector, this can be evaluated once
 * per K:
 *
 * <pre>
 * min(V) <> max(V)
 * count_if(P) > 0
 * min(IF(P, V, null)) = max(IF(P, V, null))
 * </pre>
 *
 * The counted row multiplicity is preserved by summing count_if(P) after the
 * grouped fact rows are joined to the remaining query.
 */
public class TransformCountOverPairedNotEqualExistsToGroupedAggregation
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = typeOf(AggregationNode.class);

    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public TransformCountOverPairedNotEqualExistsToGroupedAggregation(FunctionAndTypeManager functionAndTypeManager)
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
        // Experimental guard: this rewrite produces a very large final
        // aggregation for canonical TPC-H Q21 at SF10K, which currently OOMs
        // in the native GPU final groupby. Keep Q21 on the standard per-EXISTS
        // min/max aggregation shape for now.
        return false;
    }

    @Override
    public Result apply(AggregationNode aggregation, Captures captures, Context context)
    {
        if (!isCountAggregation(aggregation)) {
            return Result.empty();
        }

        PlanNode aggregationSource = context.getLookup().resolve(aggregation.getSource());
        Optional<CountedFilterSource> countedSource = extractCountedFilterSource(aggregationSource, context);
        if (!countedSource.isPresent()) {
            return Result.empty();
        }

        Optional<FactsJoin> notExistsJoin = extractOuterFactsJoin(countedSource.get().getJoin(), context);
        if (!notExistsJoin.isPresent()) {
            return Result.empty();
        }

        MinMaxFacts lateFacts = notExistsJoin.get().getFacts();
        if (normalizedConjuncts(lateFacts.getPredicate(), lateFacts.getScan()).isEmpty()) {
            return Result.empty();
        }

        PlanNode notExistsProbe = notExistsJoin.get().getProbe();
        Optional<FactsJoin> existsJoin = findInnerFactsJoin(notExistsProbe, context);
        if (!existsJoin.isPresent()) {
            return Result.empty();
        }

        MinMaxFacts allFacts = existsJoin.get().getFacts();
        if (!normalizedConjuncts(allFacts.getPredicate(), allFacts.getScan()).isEmpty()) {
            return Result.empty();
        }

        if (!existsJoin.get().getProbeKey().equals(notExistsJoin.get().getProbeKey())) {
            return Result.empty();
        }

        Optional<VariableReferenceExpression> existsValue = extractPositiveCoalescedMinMaxNotEqual(
                existsJoin.get().getFilter().get(),
                allFacts.getMin(),
                allFacts.getMax());
        Optional<VariableReferenceExpression> notExistsValue = extractNegatedCoalescedMinMaxNotEqual(
                countedSource.get().getPredicate(),
                lateFacts.getMin(),
                lateFacts.getMax());
        if (!existsValue.isPresent() || !existsValue.equals(notExistsValue)) {
            return Result.empty();
        }

        Optional<PlanNode> remainingQueryWithoutExists = replaceNode(
                notExistsProbe,
                existsJoin.get().getJoin(),
                existsJoin.get().getProbe(),
                context);
        if (!remainingQueryWithoutExists.isPresent()) {
            return Result.empty();
        }

        PlanNode remainingQuery = remainingQueryWithoutExists.get();
        if (referencesAny(
                remainingQuery,
                ImmutableList.of(allFacts.getKey(), allFacts.getValue(), allFacts.getMin(), allFacts.getMax()),
                context)) {
            return Result.empty();
        }

        Optional<FactSource> outerFact = findFactSource(
                remainingQuery,
                existsJoin.get().getProbeKey(),
                existsValue.get(),
                lateFacts,
                context);
        if (!outerFact.isPresent() || !sameFacts(allFacts, lateFacts, outerFact.get())) {
            return Result.empty();
        }

        Optional<FilteringJoin> filteringJoin = findUniqueFilteringJoin(remainingQuery, outerFact.get(), context);

        VariableReferenceExpression rowCount = context.getVariableAllocator().newVariable("fact_count", BIGINT);
        PlanNode groupedFacts = buildGroupedFacts(
                outerFact.get(),
                allFacts,
                lateFacts,
                rowCount,
                filteringJoin.map(FilteringJoin::getFilterSource),
                context);

        PlanNode replacementTarget = outerFact.get().getRoot();
        PlanNode replacement = groupedFacts;
        if (filteringJoin.isPresent()) {
            Optional<PlanNode> factSideWithGroupedFacts = replaceFactSourceAndCarry(
                    filteringJoin.get().getFactSide(),
                    outerFact.get().getRoot(),
                    groupedFacts,
                    rowCount,
                    context);
            if (!factSideWithGroupedFacts.isPresent() || !factSideWithGroupedFacts.get().getOutputVariables().contains(rowCount)) {
                return Result.empty();
            }
            replacementTarget = filteringJoin.get().getJoin();
            replacement = factSideWithGroupedFacts.get();
        }

        Optional<PlanNode> newRemainingQuery = replaceFactSourceAndCarry(
                remainingQuery,
                replacementTarget,
                replacement,
                rowCount,
                context);
        if (!newRemainingQuery.isPresent() || !newRemainingQuery.get().getOutputVariables().contains(rowCount)) {
            return Result.empty();
        }

        Assignments.Builder assignments = Assignments.builder();
        countedSource.get().getProjection().getAssignments().getMap().forEach(assignments::put);
        assignments.put(rowCount, rowCount);
        PlanNode projected = new ProjectNode(
                newRemainingQuery.get().getSourceLocation(),
                context.getIdAllocator().getNextId(),
                newRemainingQuery.get(),
                assignments.build(),
                LOCAL);

        VariableReferenceExpression output = aggregation.getAggregations().keySet().iterator().next();
        AggregationNode rewrittenAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                projected,
                ImmutableMap.of(output, new Aggregation(
                        new CallExpression(
                                output.getSourceLocation(),
                                "sum",
                                functionAndTypeManager.lookupFunction("sum", fromTypes(BIGINT)),
                                BIGINT,
                                ImmutableList.of(rowCount)),
                        Optional.empty(),
                        Optional.empty(),
                        false,
                        Optional.empty())),
                aggregation.getGroupingSets(),
                aggregation.getPreGroupedVariables(),
                aggregation.getStep(),
                aggregation.getHashVariable(),
                aggregation.getGroupIdVariable(),
                aggregation.getAggregationId());

        return Result.ofPlanNode(rewrittenAggregation);
    }

    private boolean isCountAggregation(AggregationNode aggregation)
    {
        if (aggregation.getAggregations().size() != 1
                || aggregation.getGroupingSetCount() != 1
                || aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()) {
            return false;
        }

        Aggregation onlyAggregation = aggregation.getAggregations().values().iterator().next();
        return functionResolution.isCountFunction(onlyAggregation.getFunctionHandle())
                && onlyAggregation.getArguments().isEmpty()
                && !onlyAggregation.isDistinct()
                && !onlyAggregation.getFilter().isPresent()
                && !onlyAggregation.getMask().isPresent()
                && !onlyAggregation.getOrderBy().isPresent();
    }

    private Optional<CountedFilterSource> extractCountedFilterSource(PlanNode source, Context context)
    {
        source = context.getLookup().resolve(source);
        if (!(source instanceof ProjectNode)) {
            return Optional.empty();
        }
        ProjectNode project = (ProjectNode) source;
        PlanNode projectSource = context.getLookup().resolve(project.getSource());
        if (!(projectSource instanceof FilterNode)) {
            return Optional.empty();
        }
        FilterNode filter = (FilterNode) projectSource;
        PlanNode filterSource = context.getLookup().resolve(filter.getSource());
        if (!(filterSource instanceof JoinNode)) {
            return Optional.empty();
        }
        return Optional.of(new CountedFilterSource(project, filter.getPredicate(), (JoinNode) filterSource));
    }

    private Optional<MinMaxFacts> extractMinMaxFacts(PlanNode node, Context context)
    {
        node = context.getLookup().resolve(node);
        if (!(node instanceof AggregationNode)) {
            return Optional.empty();
        }

        AggregationNode aggregation = (AggregationNode) node;
        if (aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getGroupingKeys().size() != 1
                || aggregation.getAggregations().size() != 2
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getAggregationId().isPresent()) {
            return Optional.empty();
        }

        Optional<VariableReferenceExpression> minOutput = Optional.empty();
        Optional<VariableReferenceExpression> maxOutput = Optional.empty();
        Optional<VariableReferenceExpression> value = Optional.empty();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregation.getAggregations().entrySet()) {
            Aggregation aggregate = entry.getValue();
            if (aggregate.getArguments().size() != 1
                    || aggregate.isDistinct()
                    || aggregate.getFilter().isPresent()
                    || aggregate.getMask().isPresent()
                    || aggregate.getOrderBy().isPresent()
                    || !(aggregate.getArguments().get(0) instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }

            VariableReferenceExpression argument = (VariableReferenceExpression) aggregate.getArguments().get(0);
            if (functionResolution.isMinFunction(aggregate.getFunctionHandle())) {
                minOutput = Optional.of(entry.getKey());
                value = Optional.of(argument);
            }
            else if (functionResolution.isMaxFunction(aggregate.getFunctionHandle())) {
                maxOutput = Optional.of(entry.getKey());
                if (value.isPresent() && !value.get().equals(argument)) {
                    return Optional.empty();
                }
                value = Optional.of(argument);
            }
            else {
                return Optional.empty();
            }
        }

        if (!minOutput.isPresent() || !maxOutput.isPresent() || !value.isPresent()) {
            return Optional.empty();
        }

        Optional<FilteredScan> source = extractFilteredScan(aggregation.getSource(), context);
        if (!source.isPresent()
                || !source.get().getScan().getAssignments().containsKey(aggregation.getGroupingKeys().get(0))
                || !source.get().getScan().getAssignments().containsKey(value.get())) {
            return Optional.empty();
        }

        return Optional.of(new MinMaxFacts(
                aggregation.getGroupingKeys().get(0),
                value.get(),
                minOutput.get(),
                maxOutput.get(),
                source.get()));
    }

    private Optional<FactsJoin> extractOuterFactsJoin(JoinNode join, Context context)
    {
        if ((join.getType() != JoinType.LEFT && join.getType() != JoinType.RIGHT)
                || join.getCriteria().size() != 1
                || join.getFilter().isPresent()) {
            return Optional.empty();
        }

        EquiJoinClause clause = join.getCriteria().get(0);
        PlanNode factsSide;
        PlanNode probeSide;
        VariableReferenceExpression factsKey;
        VariableReferenceExpression probeKey;
        if (join.getType() == JoinType.LEFT) {
            factsSide = join.getRight();
            probeSide = join.getLeft();
            factsKey = clause.getRight();
            probeKey = clause.getLeft();
        }
        else {
            factsSide = join.getLeft();
            probeSide = join.getRight();
            factsKey = clause.getLeft();
            probeKey = clause.getRight();
        }

        Optional<MinMaxFacts> facts = extractMinMaxFacts(factsSide, context);
        if (!facts.isPresent() || !factsKey.equals(facts.get().getKey())) {
            return Optional.empty();
        }

        return Optional.of(new FactsJoin(join, facts.get(), context.getLookup().resolve(probeSide), probeKey, Optional.empty()));
    }

    private Optional<FactsJoin> extractInnerFactsJoin(JoinNode join, Context context)
    {
        if (join.getType() != JoinType.INNER || join.getCriteria().size() != 1 || !join.getFilter().isPresent()) {
            return Optional.empty();
        }

        EquiJoinClause clause = join.getCriteria().get(0);
        Optional<MinMaxFacts> leftFacts = extractMinMaxFacts(join.getLeft(), context);
        Optional<MinMaxFacts> rightFacts = extractMinMaxFacts(join.getRight(), context);
        if (leftFacts.isPresent() == rightFacts.isPresent()) {
            return Optional.empty();
        }

        if (leftFacts.isPresent()) {
            if (!clause.getLeft().equals(leftFacts.get().getKey())) {
                return Optional.empty();
            }
            return Optional.of(new FactsJoin(join, leftFacts.get(), context.getLookup().resolve(join.getRight()), clause.getRight(), join.getFilter()));
        }

        if (!clause.getRight().equals(rightFacts.get().getKey())) {
            return Optional.empty();
        }
        return Optional.of(new FactsJoin(join, rightFacts.get(), context.getLookup().resolve(join.getLeft()), clause.getLeft(), join.getFilter()));
    }

    private Optional<FactsJoin> findInnerFactsJoin(PlanNode node, Context context)
    {
        node = context.getLookup().resolve(node);
        if (node instanceof JoinNode) {
            Optional<FactsJoin> factsJoin = extractInnerFactsJoin((JoinNode) node, context);
            if (factsJoin.isPresent()) {
                return factsJoin;
            }
        }

        for (PlanNode source : node.getSources()) {
            Optional<FactsJoin> factsJoin = findInnerFactsJoin(source, context);
            if (factsJoin.isPresent()) {
                return factsJoin;
            }
        }
        return Optional.empty();
    }

    private Optional<FilteredScan> extractFilteredScan(PlanNode node, Context context)
    {
        Optional<RowExpression> predicate = Optional.empty();
        PlanNode source = context.getLookup().resolve(node);
        if (source instanceof FilterNode) {
            predicate = Optional.of(((FilterNode) source).getPredicate());
            source = context.getLookup().resolve(((FilterNode) source).getSource());
        }
        if (!(source instanceof TableScanNode)) {
            return Optional.empty();
        }
        return Optional.of(new FilteredScan((TableScanNode) source, predicate));
    }

    private Optional<FactSource> findFactSource(
            PlanNode node,
            VariableReferenceExpression key,
            VariableReferenceExpression value,
            MinMaxFacts lateFacts,
            Context context)
    {
        node = context.getLookup().resolve(node);
        Optional<FactSource> candidate = extractFactSource(node, key, value, lateFacts, context);
        if (candidate.isPresent()) {
            return candidate;
        }

        for (PlanNode source : node.getSources()) {
            Optional<FactSource> found = findFactSource(source, key, value, lateFacts, context);
            if (found.isPresent()) {
                return found;
            }
        }
        return Optional.empty();
    }

    private Optional<FactSource> extractFactSource(
            PlanNode root,
            VariableReferenceExpression key,
            VariableReferenceExpression value,
            MinMaxFacts lateFacts,
            Context context)
    {
        root = context.getLookup().resolve(root);
        if (!(root instanceof ProjectNode || root instanceof FilterNode || root instanceof TableScanNode)
                || !root.getOutputVariables().contains(key)
                || !root.getOutputVariables().contains(value)) {
            return Optional.empty();
        }

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
        if (!sameTable(scan.getTable(), lateFacts.getScan().getTable())
                || !sameColumn(scan, key, lateFacts.getScan(), lateFacts.getKey())
                || !sameColumn(scan, value, lateFacts.getScan(), lateFacts.getValue())
                || !samePredicate(predicate, scan, lateFacts.getPredicate(), lateFacts.getScan())) {
            return Optional.empty();
        }

        return Optional.of(new FactSource(root, scan, key, value));
    }

    private boolean sameFacts(MinMaxFacts allFacts, MinMaxFacts lateFacts, FactSource outerFact)
    {
        return sameTable(allFacts.getScan().getTable(), lateFacts.getScan().getTable())
                && sameTable(allFacts.getScan().getTable(), outerFact.getScan().getTable())
                && sameColumn(allFacts.getScan(), allFacts.getKey(), lateFacts.getScan(), lateFacts.getKey())
                && sameColumn(allFacts.getScan(), allFacts.getKey(), outerFact.getScan(), outerFact.getKey())
                && sameColumn(allFacts.getScan(), allFacts.getValue(), lateFacts.getScan(), lateFacts.getValue())
                && sameColumn(allFacts.getScan(), allFacts.getValue(), outerFact.getScan(), outerFact.getValue())
                && isKnownNonNull(allFacts.getScan(), allFacts.getKey())
                && isKnownNonNull(allFacts.getScan(), allFacts.getValue());
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
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> mapping = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, ColumnHandle> rightEntry : rightScan.getAssignments().entrySet()) {
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> leftEntry : leftScan.getAssignments().entrySet()) {
                if (rightEntry.getValue().equals(leftEntry.getValue())) {
                    mapping.put(rightEntry.getKey(), leftEntry.getKey());
                    break;
                }
            }
        }
        Map<VariableReferenceExpression, VariableReferenceExpression> rightToLeft = mapping.build();
        List<RowExpression> leftConjuncts = normalizedConjuncts(leftPredicate, leftScan);
        List<RowExpression> rightConjuncts = new ArrayList<>();
        for (RowExpression conjunct : normalizedConjuncts(rightPredicate, rightScan)) {
            rightConjuncts.add(rewriteVariables(conjunct, rightToLeft));
        }

        return leftConjuncts.size() == rightConjuncts.size()
                && leftConjuncts.containsAll(rightConjuncts)
                && rightConjuncts.containsAll(leftConjuncts);
    }

    private List<RowExpression> normalizedConjuncts(Optional<RowExpression> predicate, TableScanNode scan)
    {
        if (!predicate.isPresent()) {
            return ImmutableList.of();
        }

        List<RowExpression> conjuncts = new ArrayList<>();
        collectConjuncts(predicate.get(), conjuncts);
        conjuncts.removeIf(conjunct -> isRedundantKnownNonNull(conjunct, scan));
        return conjuncts;
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

    private boolean isRedundantKnownNonNull(RowExpression expression, TableScanNode scan)
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
                && isNull.getArguments().get(0) instanceof VariableReferenceExpression
                && isKnownNonNull(scan, (VariableReferenceExpression) isNull.getArguments().get(0));
    }

    private boolean isKnownNonNull(TableScanNode scan, VariableReferenceExpression variable)
    {
        ColumnHandle column = scan.getAssignments().get(variable);
        return column != null && scan.getTableConstraints().stream()
                .anyMatch(constraint -> (constraint.isEnabled() || constraint.isRely()) &&
                        (constraint instanceof NotNullConstraint || constraint instanceof PrimaryKeyConstraint) &&
                        constraint.getColumns().contains(column));
    }

    private boolean isKnownUnique(TableScanNode scan, VariableReferenceExpression variable)
    {
        ColumnHandle column = scan.getAssignments().get(variable);
        return column != null && scan.getTableConstraints().stream()
                .anyMatch(constraint -> (constraint.isEnabled() || constraint.isRely()) &&
                        (constraint instanceof PrimaryKeyConstraint || constraint instanceof UniqueConstraint) &&
                        constraint.getColumns().size() == 1 &&
                        constraint.getColumns().contains(column));
    }

    private Optional<FilteringJoin> findUniqueFilteringJoin(PlanNode node, FactSource outerFact, Context context)
    {
        node = context.getLookup().resolve(node);
        if (node instanceof JoinNode) {
            Optional<FilteringJoin> filteringJoin = extractUniqueFilteringJoin((JoinNode) node, outerFact, context);
            if (filteringJoin.isPresent()) {
                return filteringJoin;
            }
        }

        for (PlanNode source : node.getSources()) {
            Optional<FilteringJoin> filteringJoin = findUniqueFilteringJoin(source, outerFact, context);
            if (filteringJoin.isPresent()) {
                return filteringJoin;
            }
        }
        return Optional.empty();
    }

    private Optional<FilteringJoin> extractUniqueFilteringJoin(JoinNode join, FactSource outerFact, Context context)
    {
        if (join.getType() != JoinType.INNER || join.getCriteria().size() != 1 || join.getFilter().isPresent()) {
            return Optional.empty();
        }

        PlanNode left = context.getLookup().resolve(join.getLeft());
        PlanNode right = context.getLookup().resolve(join.getRight());
        boolean leftContainsFact = containsNode(left, outerFact.getRoot(), context);
        boolean rightContainsFact = containsNode(right, outerFact.getRoot(), context);
        if (leftContainsFact == rightContainsFact) {
            return Optional.empty();
        }

        EquiJoinClause clause = join.getCriteria().get(0);
        PlanNode factSide;
        PlanNode filterSide;
        VariableReferenceExpression factKey;
        VariableReferenceExpression filterKey;
        if (leftContainsFact) {
            factSide = left;
            filterSide = right;
            factKey = clause.getLeft();
            filterKey = clause.getRight();
        }
        else {
            factSide = right;
            filterSide = left;
            factKey = clause.getRight();
            filterKey = clause.getLeft();
        }

        if (!factKey.equals(outerFact.getKey())) {
            return Optional.empty();
        }

        if (filterSide.getOutputVariables().stream().anyMatch(join.getOutputVariables()::contains)) {
            return Optional.empty();
        }

        Optional<UniqueFilteringSource> filterSource = extractUniqueFilteringSource(filterSide, filterKey, context);
        if (!filterSource.isPresent()) {
            return Optional.empty();
        }

        return Optional.of(new FilteringJoin(join, factSide, filterSource.get()));
    }

    private boolean containsNode(PlanNode node, PlanNode target, Context context)
    {
        node = context.getLookup().resolve(node);
        if (node == target || node.getId().equals(target.getId())) {
            return true;
        }

        for (PlanNode source : node.getSources()) {
            if (containsNode(source, target, context)) {
                return true;
            }
        }
        return false;
    }

    private Optional<UniqueFilteringSource> extractUniqueFilteringSource(
            PlanNode root,
            VariableReferenceExpression rootKey,
            Context context)
    {
        VariableReferenceExpression key = rootKey;
        PlanNode node = context.getLookup().resolve(root);
        while (true) {
            if (node instanceof ProjectNode) {
                ProjectNode project = (ProjectNode) node;
                if (!(project.getAssignments().get(key) instanceof VariableReferenceExpression)) {
                    return Optional.empty();
                }
                key = (VariableReferenceExpression) project.getAssignments().get(key);
                node = context.getLookup().resolve(project.getSource());
                continue;
            }

            if (node instanceof FilterNode) {
                node = context.getLookup().resolve(((FilterNode) node).getSource());
                continue;
            }

            if (!(node instanceof TableScanNode)) {
                return Optional.empty();
            }

            TableScanNode scan = (TableScanNode) node;
            if (!scan.getAssignments().containsKey(key) || !isKnownUnique(scan, key)) {
                return Optional.empty();
            }
            return Optional.of(new UniqueFilteringSource(root, rootKey));
        }
    }

    private PlanNode buildGroupedFacts(
            FactSource outerFact,
            MinMaxFacts allFacts,
            MinMaxFacts lateFacts,
            VariableReferenceExpression rowCount,
            Optional<UniqueFilteringSource> filteringSource,
            Context context)
    {
        VariableReferenceExpression key = lateFacts.getKey();
        VariableReferenceExpression value = lateFacts.getValue();
        RowExpression predicate = lateFacts.getPredicate().get();

        VariableReferenceExpression lateValue = context.getVariableAllocator().newVariable("late_value", value.getType());
        VariableReferenceExpression lateOne = context.getVariableAllocator().newVariable("late_one", BIGINT);
        PlanNode projectionSource = lateFacts.getScan();
        if (filteringSource.isPresent()) {
            projectionSource = new JoinNode(
                    lateFacts.getScan().getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    JoinType.INNER,
                    lateFacts.getScan(),
                    filteringSource.get().getRoot(),
                    ImmutableList.of(new EquiJoinClause(key, filteringSource.get().getRootKey())),
                    lateFacts.getScan().getOutputVariables(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    emptyMap());
        }
        ProjectNode projection = new ProjectNode(
                projectionSource.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                projectionSource,
                Assignments.builder()
                        .put(key, key)
                        .put(value, value)
                        .put(lateValue, new SpecialFormExpression(
                                predicate.getSourceLocation(),
                                IF,
                                value.getType(),
                                predicate,
                                value,
                                new ConstantExpression(null, value.getType())))
                        .put(lateOne, new SpecialFormExpression(
                                predicate.getSourceLocation(),
                                IF,
                                BIGINT,
                                predicate,
                                new ConstantExpression(1L, BIGINT),
                                new ConstantExpression(0L, BIGINT)))
                        .build(),
                LOCAL);

        VariableReferenceExpression minValue = context.getVariableAllocator().newVariable("fact_min", value.getType());
        VariableReferenceExpression maxValue = context.getVariableAllocator().newVariable("fact_max", value.getType());
        VariableReferenceExpression lateMinValue = context.getVariableAllocator().newVariable("late_min", value.getType());
        VariableReferenceExpression lateMaxValue = context.getVariableAllocator().newVariable("late_max", value.getType());

        AggregationNode grouped = new AggregationNode(
                lateFacts.getScan().getSourceLocation(),
                context.getIdAllocator().getNextId(),
                projection,
                ImmutableMap.of(
                        minValue, min(value),
                        maxValue, max(value),
                        lateMinValue, min(lateValue),
                        lateMaxValue, max(lateValue),
                        rowCount, sum(lateOne)),
                singleGroupingSet(ImmutableList.of(key)),
                ImmutableList.of(),
                AggregationNode.Step.SINGLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        FilterNode filtered = new FilterNode(
                grouped.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                grouped,
                and(
                        comparisonExpression(functionResolution, NOT_EQUAL, minValue, maxValue),
                        comparisonExpression(functionResolution, GREATER_THAN, rowCount, new ConstantExpression(0L, BIGINT)),
                        comparisonExpression(functionResolution, EQUAL, lateMinValue, lateMaxValue)));

        return new ProjectNode(
                filtered.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                filtered,
                Assignments.builder()
                        .put(outerFact.getKey(), key)
                        .put(outerFact.getValue(), lateMinValue)
                        .put(rowCount, rowCount)
                        .build(),
                LOCAL);
    }

    private Aggregation min(VariableReferenceExpression variable)
    {
        return new Aggregation(
                new CallExpression(
                        variable.getSourceLocation(),
                        "min",
                        functionResolution.minFunction(variable.getType()),
                        variable.getType(),
                        ImmutableList.of(variable)),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    private Aggregation max(VariableReferenceExpression variable)
    {
        return new Aggregation(
                new CallExpression(
                        variable.getSourceLocation(),
                        "max",
                        functionResolution.maxFunction(variable.getType()),
                        variable.getType(),
                        ImmutableList.of(variable)),
                Optional.empty(),
                Optional.empty(),
                false,
                Optional.empty());
    }

    private Aggregation sum(VariableReferenceExpression variable)
    {
        return new Aggregation(
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

    private RowExpression and(RowExpression left, RowExpression middle, RowExpression right)
    {
        return new SpecialFormExpression(
                SpecialFormExpression.Form.AND,
                BOOLEAN,
                left,
                new SpecialFormExpression(SpecialFormExpression.Form.AND, BOOLEAN, middle, right));
    }

    private Optional<PlanNode> replaceFactSourceAndCarry(
            PlanNode node,
            PlanNode target,
            PlanNode replacement,
            VariableReferenceExpression carried,
            Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);
        if (resolved == target || resolved.getId().equals(target.getId())) {
            return Optional.of(replacement);
        }

        List<PlanNode> rewrittenSources = new ArrayList<>();
        boolean changed = false;
        for (PlanNode source : resolved.getSources()) {
            Optional<PlanNode> rewritten = replaceFactSourceAndCarry(source, target, replacement, carried, context);
            if (rewritten.isPresent()) {
                rewrittenSources.add(rewritten.get());
                changed = true;
            }
            else {
                rewrittenSources.add(context.getLookup().resolve(source));
            }
        }
        if (!changed) {
            return Optional.empty();
        }

        PlanNode rewritten = ChildReplacer.replaceChildren(resolved, rewrittenSources);
        if (!rewritten.getSources().stream().anyMatch(source -> source.getOutputVariables().contains(carried))
                || rewritten.getOutputVariables().contains(carried)) {
            return Optional.of(rewritten);
        }

        if (rewritten instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) rewritten;
            return Optional.of(new ProjectNode(
                    project.getSourceLocation(),
                    project.getId(),
                    project.getStatsEquivalentPlanNode(),
                    project.getSource(),
                    Assignments.builder()
                            .putAll(project.getAssignments())
                            .put(carried, carried)
                            .build(),
                    project.getLocality()));
        }

        if (rewritten instanceof FilterNode) {
            return Optional.of(rewritten);
        }

        if (rewritten instanceof JoinNode) {
            JoinNode join = (JoinNode) rewritten;
            return Optional.of(new JoinNode(
                    join.getSourceLocation(),
                    join.getId(),
                    join.getStatsEquivalentPlanNode(),
                    join.getType(),
                    join.getLeft(),
                    join.getRight(),
                    join.getCriteria(),
                    withCarriedJoinOutput(join, carried),
                    join.getFilter(),
                    join.getLeftHashVariable(),
                    join.getRightHashVariable(),
                    join.getDistributionType(),
                    join.getDynamicFilters()));
        }

        return Optional.of(rewritten);
    }

    private Optional<PlanNode> replaceNode(
            PlanNode node,
            PlanNode target,
            PlanNode replacement,
            Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);
        if (resolved == target || resolved.getId().equals(target.getId())) {
            return Optional.of(replacement);
        }

        List<PlanNode> rewrittenSources = new ArrayList<>();
        boolean changed = false;
        for (PlanNode source : resolved.getSources()) {
            Optional<PlanNode> rewritten = replaceNode(source, target, replacement, context);
            if (rewritten.isPresent()) {
                rewrittenSources.add(rewritten.get());
                changed = true;
            }
            else {
                rewrittenSources.add(context.getLookup().resolve(source));
            }
        }
        if (!changed) {
            return Optional.empty();
        }
        return Optional.of(ChildReplacer.replaceChildren(resolved, rewrittenSources));
    }

    private boolean referencesAny(
            PlanNode node,
            List<VariableReferenceExpression> variables,
            Context context)
    {
        if (outputsAny(node, variables, context)) {
            return true;
        }

        Set<VariableReferenceExpression> references = VariablesExtractor.extractUnique(node, context.getLookup());
        return references.stream().anyMatch(variables::contains);
    }

    private boolean outputsAny(
            PlanNode node,
            List<VariableReferenceExpression> variables,
            Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);
        if (resolved.getOutputVariables().stream().anyMatch(variables::contains)) {
            return true;
        }
        for (PlanNode source : resolved.getSources()) {
            if (outputsAny(source, variables, context)) {
                return true;
            }
        }
        return false;
    }

    private List<VariableReferenceExpression> withCarriedJoinOutput(JoinNode join, VariableReferenceExpression carried)
    {
        ImmutableList.Builder<VariableReferenceExpression> outputs = ImmutableList.builder();
        for (VariableReferenceExpression variable : join.getOutputVariables()) {
            if (join.getLeft().getOutputVariables().contains(variable)) {
                outputs.add(variable);
            }
        }
        if (join.getLeft().getOutputVariables().contains(carried)) {
            outputs.add(carried);
        }
        for (VariableReferenceExpression variable : join.getOutputVariables()) {
            if (join.getRight().getOutputVariables().contains(variable)) {
                outputs.add(variable);
            }
        }
        if (join.getRight().getOutputVariables().contains(carried)) {
            outputs.add(carried);
        }
        return outputs.build();
    }

    private Optional<VariableReferenceExpression> extractPositiveCoalescedMinMaxNotEqual(
            RowExpression expression,
            VariableReferenceExpression min,
            VariableReferenceExpression max)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expression;
        if (!functionResolution.isEqualsFunction(call.getFunctionHandle()) || call.getArguments().size() != 2) {
            return Optional.empty();
        }
        if (isTrueConstant(call.getArguments().get(0))) {
            return extractCoalescedMinMaxNotEqual(call.getArguments().get(1), min, max);
        }
        if (isTrueConstant(call.getArguments().get(1))) {
            return extractCoalescedMinMaxNotEqual(call.getArguments().get(0), min, max);
        }
        return Optional.empty();
    }

    private Optional<VariableReferenceExpression> extractNegatedCoalescedMinMaxNotEqual(
            RowExpression expression,
            VariableReferenceExpression min,
            VariableReferenceExpression max)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expression;
        if (!functionResolution.isNotFunction(call.getFunctionHandle()) || call.getArguments().size() != 1) {
            return Optional.empty();
        }
        return extractCoalescedMinMaxNotEqual(call.getArguments().get(0), min, max);
    }

    private Optional<VariableReferenceExpression> extractCoalescedMinMaxNotEqual(
            RowExpression expression,
            VariableReferenceExpression min,
            VariableReferenceExpression max)
    {
        if (!(expression instanceof SpecialFormExpression)) {
            return Optional.empty();
        }
        SpecialFormExpression coalesce = (SpecialFormExpression) expression;
        if (coalesce.getForm() != SpecialFormExpression.Form.COALESCE
                || coalesce.getArguments().size() != 2
                || !isFalseConstant(coalesce.getArguments().get(1))
                || !(coalesce.getArguments().get(0) instanceof SpecialFormExpression)) {
            return Optional.empty();
        }

        SpecialFormExpression disjunction = (SpecialFormExpression) coalesce.getArguments().get(0);
        if (disjunction.getForm() != OR || disjunction.getArguments().size() != 2) {
            return Optional.empty();
        }

        Optional<VariableReferenceExpression> first = extractNotEqualOther(disjunction.getArguments().get(0), min);
        Optional<VariableReferenceExpression> second = extractNotEqualOther(disjunction.getArguments().get(1), max);
        if (first.isPresent() && first.equals(second)) {
            return first;
        }

        first = extractNotEqualOther(disjunction.getArguments().get(0), max);
        second = extractNotEqualOther(disjunction.getArguments().get(1), min);
        if (first.isPresent() && first.equals(second)) {
            return first;
        }
        return Optional.empty();
    }

    private Optional<VariableReferenceExpression> extractNotEqualOther(RowExpression expression, VariableReferenceExpression expected)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }
        CallExpression call = (CallExpression) expression;
        if (!isNotEqualFunction(call) || call.getArguments().size() != 2) {
            return Optional.empty();
        }

        Optional<VariableReferenceExpression> left = asVariable(call.getArguments().get(0));
        Optional<VariableReferenceExpression> right = asVariable(call.getArguments().get(1));
        if (left.isPresent() && left.get().equals(expected) && right.isPresent()) {
            return right;
        }
        if (right.isPresent() && right.get().equals(expected) && left.isPresent()) {
            return left;
        }
        return Optional.empty();
    }

    private boolean isNotEqualFunction(CallExpression call)
    {
        List<RowExpression> arguments = call.getArguments();
        return arguments.size() == 2
                && arguments.get(0).getType().isComparable()
                && arguments.get(1).getType().isComparable()
                && functionResolution.comparisonFunction(NOT_EQUAL, arguments.get(0).getType(), arguments.get(1).getType()).equals(call.getFunctionHandle());
    }

    private Optional<VariableReferenceExpression> asVariable(RowExpression expression)
    {
        if (expression instanceof VariableReferenceExpression) {
            return Optional.of((VariableReferenceExpression) expression);
        }
        return Optional.empty();
    }

    private boolean isTrueConstant(RowExpression expression)
    {
        return expression instanceof ConstantExpression
                && ((ConstantExpression) expression).getType().equals(BOOLEAN)
                && Boolean.TRUE.equals(((ConstantExpression) expression).getValue());
    }

    private boolean isFalseConstant(RowExpression expression)
    {
        return expression instanceof ConstantExpression
                && ((ConstantExpression) expression).getType().equals(BOOLEAN)
                && Boolean.FALSE.equals(((ConstantExpression) expression).getValue());
    }

    private RowExpression rewriteVariables(RowExpression expression, Map<VariableReferenceExpression, VariableReferenceExpression> mapping)
    {
        return RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Void>()
        {
            @Override
            public RowExpression rewriteVariableReference(VariableReferenceExpression variable, Void context, RowExpressionTreeRewriter<Void> treeRewriter)
            {
                return mapping.getOrDefault(variable, variable);
            }
        }, expression);
    }

    private static class CountedFilterSource
    {
        private final ProjectNode projection;
        private final RowExpression predicate;
        private final JoinNode join;

        private CountedFilterSource(ProjectNode projection, RowExpression predicate, JoinNode join)
        {
            this.projection = projection;
            this.predicate = predicate;
            this.join = join;
        }

        public ProjectNode getProjection()
        {
            return projection;
        }

        public RowExpression getPredicate()
        {
            return predicate;
        }

        public JoinNode getJoin()
        {
            return join;
        }
    }

    private static class MinMaxFacts
    {
        private final VariableReferenceExpression key;
        private final VariableReferenceExpression value;
        private final VariableReferenceExpression min;
        private final VariableReferenceExpression max;
        private final FilteredScan source;

        private MinMaxFacts(
                VariableReferenceExpression key,
                VariableReferenceExpression value,
                VariableReferenceExpression min,
                VariableReferenceExpression max,
                FilteredScan source)
        {
            this.key = key;
            this.value = value;
            this.min = min;
            this.max = max;
            this.source = source;
        }

        public VariableReferenceExpression getKey()
        {
            return key;
        }

        public VariableReferenceExpression getValue()
        {
            return value;
        }

        public VariableReferenceExpression getMin()
        {
            return min;
        }

        public VariableReferenceExpression getMax()
        {
            return max;
        }

        public TableScanNode getScan()
        {
            return source.getScan();
        }

        public Optional<RowExpression> getPredicate()
        {
            return source.getPredicate();
        }
    }

    private static class FactsJoin
    {
        private final JoinNode join;
        private final MinMaxFacts facts;
        private final PlanNode probe;
        private final VariableReferenceExpression probeKey;
        private final Optional<RowExpression> filter;

        private FactsJoin(
                JoinNode join,
                MinMaxFacts facts,
                PlanNode probe,
                VariableReferenceExpression probeKey,
                Optional<RowExpression> filter)
        {
            this.join = join;
            this.facts = facts;
            this.probe = probe;
            this.probeKey = probeKey;
            this.filter = filter;
        }

        public JoinNode getJoin()
        {
            return join;
        }

        public MinMaxFacts getFacts()
        {
            return facts;
        }

        public PlanNode getProbe()
        {
            return probe;
        }

        public VariableReferenceExpression getProbeKey()
        {
            return probeKey;
        }

        public Optional<RowExpression> getFilter()
        {
            return filter;
        }
    }

    private static class FilteredScan
    {
        private final TableScanNode scan;
        private final Optional<RowExpression> predicate;

        private FilteredScan(TableScanNode scan, Optional<RowExpression> predicate)
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

    private static class FilteringJoin
    {
        private final JoinNode join;
        private final PlanNode factSide;
        private final UniqueFilteringSource filterSource;

        private FilteringJoin(JoinNode join, PlanNode factSide, UniqueFilteringSource filterSource)
        {
            this.join = join;
            this.factSide = factSide;
            this.filterSource = filterSource;
        }

        public JoinNode getJoin()
        {
            return join;
        }

        public PlanNode getFactSide()
        {
            return factSide;
        }

        public UniqueFilteringSource getFilterSource()
        {
            return filterSource;
        }
    }

    private static class UniqueFilteringSource
    {
        private final PlanNode root;
        private final VariableReferenceExpression rootKey;

        private UniqueFilteringSource(PlanNode root, VariableReferenceExpression rootKey)
        {
            this.root = root;
            this.rootKey = rootKey;
        }

        public PlanNode getRoot()
        {
            return root;
        }

        public VariableReferenceExpression getRootKey()
        {
            return rootKey;
        }
    }

    private static class FactSource
    {
        private final PlanNode root;
        private final TableScanNode scan;
        private final VariableReferenceExpression key;
        private final VariableReferenceExpression value;

        private FactSource(PlanNode root, TableScanNode scan, VariableReferenceExpression key, VariableReferenceExpression value)
        {
            this.root = root;
            this.scan = scan;
            this.key = key;
            this.value = value;
        }

        public PlanNode getRoot()
        {
            return root;
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
    }
}
