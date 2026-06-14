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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableMetadata;
import com.facebook.presto.spi.constraints.ForeignKeyConstraint;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.constraints.UniqueConstraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.shouldPushAggregationThroughJoin;
import static com.facebook.presto.expressions.LogicalRowExpressions.extractConjuncts;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.LOCAL;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

/**
 * Pushes an aggregation below lookup joins that trusted constraints prove are
 * cardinality preserving.
 *
 * The rule targets shapes like:
 *
 * <pre>
 * - Aggregation(GROUP BY lookup payload columns; sum(base expression))
 *   - Project(payload columns, base expression)
 *     - Join(base.lookup_key = lookup.key)
 *       - base
 *       - lookup -- trusted unique/PK on lookup.key
 * </pre>
 *
 * and rewrites them to:
 *
 * <pre>
 * - Project(original aggregation outputs)
 *   - Join(agg.lookup_key = lookup.key)
 *     - Aggregation(GROUP BY base.lookup_key; sum(base expression))
 *       - Project(base.lookup_key, base expression)
 *         - base
 *     - lookup
 * </pre>
 *
 * The lookup join is preserved after the aggregation, so lookup payload columns
 * are still available. The rewrite is only valid when the base side has a
 * trusted non-null FK to a trusted unique/PK lookup key. That proves the inner
 * lookup join neither filters nor duplicates base rows. If the pre-aggregation
 * keys do not prove one output row per original grouping key, the rule keeps a
 * final aggregation above the lookup joins.
 */
public class PushAggregationThroughCardinalityPreservingLookupJoin
        implements Rule<AggregationNode>
{
    private static final Pattern<AggregationNode> PATTERN = typeOf(AggregationNode.class);

    private final Metadata metadata;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public PushAggregationThroughCardinalityPreservingLookupJoin(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.functionAndTypeManager = metadata.getFunctionAndTypeManager();
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
        if (aggregation.getStep() != AggregationNode.Step.SINGLE
                || aggregation.getGroupingSetCount() != 1
                || aggregation.getHashVariable().isPresent()
                || aggregation.getGroupIdVariable().isPresent()
                || aggregation.getGroupingKeys().isEmpty()) {
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

        if (!aggregation.getAggregations().values().stream().allMatch(this::isSupportedSumAggregation)) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> groupingKeys = ImmutableSet.copyOf(aggregation.getGroupingKeys());
        Optional<PeelResult> peel = peelLookupJoins(source, groupingKeys, context);
        if (!peel.isPresent() || peel.get().getLookups().isEmpty()) {
            return Result.empty();
        }

        PlanNode base = peel.get().getBase();
        Set<VariableReferenceExpression> baseOutputs = ImmutableSet.copyOf(base.getOutputVariables());

        ImmutableSet.Builder<VariableReferenceExpression> aggregateInputVariables = ImmutableSet.builder();
        for (AggregationNode.Aggregation aggregate : aggregation.getAggregations().values()) {
            aggregateInputVariables.addAll(extractAggregateInputVariables(aggregate));
        }

        Assignments.Builder preProjectAssignments = Assignments.builder();
        LinkedHashSet<VariableReferenceExpression> preGroupingKeys = new LinkedHashSet<>();
        for (VariableReferenceExpression groupingKey : aggregation.getGroupingKeys()) {
            if (baseOutputs.contains(groupingKey)) {
                preGroupingKeys.add(groupingKey);
                preProjectAssignments.put(groupingKey, groupingKey);
            }
        }

        for (LookupJoin lookup : peel.get().getLookups()) {
            for (VariableReferenceExpression baseKey : lookup.getBaseKeys()) {
                if (baseOutputs.contains(baseKey)) {
                    preGroupingKeys.add(baseKey);
                    preProjectAssignments.put(baseKey, baseKey);
                }
            }
        }

        if (preGroupingKeys.isEmpty()) {
            return Result.empty();
        }

        for (VariableReferenceExpression input : aggregateInputVariables.build()) {
            Optional<RowExpression> baseExpression = mapThroughProject(input, project);
            if (!baseExpression.isPresent()) {
                return Result.empty();
            }
            if (!baseOutputs.containsAll(VariablesExtractor.extractUnique(baseExpression.get()))) {
                return Result.empty();
            }
            preProjectAssignments.put(input, baseExpression.get());
        }

        if (!allGroupingKeysRestored(aggregation.getGroupingKeys(), baseOutputs, peel.get().getLookups())) {
            return Result.empty();
        }

        if (containsAggregation(base, context)) {
            return tryProjectPreAggregatedResult(aggregation, project, base, preGroupingKeys, peel.get().getLookups(), context)
                    .map(Result::ofPlanNode)
                    .orElseGet(Result::empty);
        }

        ProjectNode preProject = new ProjectNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                base,
                preProjectAssignments.build(),
                LOCAL);

        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> preAggregations = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, AggregationNode.Aggregation> finalAggregations = ImmutableMap.builder();
        ImmutableMap.Builder<VariableReferenceExpression, VariableReferenceExpression> preAggregateOutputs = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry : aggregation.getAggregations().entrySet()) {
            VariableReferenceExpression preAggregateOutput = context.getVariableAllocator().newVariable(entry.getKey(), "pre");
            preAggregations.put(preAggregateOutput, entry.getValue());
            finalAggregations.put(entry.getKey(), sum(preAggregateOutput));
            preAggregateOutputs.put(entry.getKey(), preAggregateOutput);
        }
        Map<VariableReferenceExpression, VariableReferenceExpression> preAggregateOutputMap = preAggregateOutputs.build();

        Integer aggregationId = Integer.parseInt(context.getIdAllocator().getNextId().getId());
        AggregationNode preAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                preProject,
                preAggregations.build(),
                singleGroupingSet(ImmutableList.copyOf(preGroupingKeys)),
                ImmutableList.of(),
                aggregation.getStep(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(aggregationId));

        Optional<PlanNode> restoredLookupJoins = restoreLookupJoins(preAggregation, peel.get().getLookups(), context);
        if (!restoredLookupJoins.isPresent()) {
            return Result.empty();
        }
        PlanNode rewritten = restoredLookupJoins.get();

        if (canProjectPreAggregations(aggregation.getGroupingKeys(), preGroupingKeys, peel.get().getLookups()) ||
                isKnownUnique(rewritten, aggregation.getGroupingKeys(), context)) {
            Assignments.Builder finalAssignments = Assignments.builder();
            for (VariableReferenceExpression variable : aggregation.getOutputVariables()) {
                finalAssignments.put(variable, preAggregateOutputMap.getOrDefault(variable, variable));
            }
            return Result.ofPlanNode(new ProjectNode(
                    aggregation.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    rewritten,
                    finalAssignments.build(),
                    LOCAL));
        }

        AggregationNode finalAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                rewritten,
                finalAggregations.build(),
                aggregation.getGroupingSets(),
                ImmutableList.of(),
                aggregation.getStep(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(aggregationId));

        Assignments.Builder finalAssignments = Assignments.builder();
        aggregation.getOutputVariables().forEach(variable -> finalAssignments.put(variable, variable));
        return Result.ofPlanNode(new ProjectNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                finalAggregation,
                finalAssignments.build(),
                LOCAL));
    }

    private Optional<PlanNode> tryProjectPreAggregatedResult(
            AggregationNode aggregation,
            Optional<ProjectNode> project,
            PlanNode base,
            Set<VariableReferenceExpression> preGroupingKeys,
            List<LookupJoin> lookups,
            Context context)
    {
        Optional<PlanNode> restoredLookupJoins = restoreLookupJoins(base, lookups, context);
        if (!restoredLookupJoins.isPresent()) {
            return Optional.empty();
        }

        PlanNode rewritten = restoredLookupJoins.get();
        if (!canProjectPreAggregations(aggregation.getGroupingKeys(), preGroupingKeys, lookups) &&
                !isKnownUnique(rewritten, aggregation.getGroupingKeys(), context)) {
            return Optional.empty();
        }

        if (!isKnownUnique(rewritten, aggregation.getGroupingKeys(), context) &&
                !isKnownUnique(base, ImmutableList.copyOf(preGroupingKeys), context)) {
            return Optional.empty();
        }

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression variable : aggregation.getOutputVariables()) {
            AggregationNode.Aggregation aggregate = aggregation.getAggregations().get(variable);
            if (aggregate == null) {
                if (!rewritten.getOutputVariables().contains(variable)) {
                    return Optional.empty();
                }
                assignments.put(variable, variable);
                continue;
            }

            Optional<VariableReferenceExpression> preAggregateOutput = mapAggregationInputVariable(aggregate, project);
            if (!preAggregateOutput.isPresent() || !rewritten.getOutputVariables().contains(preAggregateOutput.get())) {
                return Optional.empty();
            }
            assignments.put(variable, preAggregateOutput.get());
        }

        return Optional.of(new ProjectNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                rewritten,
                assignments.build(),
                LOCAL));
    }

    private Optional<PlanNode> restoreLookupJoins(
            PlanNode base,
            List<LookupJoin> lookups,
            Context context)
    {
        PlanNode rewritten = base;
        for (int i = lookups.size() - 1; i >= 0; i--) {
            LookupJoin lookup = lookups.get(i);
            if (!rewritten.getOutputVariables().containsAll(lookup.getBaseKeys())) {
                return Optional.empty();
            }

            LinkedHashSet<VariableReferenceExpression> outputs = new LinkedHashSet<>();
            outputs.addAll(rewritten.getOutputVariables());
            outputs.addAll(lookup.getLookupSide().getOutputVariables());

            rewritten = new JoinNode(
                    lookup.getSource().getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    JoinType.INNER,
                    rewritten,
                    lookup.getLookupSide(),
                    lookup.getCriteria(),
                    ImmutableList.copyOf(outputs),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    emptyMap());
        }
        return Optional.of(rewritten);
    }

    private Optional<VariableReferenceExpression> mapAggregationInputVariable(
            AggregationNode.Aggregation aggregation,
            Optional<ProjectNode> project)
    {
        if (aggregation.getArguments().size() != 1 ||
                !(aggregation.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }

        Optional<RowExpression> mapped = mapThroughProject((VariableReferenceExpression) aggregation.getArguments().get(0), project);
        if (!mapped.isPresent() || !(mapped.get() instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }
        return Optional.of((VariableReferenceExpression) mapped.get());
    }

    private boolean canProjectPreAggregations(
            List<VariableReferenceExpression> finalGroupingKeys,
            Set<VariableReferenceExpression> preGroupingKeys,
            List<LookupJoin> lookups)
    {
        Set<VariableReferenceExpression> finalGroupingKeySet = ImmutableSet.copyOf(finalGroupingKeys);
        for (VariableReferenceExpression preGroupingKey : preGroupingKeys) {
            if (finalGroupingKeySet.contains(preGroupingKey)) {
                continue;
            }
            if (isEquatedToFinalGroupingKey(preGroupingKey, finalGroupingKeySet, lookups)) {
                continue;
            }
            return false;
        }
        return true;
    }

    private boolean isEquatedToFinalGroupingKey(
            VariableReferenceExpression preGroupingKey,
            Set<VariableReferenceExpression> finalGroupingKeys,
            List<LookupJoin> lookups)
    {
        for (LookupJoin lookup : lookups) {
            for (int i = 0; i < lookup.getBaseKeys().size(); i++) {
                if (lookup.getBaseKeys().get(i).equals(preGroupingKey) &&
                        finalGroupingKeys.contains(lookup.getLookupKeys().get(i))) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isSupportedSumAggregation(AggregationNode.Aggregation aggregation)
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
        return functionAndTypeManager.lookupFunction("sum", fromTypes(input.getType())).equals(aggregation.getFunctionHandle());
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

    private Optional<PeelResult> peelLookupJoins(
            PlanNode root,
            Set<VariableReferenceExpression> groupingKeys,
            Context context)
    {
        PlanNode current = skipIdentityProjects(root, context);
        List<LookupJoin> lookups = new ArrayList<>();

        while (current instanceof JoinNode) {
            JoinNode join = (JoinNode) current;
            if (!isPlainInnerJoin(join)) {
                break;
            }

            Optional<LookupJoin> lookup = trySplitLookupJoin(join, true, groupingKeys, context);
            if (!lookup.isPresent()) {
                lookup = trySplitLookupJoin(join, false, groupingKeys, context);
            }
            if (!lookup.isPresent()) {
                break;
            }

            lookups.add(lookup.get());
            current = skipIdentityProjects(lookup.get().getBaseSide(), context);
        }

        return Optional.of(new PeelResult(current, lookups));
    }

    private PlanNode skipIdentityProjects(PlanNode node, Context context)
    {
        while (true) {
            node = context.getLookup().resolve(node);
            if (!(node instanceof ProjectNode) || !isIdentityProject((ProjectNode) node)) {
                return node;
            }
            node = ((ProjectNode) node).getSource();
        }
    }

    private boolean isIdentityProject(ProjectNode project)
    {
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : project.getAssignments().entrySet()) {
            if (!(assignment.getValue() instanceof VariableReferenceExpression) ||
                    !assignment.getKey().equals(assignment.getValue())) {
                return false;
            }
        }
        return true;
    }

    private Optional<LookupJoin> trySplitLookupJoin(
            JoinNode join,
            boolean lookupOnLeft,
            Set<VariableReferenceExpression> groupingKeys,
            Context context)
    {
        PlanNode left = context.getLookup().resolve(join.getLeft());
        PlanNode right = context.getLookup().resolve(join.getRight());
        PlanNode lookupSide = lookupOnLeft ? left : right;
        PlanNode baseSide = lookupOnLeft ? right : left;

        if (lookupSide.getOutputVariables().stream().noneMatch(groupingKeys::contains)) {
            return Optional.empty();
        }

        Optional<List<VariableReferenceExpression>> lookupKeys = keysForSide(join.getCriteria(), lookupSide);
        Optional<List<VariableReferenceExpression>> baseKeys = keysForSide(join.getCriteria(), baseSide);
        if (!lookupKeys.isPresent() || !baseKeys.isPresent()) {
            return Optional.empty();
        }

        if (!isKnownUnique(lookupSide, lookupKeys.get(), context)) {
            return Optional.empty();
        }

        if (!hasTrustedForeignKey(baseSide, baseKeys.get(), lookupSide, lookupKeys.get(), context.getSession(), context)) {
            return Optional.empty();
        }

        ImmutableList.Builder<EquiJoinClause> criteria = ImmutableList.builder();
        for (int i = 0; i < baseKeys.get().size(); i++) {
            criteria.add(new EquiJoinClause(baseKeys.get().get(i), lookupKeys.get().get(i)));
        }

        return Optional.of(new LookupJoin(join, baseSide, lookupSide, baseKeys.get(), lookupKeys.get(), criteria.build()));
    }

    private boolean isPlainInnerJoin(JoinNode join)
    {
        return join.getType() == JoinType.INNER && !join.getFilter().isPresent() && !join.getCriteria().isEmpty();
    }

    private Set<VariableReferenceExpression> extractAggregateInputVariables(AggregationNode.Aggregation aggregate)
    {
        ImmutableSet.Builder<VariableReferenceExpression> variables = ImmutableSet.builder();
        variables.addAll(VariablesExtractor.extractUnique(aggregate.getArguments()));
        aggregate.getFilter().ifPresent(filter -> variables.addAll(VariablesExtractor.extractUnique(filter)));
        aggregate.getMask().ifPresent(variables::add);
        aggregate.getOrderBy().map(OrderingScheme::getOrderByVariables).ifPresent(variables::addAll);
        return variables.build();
    }

    private Optional<RowExpression> mapThroughProject(VariableReferenceExpression variable, Optional<ProjectNode> project)
    {
        if (!project.isPresent()) {
            return Optional.of(variable);
        }

        RowExpression expression = project.get().getAssignments().get(variable);
        return Optional.ofNullable(expression);
    }

    private boolean allGroupingKeysRestored(
            List<VariableReferenceExpression> groupingKeys,
            Set<VariableReferenceExpression> baseOutputs,
            List<LookupJoin> lookups)
    {
        ImmutableSet.Builder<VariableReferenceExpression> restored = ImmutableSet.builder();
        restored.addAll(baseOutputs);
        lookups.forEach(lookup -> restored.addAll(lookup.getLookupSide().getOutputVariables()));
        Set<VariableReferenceExpression> restoredVariables = restored.build();
        return groupingKeys.stream().allMatch(restoredVariables::contains);
    }

    private boolean hasTrustedForeignKey(
            PlanNode source,
            List<VariableReferenceExpression> sourceKeys,
            PlanNode target,
            List<VariableReferenceExpression> targetKeys,
            Session session,
            Context context)
    {
        Optional<TableKeys> sourceTableKeys = traceToTableKeys(source, sourceKeys, context, session, true);
        Optional<TableKeys> targetTableKeys = traceToTableKeys(target, targetKeys, context, session, false);
        if (!sourceTableKeys.isPresent() || !targetTableKeys.isPresent()) {
            return false;
        }

        if (!areKnownNotNull(sourceTableKeys.get(), session)) {
            return false;
        }

        for (TableConstraint<ColumnHandle> constraint : sourceTableKeys.get().getScan().getTableConstraints()) {
            if (!(constraint instanceof ForeignKeyConstraint) || !(constraint.isEnabled() || constraint.isRely())) {
                continue;
            }

            ForeignKeyConstraint<ColumnHandle> foreignKey = (ForeignKeyConstraint<ColumnHandle>) constraint;
            if (ImmutableList.copyOf(foreignKey.getColumns()).equals(sourceTableKeys.get().getColumns()) &&
                    foreignKey.getReferencedTable().equals(targetTableKeys.get().getTableName()) &&
                    ImmutableList.copyOf(foreignKey.getReferencedColumns()).equals(targetTableKeys.get().getColumnNames())) {
                return true;
            }
        }
        return false;
    }

    private Optional<TableKeys> traceToTableKeys(
            PlanNode node,
            List<VariableReferenceExpression> keys,
            Context context,
            Session session,
            boolean allowFiltering)
    {
        node = context.getLookup().resolve(node);
        if (keys.isEmpty() || !node.getOutputVariables().containsAll(keys)) {
            return Optional.empty();
        }

        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            ImmutableList.Builder<VariableReferenceExpression> sourceKeys = ImmutableList.builder();
            for (VariableReferenceExpression key : keys) {
                RowExpression assignment = project.getAssignments().get(key);
                if (!(assignment instanceof VariableReferenceExpression)) {
                    return Optional.empty();
                }
                sourceKeys.add((VariableReferenceExpression) assignment);
            }
            return traceToTableKeys(project.getSource(), sourceKeys.build(), context, session, allowFiltering);
        }

        if (node instanceof FilterNode) {
            FilterNode filter = (FilterNode) node;
            if (!allowFiltering && !isRedundantNotNullFilter(filter, context, session)) {
                return Optional.empty();
            }
            return traceToTableKeys(filter.getSource(), keys, context, session, allowFiltering);
        }

        if (node instanceof AggregationNode) {
            AggregationNode aggregation = (AggregationNode) node;
            if (!allowFiltering ||
                    aggregation.getGroupingSetCount() != 1 ||
                    aggregation.getGroupIdVariable().isPresent() ||
                    !aggregation.getGroupingKeys().containsAll(keys)) {
                return Optional.empty();
            }
            return traceToTableKeys(aggregation.getSource(), keys, context, session, allowFiltering);
        }

        if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            PlanNode left = context.getLookup().resolve(join.getLeft());
            PlanNode right = context.getLookup().resolve(join.getRight());
            if (left.getOutputVariables().containsAll(keys)) {
                return traceToTableKeys(left, keys, context, session, allowFiltering);
            }
            if (right.getOutputVariables().containsAll(keys)) {
                return traceToTableKeys(right, keys, context, session, allowFiltering);
            }
            return Optional.empty();
        }

        if (!(node instanceof TableScanNode)) {
            return Optional.empty();
        }

        TableScanNode scan = (TableScanNode) node;
        if (!allowFiltering && hasNonRedundantConstraint(scan, session)) {
            return Optional.empty();
        }

        ImmutableList.Builder<ColumnHandle> columns = ImmutableList.builder();
        ImmutableList.Builder<String> columnNames = ImmutableList.builder();
        for (VariableReferenceExpression key : keys) {
            ColumnHandle column = scan.getAssignments().get(key);
            if (column == null) {
                return Optional.empty();
            }
            columns.add(column);
            columnNames.add(metadata.getColumnMetadata(session, scan.getTable(), column).getName());
        }

        TableMetadata tableMetadata = metadata.getTableMetadata(session, scan.getTable());
        return Optional.of(new TableKeys(scan, tableMetadata.getTable(), columns.build(), columnNames.build()));
    }

    private boolean isRedundantNotNullFilter(FilterNode filter, Context context, Session session)
    {
        PlanNode source = context.getLookup().resolve(filter.getSource());
        if (!(source instanceof TableScanNode)) {
            return false;
        }

        TableScanNode scan = (TableScanNode) source;
        for (RowExpression conjunct : extractConjuncts(filter.getPredicate())) {
            Optional<VariableReferenceExpression> variable = extractIsNotNullVariable(conjunct);
            if (!variable.isPresent()) {
                return false;
            }

            ColumnHandle column = scan.getAssignments().get(variable.get());
            if (column == null || !isKnownNotNull(scan, column, session)) {
                return false;
            }
        }
        return true;
    }

    private Optional<VariableReferenceExpression> extractIsNotNullVariable(RowExpression expression)
    {
        if (!(expression instanceof CallExpression)) {
            return Optional.empty();
        }

        CallExpression call = (CallExpression) expression;
        if (!functionResolution.isNotFunction(call.getFunctionHandle()) ||
                call.getArguments().size() != 1 ||
                !(call.getArguments().get(0) instanceof SpecialFormExpression)) {
            return Optional.empty();
        }

        SpecialFormExpression isNull = (SpecialFormExpression) call.getArguments().get(0);
        if (isNull.getForm() != IS_NULL ||
                isNull.getArguments().size() != 1 ||
                !(isNull.getArguments().get(0) instanceof VariableReferenceExpression)) {
            return Optional.empty();
        }

        return Optional.of((VariableReferenceExpression) isNull.getArguments().get(0));
    }

    private boolean hasNonRedundantConstraint(TableScanNode scan, Session session)
    {
        return !isAllOrKnownNotNullOnly(scan, scan.getCurrentConstraint(), session) ||
                !isAllOrKnownNotNullOnly(scan, scan.getEnforcedConstraint(), session);
    }

    private boolean isAllOrKnownNotNullOnly(TableScanNode scan, TupleDomain<ColumnHandle> constraint, Session session)
    {
        if (constraint == null || constraint.isAll()) {
            return true;
        }

        if (!constraint.getDomains().isPresent()) {
            return false;
        }

        for (Map.Entry<ColumnHandle, Domain> entry : constraint.getDomains().get().entrySet()) {
            Domain domain = entry.getValue();
            if (domain.isNullAllowed() || !domain.getValues().isAll()) {
                return false;
            }
            if (!isKnownNotNull(scan, entry.getKey(), session)) {
                return false;
            }
        }
        return true;
    }

    private boolean areKnownNotNull(TableKeys tableKeys, Session session)
    {
        for (ColumnHandle column : tableKeys.getColumns()) {
            if (!isKnownNotNull(tableKeys.getScan(), column, session)) {
                return false;
            }
        }
        return true;
    }

    private boolean isKnownNotNull(TableScanNode scan, ColumnHandle column, Session session)
    {
        if (!metadata.getColumnMetadata(session, scan.getTable(), column).isNullable()) {
            return true;
        }

        return scan.getTableConstraints().stream()
                .anyMatch(constraint -> (constraint.isEnabled() || constraint.isRely()) &&
                        (constraint instanceof NotNullConstraint || constraint instanceof PrimaryKeyConstraint) &&
                        constraint.getColumns().contains(column));
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

        if (node instanceof AggregationNode) {
            AggregationNode aggregation = (AggregationNode) node;
            if (aggregation.getGroupingSetCount() != 1 ||
                    aggregation.getGroupIdVariable().isPresent()) {
                return false;
            }

            Set<VariableReferenceExpression> groupingKeys = ImmutableSet.copyOf(aggregation.getGroupingKeys());
            if (variables.containsAll(groupingKeys)) {
                return true;
            }

            if (!groupingKeys.containsAll(variables)) {
                return false;
            }

            return functionallyDetermines(
                    aggregation.getSource(),
                    ImmutableSet.copyOf(variables),
                    groupingKeys,
                    context);
        }

        if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            if (!isPlainInnerJoin(join)) {
                return false;
            }

            PlanNode left = context.getLookup().resolve(join.getLeft());
            PlanNode right = context.getLookup().resolve(join.getRight());

            ImmutableList.Builder<VariableReferenceExpression> leftVariables = ImmutableList.builder();
            ImmutableList.Builder<VariableReferenceExpression> rightVariables = ImmutableList.builder();
            for (VariableReferenceExpression variable : variables) {
                boolean fromLeft = left.getOutputVariables().contains(variable);
                boolean fromRight = right.getOutputVariables().contains(variable);
                if (fromLeft == fromRight) {
                    return false;
                }
                if (fromLeft) {
                    leftVariables.add(variable);
                }
                else {
                    rightVariables.add(variable);
                }
            }

            List<VariableReferenceExpression> requestedLeftVariables = leftVariables.build();
            List<VariableReferenceExpression> requestedRightVariables = rightVariables.build();
            Optional<List<VariableReferenceExpression>> leftJoinKeys = keysForSide(join.getCriteria(), left);
            Optional<List<VariableReferenceExpression>> rightJoinKeys = keysForSide(join.getCriteria(), right);
            if (!leftJoinKeys.isPresent() || !rightJoinKeys.isPresent()) {
                return false;
            }

            boolean leftRowsAreNotDuplicated = isKnownUnique(right, rightJoinKeys.get(), context);
            boolean rightRowsAreNotDuplicated = isKnownUnique(left, leftJoinKeys.get(), context);
            boolean leftVariablesAreUnique = !requestedLeftVariables.isEmpty() && isKnownUnique(left, requestedLeftVariables, context);
            boolean rightVariablesAreUnique = !requestedRightVariables.isEmpty() && isKnownUnique(right, requestedRightVariables, context);
            if (requestedRightVariables.isEmpty()) {
                return leftRowsAreNotDuplicated && leftVariablesAreUnique;
            }
            if (requestedLeftVariables.isEmpty()) {
                return rightRowsAreNotDuplicated && rightVariablesAreUnique;
            }
            return (leftRowsAreNotDuplicated && leftVariablesAreUnique) ||
                    (rightRowsAreNotDuplicated && rightVariablesAreUnique);
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
                            columns.containsAll(constraint.getColumns()));
        }

        return false;
    }

    private boolean functionallyDetermines(
            PlanNode node,
            Set<VariableReferenceExpression> determinants,
            Set<VariableReferenceExpression> dependents,
            Context context)
    {
        node = context.getLookup().resolve(node);
        if (determinants.containsAll(dependents)) {
            return true;
        }
        if (!node.getOutputVariables().containsAll(determinants) ||
                !node.getOutputVariables().containsAll(dependents)) {
            return false;
        }

        if (node instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) node;
            Optional<Set<VariableReferenceExpression>> sourceDeterminants = mapVariablesThroughProject(project, determinants);
            Optional<Set<VariableReferenceExpression>> sourceDependents = mapVariablesThroughProject(project, dependents);
            return sourceDeterminants.isPresent() &&
                    sourceDependents.isPresent() &&
                    functionallyDetermines(project.getSource(), sourceDeterminants.get(), sourceDependents.get(), context);
        }

        if (node instanceof FilterNode) {
            return functionallyDetermines(((FilterNode) node).getSource(), determinants, dependents, context);
        }

        if (node instanceof AggregationNode) {
            AggregationNode aggregation = (AggregationNode) node;
            if (aggregation.getGroupingSetCount() != 1 ||
                    aggregation.getGroupIdVariable().isPresent()) {
                return false;
            }

            Set<VariableReferenceExpression> groupingKeys = ImmutableSet.copyOf(aggregation.getGroupingKeys());
            if (determinants.containsAll(groupingKeys)) {
                return true;
            }
            if (!groupingKeys.containsAll(determinants) ||
                    !groupingKeys.containsAll(dependents)) {
                return false;
            }
            return functionallyDetermines(aggregation.getSource(), determinants, dependents, context);
        }

        if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            if (!isPlainInnerJoin(join)) {
                return false;
            }

            PlanNode left = context.getLookup().resolve(join.getLeft());
            PlanNode right = context.getLookup().resolve(join.getRight());
            Set<VariableReferenceExpression> leftDependents = variablesFromSide(dependents, left);
            Set<VariableReferenceExpression> rightDependents = variablesFromSide(dependents, right);
            if (leftDependents.size() + rightDependents.size() != dependents.size()) {
                return false;
            }

            Set<VariableReferenceExpression> leftDeterminants = variablesFromSide(determinants, left);
            Set<VariableReferenceExpression> rightDeterminants = variablesFromSide(determinants, right);
            if (leftDeterminants.size() + rightDeterminants.size() != determinants.size()) {
                return false;
            }

            boolean leftDetermined = leftDependents.isEmpty() ||
                    (!leftDeterminants.isEmpty() && functionallyDetermines(left, leftDeterminants, leftDependents, context));
            boolean rightDetermined = rightDependents.isEmpty() ||
                    (!rightDeterminants.isEmpty() && functionallyDetermines(right, rightDeterminants, rightDependents, context));
            return leftDetermined && rightDetermined;
        }

        if (node instanceof TableScanNode) {
            TableScanNode scan = (TableScanNode) node;
            LinkedHashSet<ColumnHandle> determinantColumns = new LinkedHashSet<>();
            for (VariableReferenceExpression variable : determinants) {
                ColumnHandle column = scan.getAssignments().get(variable);
                if (column == null) {
                    return false;
                }
                determinantColumns.add(column);
            }

            return scan.getTableConstraints().stream()
                    .anyMatch(constraint -> (constraint.isEnabled() || constraint.isRely()) &&
                            (constraint instanceof PrimaryKeyConstraint || constraint instanceof UniqueConstraint) &&
                            determinantColumns.containsAll(constraint.getColumns()));
        }

        return false;
    }

    private Optional<Set<VariableReferenceExpression>> mapVariablesThroughProject(
            ProjectNode project,
            Set<VariableReferenceExpression> variables)
    {
        ImmutableSet.Builder<VariableReferenceExpression> mapped = ImmutableSet.builder();
        for (VariableReferenceExpression variable : variables) {
            RowExpression assignment = project.getAssignments().get(variable);
            if (!(assignment instanceof VariableReferenceExpression)) {
                return Optional.empty();
            }
            mapped.add((VariableReferenceExpression) assignment);
        }
        return Optional.of(mapped.build());
    }

    private Set<VariableReferenceExpression> variablesFromSide(Set<VariableReferenceExpression> variables, PlanNode side)
    {
        ImmutableSet.Builder<VariableReferenceExpression> result = ImmutableSet.builder();
        for (VariableReferenceExpression variable : variables) {
            if (side.getOutputVariables().contains(variable)) {
                result.add(variable);
            }
        }
        return result.build();
    }

    private Optional<List<VariableReferenceExpression>> keysForSide(List<EquiJoinClause> criteria, PlanNode side)
    {
        ImmutableList.Builder<VariableReferenceExpression> keys = ImmutableList.builder();
        for (EquiJoinClause clause : criteria) {
            if (side.getOutputVariables().contains(clause.getLeft())) {
                keys.add(clause.getLeft());
            }
            else if (side.getOutputVariables().contains(clause.getRight())) {
                keys.add(clause.getRight());
            }
            else {
                return Optional.empty();
            }
        }
        return Optional.of(keys.build());
    }

    private static class PeelResult
    {
        private final PlanNode base;
        private final List<LookupJoin> lookups;

        private PeelResult(PlanNode base, List<LookupJoin> lookups)
        {
            this.base = base;
            this.lookups = ImmutableList.copyOf(lookups);
        }

        private PlanNode getBase()
        {
            return base;
        }

        private List<LookupJoin> getLookups()
        {
            return lookups;
        }
    }

    private static class LookupJoin
    {
        private final JoinNode source;
        private final PlanNode baseSide;
        private final PlanNode lookupSide;
        private final List<VariableReferenceExpression> baseKeys;
        private final List<VariableReferenceExpression> lookupKeys;
        private final List<EquiJoinClause> criteria;

        private LookupJoin(
                JoinNode source,
                PlanNode baseSide,
                PlanNode lookupSide,
                List<VariableReferenceExpression> baseKeys,
                List<VariableReferenceExpression> lookupKeys,
                List<EquiJoinClause> criteria)
        {
            this.source = source;
            this.baseSide = baseSide;
            this.lookupSide = lookupSide;
            this.baseKeys = ImmutableList.copyOf(baseKeys);
            this.lookupKeys = ImmutableList.copyOf(lookupKeys);
            this.criteria = ImmutableList.copyOf(criteria);
        }

        private JoinNode getSource()
        {
            return source;
        }

        private PlanNode getBaseSide()
        {
            return baseSide;
        }

        private PlanNode getLookupSide()
        {
            return lookupSide;
        }

        private List<VariableReferenceExpression> getBaseKeys()
        {
            return baseKeys;
        }

        private List<VariableReferenceExpression> getLookupKeys()
        {
            return lookupKeys;
        }

        private List<EquiJoinClause> getCriteria()
        {
            return criteria;
        }
    }

    private static class TableKeys
    {
        private final TableScanNode scan;
        private final SchemaTableName tableName;
        private final List<ColumnHandle> columns;
        private final List<String> columnNames;

        private TableKeys(TableScanNode scan, SchemaTableName tableName, List<ColumnHandle> columns, List<String> columnNames)
        {
            this.scan = scan;
            this.tableName = tableName;
            this.columns = ImmutableList.copyOf(columns);
            this.columnNames = ImmutableList.copyOf(columnNames);
        }

        private TableScanNode getScan()
        {
            return scan;
        }

        private SchemaTableName getTableName()
        {
            return tableName;
        }

        private List<ColumnHandle> getColumns()
        {
            return columns;
        }

        private List<String> getColumnNames()
        {
            return columnNames;
        }
    }
}
