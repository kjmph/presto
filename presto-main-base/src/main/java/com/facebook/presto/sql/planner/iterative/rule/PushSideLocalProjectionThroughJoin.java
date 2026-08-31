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
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isPushSideLocalProjectionThroughJoin;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.JAVA_BUILTIN_NAMESPACE;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.REMOTE;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.iterative.rule.PushProjectionThroughJoin.getJoinRequiredVariables;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static java.util.Objects.requireNonNull;

/**
 * Reduces repartition payload by evaluating safe deterministic expressions on
 * the side of an inner join that supplies all of their inputs.
 *
 * <p>The rule runs after exchanges have been added and only handles partitioned
 * equijoins with an actual remote repartition on the rewritten side. This keeps
 * expression placement from changing join enumeration or distribution. The
 * normal projection pushdown pass subsequently moves each new projection below
 * its exchange.
 *
 * <p>Whole side-local assignments are pushed first. Their expressions are then
 * reused in mixed assignments above the join. Additional nested expressions
 * are extracted only from positions that are evaluated unconditionally. In
 * particular, expressions are never lifted from conditional result branches,
 * TRY arguments, or lambda bodies.
 */
public class PushSideLocalProjectionThroughJoin
        implements Rule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();
    private static final QualifiedObjectName SUBSTR_FUNCTION = QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, "substr");

    private final DeterminismEvaluator determinismEvaluator;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final FunctionResolution functionResolution;

    public PushSideLocalProjectionThroughJoin(FunctionAndTypeManager functionAndTypeManager)
    {
        requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.functionAndTypeManager = functionAndTypeManager;
        this.determinismEvaluator = new RowExpressionDeterminismEvaluator(functionAndTypeManager);
        this.functionResolution = new FunctionResolution(functionAndTypeManager.getFunctionAndTypeResolver());
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isPushSideLocalProjectionThroughJoin(session);
    }

    @Override
    public Result apply(ProjectNode project, Captures captures, Context context)
    {
        if (project.getLocality() == REMOTE) {
            return Result.empty();
        }

        PlanNode source = context.getLookup().resolve(project.getSource());
        if (!(source instanceof JoinNode)) {
            return Result.empty();
        }

        JoinNode join = (JoinNode) source;
        if (join.getType() != INNER ||
                join.getCriteria().isEmpty() ||
                !join.getDistributionType().equals(Optional.of(PARTITIONED))) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> leftVariables = ImmutableSet.copyOf(join.getLeft().getOutputVariables());
        Set<VariableReferenceExpression> rightVariables = ImmutableSet.copyOf(join.getRight().getOutputVariables());
        Set<VariableReferenceExpression> joinRequiredVariables = getJoinRequiredVariables(join);
        Optional<ExchangeNode> leftRemoteRepartition = getRemoteRepartition(join.getLeft(), context);
        Optional<ExchangeNode> rightRemoteRepartition = getRemoteRepartition(join.getRight(), context);
        boolean leftHasRemoteRepartition = leftRemoteRepartition.isPresent();
        boolean rightHasRemoteRepartition = rightRemoteRepartition.isPresent();

        if (!leftHasRemoteRepartition && !rightHasRemoteRepartition) {
            return Result.empty();
        }

        ExpressionExtractor extractor = new ExpressionExtractor(
                context,
                leftVariables,
                rightVariables,
                leftHasRemoteRepartition,
                rightHasRemoteRepartition,
                joinRequiredVariables);

        // Prefer an existing project output for a whole side-local expression.
        // This lets duplicate occurrences reuse the same value instead of adding
        // another child symbol.
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : project.getAssignments().entrySet()) {
            extractor.seed(assignment.getKey(), assignment.getValue());
        }

        Assignments.Builder rewrittenAssignments = Assignments.builder();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : project.getAssignments().entrySet()) {
            rewrittenAssignments.put(assignment.getKey(), extractor.rewrite(assignment.getValue(), true));
        }
        Assignments residualAssignments = rewrittenAssignments.build();

        Map<RowExpression, Candidate> candidates = extractor.getCandidates();
        if (candidates.isEmpty()) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> residualInputs = extractUnique(residualAssignments.getExpressions());
        Assignments leftAssignments = buildChildAssignments(
                join.getLeft().getOutputVariables(),
                residualInputs,
                joinRequiredVariables,
                leftRemoteRepartition.map(PushSideLocalProjectionThroughJoin::getExchangeRequiredVariables).orElse(ImmutableSet.of()),
                candidates,
                Side.LEFT);
        Assignments rightAssignments = buildChildAssignments(
                join.getRight().getOutputVariables(),
                residualInputs,
                joinRequiredVariables,
                rightRemoteRepartition.map(PushSideLocalProjectionThroughJoin::getExchangeRequiredVariables).orElse(ImmutableSet.of()),
                candidates,
                Side.RIGHT);

        WidthChange leftWidthChange = leftHasRemoteRepartition ? compareWidths(join.getLeft().getOutputVariables(), leftAssignments.getOutputs()) : WidthChange.SAME;
        WidthChange rightWidthChange = rightHasRemoteRepartition ? compareWidths(join.getRight().getOutputVariables(), rightAssignments.getOutputs()) : WidthChange.SAME;

        // Do not trade a reduction on one remote exchange for wider rows on
        // the other, and require at least one exchange to become narrower.
        if (leftWidthChange == WidthChange.WIDER ||
                rightWidthChange == WidthChange.WIDER ||
                (leftWidthChange != WidthChange.NARROWER && rightWidthChange != WidthChange.NARROWER)) {
            return Result.empty();
        }

        PlanNode newLeft = leftHasRemoteRepartition ? new ProjectNode(
                project.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                join.getLeft(),
                leftAssignments,
                project.getLocality()) : join.getLeft();
        PlanNode newRight = rightHasRemoteRepartition ? new ProjectNode(
                project.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                join.getRight(),
                rightAssignments,
                project.getLocality()) : join.getRight();

        ImmutableList.Builder<VariableReferenceExpression> joinOutputs = ImmutableList.builder();
        newLeft.getOutputVariables().stream()
                .filter(residualInputs::contains)
                .forEach(joinOutputs::add);
        newRight.getOutputVariables().stream()
                .filter(residualInputs::contains)
                .forEach(joinOutputs::add);

        JoinNode rewrittenJoin = new JoinNode(
                join.getSourceLocation(),
                join.getId(),
                Optional.empty(),
                join.getType(),
                newLeft,
                newRight,
                join.getCriteria(),
                joinOutputs.build(),
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
                join.isRightKeysCoveredByLeftKeys());

        return Result.ofPlanNode(new ProjectNode(
                project.getSourceLocation(),
                project.getId(),
                Optional.empty(),
                rewrittenJoin,
                residualAssignments,
                project.getLocality()));
    }

    private static Assignments buildChildAssignments(
            List<VariableReferenceExpression> sourceOutputs,
            Set<VariableReferenceExpression> residualInputs,
            Set<VariableReferenceExpression> joinRequiredVariables,
            Set<VariableReferenceExpression> exchangeRequiredVariables,
            Map<RowExpression, Candidate> candidates,
            Side side)
    {
        Assignments.Builder assignments = Assignments.builder();
        sourceOutputs.stream()
                .filter(variable -> residualInputs.contains(variable) ||
                        joinRequiredVariables.contains(variable) ||
                        exchangeRequiredVariables.contains(variable))
                .forEach(variable -> assignments.put(variable, variable));
        candidates.forEach((expression, candidate) -> {
            if (candidate.getSide() == side) {
                assignments.put(candidate.getVariable(), expression);
            }
        });
        return assignments.build();
    }

    private static Optional<ExchangeNode> getRemoteRepartition(PlanNode node, Context context)
    {
        PlanNode resolved = context.getLookup().resolve(node);
        if (resolved instanceof ExchangeNode &&
                ((ExchangeNode) resolved).getType() == REPARTITION &&
                ((ExchangeNode) resolved).getScope().isRemote()) {
            return Optional.of((ExchangeNode) resolved);
        }
        return Optional.empty();
    }

    private static Set<VariableReferenceExpression> getExchangeRequiredVariables(ExchangeNode exchange)
    {
        ImmutableSet.Builder<VariableReferenceExpression> requiredVariables = ImmutableSet.builder();
        requiredVariables.addAll(exchange.getPartitioningScheme().getPartitioning().getVariableReferences());
        exchange.getPartitioningScheme().getHashColumn().ifPresent(requiredVariables::add);
        exchange.getOrderingScheme().ifPresent(ordering -> requiredVariables.addAll(ordering.getOrderByVariables()));
        return requiredVariables.build();
    }

    private enum Side
    {
        LEFT,
        RIGHT
    }

    private enum WidthChange
    {
        NARROWER,
        SAME,
        WIDER
    }

    private static WidthChange compareWidths(
            List<VariableReferenceExpression> original,
            List<VariableReferenceExpression> rewritten)
    {
        Set<VariableReferenceExpression> originalSet = ImmutableSet.copyOf(original);
        Set<VariableReferenceExpression> rewrittenSet = ImmutableSet.copyOf(rewritten);

        long removedMinimumWidth = 0;
        for (VariableReferenceExpression variable : originalSet) {
            if (!rewrittenSet.contains(variable)) {
                if (variable.getType() instanceof FixedWidthType) {
                    removedMinimumWidth += ((FixedWidthType) variable.getType()).getFixedSize();
                }
                else {
                    // Variable-width values may be empty. Count only the
                    // smallest possible payload here rather than assuming an
                    // average size that is unavailable at this optimizer phase.
                    removedMinimumWidth++;
                }
            }
        }

        long addedFixedWidth = 0;
        for (VariableReferenceExpression variable : rewrittenSet) {
            if (!originalSet.contains(variable)) {
                if (!(variable.getType() instanceof FixedWidthType)) {
                    return WidthChange.WIDER;
                }
                addedFixedWidth += ((FixedWidthType) variable.getType()).getFixedSize();
            }
        }

        if (removedMinimumWidth > addedFixedWidth) {
            return WidthChange.NARROWER;
        }
        if (removedMinimumWidth == addedFixedWidth) {
            return WidthChange.SAME;
        }
        return WidthChange.WIDER;
    }

    private static final class Candidate
    {
        private final VariableReferenceExpression variable;
        private final Side side;

        private Candidate(VariableReferenceExpression variable, Side side)
        {
            this.variable = requireNonNull(variable, "variable is null");
            this.side = requireNonNull(side, "side is null");
        }

        public VariableReferenceExpression getVariable()
        {
            return variable;
        }

        public Side getSide()
        {
            return side;
        }
    }

    private final class ExpressionExtractor
    {
        private final Context context;
        private final Set<VariableReferenceExpression> leftVariables;
        private final Set<VariableReferenceExpression> rightVariables;
        private final boolean leftEligible;
        private final boolean rightEligible;
        private final Set<VariableReferenceExpression> sourceVariables;
        private final Set<VariableReferenceExpression> joinRequiredVariables;
        private final Map<RowExpression, Candidate> candidates = new LinkedHashMap<>();

        private ExpressionExtractor(
                Context context,
                Set<VariableReferenceExpression> leftVariables,
                Set<VariableReferenceExpression> rightVariables,
                boolean leftEligible,
                boolean rightEligible,
                Set<VariableReferenceExpression> joinRequiredVariables)
        {
            this.context = requireNonNull(context, "context is null");
            this.leftVariables = requireNonNull(leftVariables, "leftVariables is null");
            this.rightVariables = requireNonNull(rightVariables, "rightVariables is null");
            this.leftEligible = leftEligible;
            this.rightEligible = rightEligible;
            this.sourceVariables = ImmutableSet.<VariableReferenceExpression>builder()
                    .addAll(leftVariables)
                    .addAll(rightVariables)
                    .build();
            this.joinRequiredVariables = requireNonNull(joinRequiredVariables, "joinRequiredVariables is null");
        }

        public Map<RowExpression, Candidate> getCandidates()
        {
            return ImmutableMap.copyOf(candidates);
        }

        public void seed(VariableReferenceExpression output, RowExpression expression)
        {
            if (sourceVariables.contains(output) || joinRequiredVariables.contains(output)) {
                return;
            }
            sideOf(expression).ifPresent(side -> {
                if (isCandidate(expression)) {
                    candidates.putIfAbsent(expression, new Candidate(output, side));
                }
            });
        }

        public RowExpression rewrite(RowExpression expression, boolean extractionAllowed)
        {
            Candidate existing = candidates.get(expression);
            if (existing != null) {
                return existing.getVariable();
            }

            if (extractionAllowed && isCandidate(expression)) {
                Side side = sideOf(expression).get();
                VariableReferenceExpression variable = context.getVariableAllocator().newVariable(expression);
                candidates.put(expression, new Candidate(variable, side));
                return variable;
            }

            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                boolean allowArguments = extractionAllowed && !functionResolution.isTryFunction(call.getFunctionHandle());
                List<RowExpression> arguments = rewriteArguments(call.getArguments(), allowArguments);
                return new CallExpression(
                        call.getSourceLocation(),
                        call.getDisplayName(),
                        call.getFunctionHandle(),
                        call.getType(),
                        arguments);
            }

            if (expression instanceof SpecialFormExpression) {
                return rewriteSpecialForm((SpecialFormExpression) expression, extractionAllowed);
            }

            // Lambda bodies have a different evaluation cardinality and scope.
            // Variables, constants, and input references do not contain useful
            // extractable children.
            return expression;
        }

        private RowExpression rewriteSpecialForm(SpecialFormExpression expression, boolean extractionAllowed)
        {
            List<RowExpression> arguments = expression.getArguments();
            List<RowExpression> rewritten = new ArrayList<>(arguments.size());

            switch (expression.getForm()) {
                case IF:
                    addRewritten(rewritten, arguments, 0, extractionAllowed);
                    addRemaining(rewritten, arguments, 1, false);
                    break;
                case SWITCH:
                    addRewritten(rewritten, arguments, 0, extractionAllowed);
                    for (int index = 1; index < arguments.size(); index++) {
                        RowExpression argument = arguments.get(index);
                        if (index == 1 && argument instanceof SpecialFormExpression &&
                                ((SpecialFormExpression) argument).getForm() == SpecialFormExpression.Form.WHEN) {
                            SpecialFormExpression when = (SpecialFormExpression) argument;
                            List<RowExpression> whenArguments = ImmutableList.of(
                                    rewrite(when.getArguments().get(0), extractionAllowed),
                                    rewrite(when.getArguments().get(1), false));
                            rewritten.add(new SpecialFormExpression(
                                    when.getSourceLocation(),
                                    when.getForm(),
                                    when.getType(),
                                    whenArguments));
                        }
                        else {
                            rewritten.add(rewrite(argument, false));
                        }
                    }
                    break;
                case IS_NULL:
                case DEREFERENCE:
                    addRemaining(rewritten, arguments, 0, extractionAllowed);
                    break;
                case ROW_CONSTRUCTOR:
                    addRemaining(rewritten, arguments, 0, extractionAllowed);
                    break;
                case NULL_IF:
                case COALESCE:
                case AND:
                case OR:
                case IN:
                    addRewritten(rewritten, arguments, 0, extractionAllowed);
                    addRemaining(rewritten, arguments, 1, false);
                    break;
                case WHEN:
                case BIND:
                    addRemaining(rewritten, arguments, 0, false);
                    break;
                default:
                    addRemaining(rewritten, arguments, 0, false);
                    break;
            }

            return new SpecialFormExpression(
                    expression.getSourceLocation(),
                    expression.getForm(),
                    expression.getType(),
                    rewritten);
        }

        private List<RowExpression> rewriteArguments(List<RowExpression> arguments, boolean extractionAllowed)
        {
            ImmutableList.Builder<RowExpression> rewritten = ImmutableList.builder();
            arguments.forEach(argument -> rewritten.add(rewrite(argument, extractionAllowed)));
            return rewritten.build();
        }

        private void addRewritten(List<RowExpression> output, List<RowExpression> arguments, int index, boolean extractionAllowed)
        {
            if (index < arguments.size()) {
                output.add(rewrite(arguments.get(index), extractionAllowed));
            }
        }

        private void addRemaining(List<RowExpression> output, List<RowExpression> arguments, int start, boolean extractionAllowed)
        {
            for (int index = start; index < arguments.size(); index++) {
                output.add(rewrite(arguments.get(index), extractionAllowed));
            }
        }

        private boolean isCandidate(RowExpression expression)
        {
            if (expression instanceof VariableReferenceExpression ||
                    expression instanceof ConstantExpression ||
                    expression instanceof LambdaDefinitionExpression ||
                    !determinismEvaluator.isDeterministic(expression) ||
                    !isSafeToPrecompute(expression)) {
                return false;
            }
            if (!(expression.getType() instanceof FixedWidthType) || !reducesWidth(expression)) {
                return false;
            }
            if (expression instanceof SpecialFormExpression) {
                SpecialFormExpression.Form form = ((SpecialFormExpression) expression).getForm();
                if (form == SpecialFormExpression.Form.WHEN || form == SpecialFormExpression.Form.BIND) {
                    return false;
                }
            }
            return sideOf(expression).isPresent();
        }

        private boolean isSafeToPrecompute(RowExpression expression)
        {
            // Determinism does not imply that an expression cannot fail. Since
            // pushing below an inner join can evaluate unmatched input rows,
            // admit only operations whose built-in implementations are total
            // over their declared input domains.
            if (expression instanceof VariableReferenceExpression || expression instanceof ConstantExpression) {
                return true;
            }
            if (expression instanceof CallExpression) {
                CallExpression call = (CallExpression) expression;
                if (!call.getArguments().stream().allMatch(this::isSafeToPrecompute)) {
                    return false;
                }

                Optional<OperatorType> operatorType = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getOperatorType();
                if (operatorType.map(OperatorType::isComparisonOperator).orElse(false) &&
                        call.getArguments().stream().map(RowExpression::getType).allMatch(this::hasTotalComparisonSemantics)) {
                    return true;
                }
                if (functionResolution.isNotFunction(call.getFunctionHandle())) {
                    return true;
                }
                if (operatorType.isPresent() &&
                        (operatorType.get() == OperatorType.ADD ||
                                operatorType.get() == OperatorType.SUBTRACT ||
                                operatorType.get() == OperatorType.MULTIPLY ||
                                operatorType.get() == OperatorType.NEGATION)) {
                    return (call.getType().equals(DOUBLE) || call.getType().equals(REAL)) &&
                            call.getArguments().stream().allMatch(argument -> argument.getType().equals(call.getType()));
                }

                QualifiedObjectName functionName = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle()).getName();
                return functionName.equals(SUBSTR_FUNCTION) && isSafeVarcharSubstring(call);
            }
            if (expression instanceof SpecialFormExpression) {
                SpecialFormExpression specialForm = (SpecialFormExpression) expression;
                if (specialForm.getForm() != SpecialFormExpression.Form.IS_NULL &&
                        specialForm.getForm() != SpecialFormExpression.Form.AND &&
                        specialForm.getForm() != SpecialFormExpression.Form.OR) {
                    return false;
                }
                return specialForm.getArguments().stream().allMatch(this::isSafeToPrecompute);
            }
            return false;
        }

        private boolean hasTotalComparisonSemantics(Type type)
        {
            return type.equals(BOOLEAN) ||
                    type.equals(TINYINT) ||
                    type.equals(SMALLINT) ||
                    type.equals(INTEGER) ||
                    type.equals(BIGINT) ||
                    type.equals(REAL) ||
                    type.equals(DOUBLE) ||
                    type instanceof VarcharType ||
                    type instanceof CharType ||
                    type instanceof VarbinaryType;
        }

        private boolean isSafeVarcharSubstring(CallExpression call)
        {
            List<RowExpression> arguments = call.getArguments();
            if (arguments.size() != 3 ||
                    !(arguments.get(0).getType() instanceof VarcharType) ||
                    !(call.getType() instanceof VarcharType)) {
                return false;
            }

            // Keep both bounds positive and within the integer domain used by
            // StringFunctions.substr so moving evaluation cannot expose index
            // arithmetic failures on rows the join would otherwise discard.
            return isPositiveBigintConstant(arguments.get(1)) &&
                    isPositiveBigintConstant(arguments.get(2));
        }

        private boolean isPositiveBigintConstant(RowExpression expression)
        {
            if (!(expression instanceof ConstantExpression)) {
                return false;
            }
            ConstantExpression constant = (ConstantExpression) expression;
            if (!constant.getType().equals(BIGINT)) {
                return false;
            }
            Object value = constant.getValue();
            return value instanceof Long && (Long) value > 0 && (Long) value <= Integer.MAX_VALUE;
        }

        private boolean reducesWidth(RowExpression expression)
        {
            long outputWidth = ((FixedWidthType) expression.getType()).getFixedSize();
            long inputWidth = 0;
            for (VariableReferenceExpression input : extractUnique(expression)) {
                if (!(input.getType() instanceof FixedWidthType)) {
                    return true;
                }
                inputWidth += ((FixedWidthType) input.getType()).getFixedSize();
            }
            return inputWidth > outputWidth;
        }

        private Optional<Side> sideOf(RowExpression expression)
        {
            Set<VariableReferenceExpression> inputs = extractUnique(expression);
            if (inputs.isEmpty()) {
                return Optional.empty();
            }
            if (leftEligible && leftVariables.containsAll(inputs)) {
                return Optional.of(Side.LEFT);
            }
            if (rightEligible && rightVariables.containsAll(inputs)) {
                return Optional.of(Side.RIGHT);
            }
            return Optional.empty();
        }
    }
}
