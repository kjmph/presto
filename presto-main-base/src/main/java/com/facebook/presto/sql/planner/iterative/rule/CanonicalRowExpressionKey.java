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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultiset;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

final class CanonicalRowExpressionKey
{
    private enum Kind
    {
        COLUMN,
        CONSTANT,
        CALL,
        SPECIAL_FORM
    }

    private final Kind kind;
    private final TypeSignature type;
    private final Optional<ColumnHandle> column;
    private final Optional<Object> constantValue;
    private final Optional<QualifiedObjectName> functionName;
    private final Optional<SpecialFormExpression.Form> specialForm;
    private final List<CanonicalRowExpressionKey> arguments;

    private CanonicalRowExpressionKey(
            Kind kind,
            TypeSignature type,
            Optional<ColumnHandle> column,
            Optional<Object> constantValue,
            Optional<QualifiedObjectName> functionName,
            Optional<SpecialFormExpression.Form> specialForm,
            List<CanonicalRowExpressionKey> arguments)
    {
        this.kind = requireNonNull(kind, "kind is null");
        this.type = requireNonNull(type, "type is null");
        this.column = requireNonNull(column, "column is null");
        this.constantValue = requireNonNull(constantValue, "constantValue is null");
        this.functionName = requireNonNull(functionName, "functionName is null");
        this.specialForm = requireNonNull(specialForm, "specialForm is null");
        this.arguments = ImmutableList.copyOf(requireNonNull(arguments, "arguments is null"));
    }

    static Optional<CanonicalRowExpressionKey> canonicalize(
            RowExpression expression,
            FunctionAndTypeManager functionAndTypeManager,
            Map<VariableReferenceExpression, ColumnHandle> assignments)
    {
        try {
            return Optional.of(expression.accept(new Visitor(functionAndTypeManager), assignments));
        }
        catch (UnsupportedOperationException ignored) {
            return Optional.empty();
        }
    }

    static Optional<ImmutableMultiset<CanonicalRowExpressionKey>> canonicalizePredicate(
            Optional<RowExpression> predicate,
            TableScanNode scan,
            FunctionAndTypeManager functionAndTypeManager,
            FunctionResolution functionResolution)
    {
        if (!predicate.isPresent()) {
            return Optional.of(ImmutableMultiset.of());
        }

        List<RowExpression> conjuncts = new ArrayList<>();
        collectConjuncts(predicate.get(), conjuncts);

        ImmutableMultiset.Builder<CanonicalRowExpressionKey> canonicalConjuncts = ImmutableMultiset.builder();
        for (RowExpression conjunct : conjuncts) {
            if (isRedundantKnownNonNull(conjunct, scan, functionResolution)) {
                continue;
            }
            Optional<CanonicalRowExpressionKey> canonicalConjunct = canonicalize(conjunct, functionAndTypeManager, scan.getAssignments());
            if (!canonicalConjunct.isPresent()) {
                return Optional.empty();
            }
            canonicalConjuncts.add(canonicalConjunct.get());
        }

        return Optional.of(canonicalConjuncts.build());
    }

    private static void collectConjuncts(RowExpression expression, List<RowExpression> conjuncts)
    {
        if (expression instanceof SpecialFormExpression &&
                ((SpecialFormExpression) expression).getForm() == SpecialFormExpression.Form.AND) {
            for (RowExpression argument : ((SpecialFormExpression) expression).getArguments()) {
                collectConjuncts(argument, conjuncts);
            }
            return;
        }
        conjuncts.add(expression);
    }

    private static boolean isRedundantKnownNonNull(
            RowExpression expression,
            TableScanNode scan,
            FunctionResolution functionResolution)
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

    private static boolean isKnownNonNull(TableScanNode scan, VariableReferenceExpression variable)
    {
        ColumnHandle column = scan.getAssignments().get(variable);
        return column != null && scan.getTableConstraints().stream()
                .anyMatch(constraint -> (constraint.isEnabled() || constraint.isRely()) &&
                        (constraint instanceof NotNullConstraint || constraint instanceof PrimaryKeyConstraint) &&
                        constraint.getColumns().contains(column));
    }

    private static CanonicalRowExpressionKey column(ColumnHandle column, TypeSignature type)
    {
        return new CanonicalRowExpressionKey(
                Kind.COLUMN,
                type,
                Optional.of(column),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    private static CanonicalRowExpressionKey constant(Object value, TypeSignature type)
    {
        if (value instanceof Block || (value != null && value.getClass().isArray())) {
            throw new UnsupportedOperationException("Unsupported constant value: " + value.getClass().getName());
        }
        return new CanonicalRowExpressionKey(
                Kind.CONSTANT,
                type,
                Optional.empty(),
                Optional.ofNullable(value),
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of());
    }

    private static CanonicalRowExpressionKey call(
            QualifiedObjectName functionName,
            TypeSignature type,
            List<CanonicalRowExpressionKey> arguments)
    {
        return new CanonicalRowExpressionKey(
                Kind.CALL,
                type,
                Optional.empty(),
                Optional.empty(),
                Optional.of(functionName),
                Optional.empty(),
                arguments);
    }

    private static CanonicalRowExpressionKey specialForm(
            SpecialFormExpression.Form form,
            TypeSignature type,
            List<CanonicalRowExpressionKey> arguments)
    {
        return new CanonicalRowExpressionKey(
                Kind.SPECIAL_FORM,
                type,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.of(form),
                arguments);
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
        CanonicalRowExpressionKey that = (CanonicalRowExpressionKey) o;
        return kind == that.kind &&
                Objects.equals(type, that.type) &&
                Objects.equals(column, that.column) &&
                Objects.equals(constantValue, that.constantValue) &&
                Objects.equals(functionName, that.functionName) &&
                Objects.equals(specialForm, that.specialForm) &&
                Objects.equals(arguments, that.arguments);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(kind, type, column, constantValue, functionName, specialForm, arguments);
    }

    private static class Visitor
            implements RowExpressionVisitor<CanonicalRowExpressionKey, Map<VariableReferenceExpression, ColumnHandle>>
    {
        private final FunctionAndTypeManager functionAndTypeManager;

        private Visitor(FunctionAndTypeManager functionAndTypeManager)
        {
            this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        }

        @Override
        public CanonicalRowExpressionKey visitVariableReference(VariableReferenceExpression reference, Map<VariableReferenceExpression, ColumnHandle> assignments)
        {
            ColumnHandle column = assignments.get(reference);
            if (column == null) {
                throw new UnsupportedOperationException("Variable is not a table column: " + reference);
            }
            return column(column, reference.getType().getTypeSignature());
        }

        @Override
        public CanonicalRowExpressionKey visitConstant(ConstantExpression literal, Map<VariableReferenceExpression, ColumnHandle> assignments)
        {
            return constant(literal.getValue(), literal.getType().getTypeSignature());
        }

        @Override
        public CanonicalRowExpressionKey visitCall(CallExpression call, Map<VariableReferenceExpression, ColumnHandle> assignments)
        {
            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(call.getFunctionHandle());
            if (!functionMetadata.isDeterministic()) {
                throw new UnsupportedOperationException("Non-deterministic function is not supported: " + functionMetadata.getName());
            }

            ImmutableList.Builder<CanonicalRowExpressionKey> arguments = ImmutableList.builder();
            for (RowExpression argument : call.getArguments()) {
                arguments.add(argument.accept(this, assignments));
            }
            return call(
                    functionMetadata.getName(),
                    call.getType().getTypeSignature(),
                    arguments.build());
        }

        @Override
        public CanonicalRowExpressionKey visitSpecialForm(SpecialFormExpression specialForm, Map<VariableReferenceExpression, ColumnHandle> assignments)
        {
            ImmutableList.Builder<CanonicalRowExpressionKey> arguments = ImmutableList.builder();
            for (RowExpression argument : specialForm.getArguments()) {
                arguments.add(argument.accept(this, assignments));
            }
            return specialForm(
                    specialForm.getForm(),
                    specialForm.getType().getTypeSignature(),
                    arguments.build());
        }

        @Override
        public CanonicalRowExpressionKey visitInputReference(InputReferenceExpression reference, Map<VariableReferenceExpression, ColumnHandle> assignments)
        {
            throw new UnsupportedOperationException("Input reference is not supported");
        }

        @Override
        public CanonicalRowExpressionKey visitLambda(LambdaDefinitionExpression lambda, Map<VariableReferenceExpression, ColumnHandle> assignments)
        {
            throw new UnsupportedOperationException("Lambda is not supported");
        }
    }
}
