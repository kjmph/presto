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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.PrimaryKeyConstraint;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.google.common.collect.ImmutableSet;

import java.util.Set;
import java.util.stream.IntStream;

import static com.facebook.presto.SystemSessionProperties.isExploitConstraints;
import static com.facebook.presto.sql.planner.plan.Patterns.join;

/**
 * Carries trusted join-key facts through the plan for native execution.
 *
 * Uniqueness facts are derived from LogicalProperties, which in turn only use
 * connector/table constraints that the planner already trusts. Non-null facts
 * are derived conservatively from enabled or RELY NOT NULL / primary-key
 * constraints through identity projections and filters. This rule does not
 * change join semantics or shape; it annotates join keys so downstream engines
 * can expose metrics and pick specialized physical implementations.
 */
public class AnnotateJoinNodeWithUniqueKeys
        implements Rule<JoinNode>
{
    private static final Pattern<JoinNode> PATTERN = join();

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isExploitConstraints(session);
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        if (!isExploitConstraints(context.getSession())) {
            return Result.empty();
        }

        if (joinNode.getCriteria().isEmpty()) {
            return Result.empty();
        }

        boolean leftKeysUnique = areJoinKeysUnique(joinNode.getLeft(), getLeftJoinKeys(joinNode));
        boolean rightKeysUnique = areJoinKeysUnique(joinNode.getRight(), getRightJoinKeys(joinNode));
        boolean leftKeysNonNull = areJoinKeysNonNull(joinNode.getLeft(), getLeftJoinKeys(joinNode), context);
        boolean rightKeysNonNull = areJoinKeysNonNull(joinNode.getRight(), getRightJoinKeys(joinNode), context);

        if (joinNode.isLeftKeysUnique() == leftKeysUnique &&
                joinNode.isRightKeysUnique() == rightKeysUnique &&
                joinNode.isLeftKeysNonNull() == leftKeysNonNull &&
                joinNode.isRightKeysNonNull() == rightKeysNonNull) {
            return Result.empty();
        }

        return Result.ofPlanNode(joinNode.withKeyProperties(leftKeysUnique, rightKeysUnique, leftKeysNonNull, rightKeysNonNull));
    }

    private static boolean areJoinKeysUnique(PlanNode source, Set<VariableReferenceExpression> joinKeys)
    {
        if (joinKeys.isEmpty() || !(source instanceof GroupReference)) {
            return false;
        }

        return ((GroupReference) source).getLogicalProperties()
                .map(logicalProperties -> logicalProperties.isDistinct(joinKeys))
                .orElse(false);
    }

    private static boolean areJoinKeysNonNull(PlanNode source, Set<VariableReferenceExpression> joinKeys, Context context)
    {
        if (joinKeys.isEmpty()) {
            return false;
        }

        return joinKeys.stream().allMatch(joinKey -> isKnownNonNull(source, joinKey, context));
    }

    private static boolean isKnownNonNull(PlanNode source, VariableReferenceExpression variable, Context context)
    {
        PlanNode resolved = context.getLookup().resolve(source);
        if (!resolved.getOutputVariables().contains(variable)) {
            return false;
        }

        if (resolved instanceof FilterNode) {
            return isKnownNonNull(((FilterNode) resolved).getSource(), variable, context);
        }

        if (resolved instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) resolved;
            RowExpression assignment = project.getAssignments().get(variable);
            return assignment instanceof VariableReferenceExpression &&
                    isKnownNonNull(project.getSource(), (VariableReferenceExpression) assignment, context);
        }

        if (resolved instanceof ExchangeNode) {
            return isKnownNonNullThroughExchange((ExchangeNode) resolved, variable, context);
        }

        if (resolved instanceof JoinNode) {
            return isKnownNonNullThroughJoin((JoinNode) resolved, variable, context);
        }

        if (resolved instanceof TableScanNode) {
            TableScanNode tableScan = (TableScanNode) resolved;
            ColumnHandle column = tableScan.getAssignments().get(variable);
            if (column == null) {
                return false;
            }

            return tableScan.getTableConstraints().stream()
                    .filter(constraint -> (constraint instanceof NotNullConstraint || constraint instanceof PrimaryKeyConstraint) &&
                            (constraint.isEnabled() || constraint.isRely()))
                    .anyMatch(constraint -> constraint.getColumns().contains(column));
        }

        return false;
    }

    private static boolean isKnownNonNullThroughExchange(ExchangeNode exchange, VariableReferenceExpression variable, Context context)
    {
        int outputIndex = exchange.getOutputVariables().indexOf(variable);
        if (outputIndex < 0) {
            return false;
        }

        return IntStream.range(0, exchange.getSources().size())
                .allMatch(sourceIndex -> isKnownNonNull(
                        exchange.getSources().get(sourceIndex),
                        exchange.getInputs().get(sourceIndex).get(outputIndex),
                        context));
    }

    private static boolean isKnownNonNullThroughJoin(JoinNode join, VariableReferenceExpression variable, Context context)
    {
        if (join.getLeft().getOutputVariables().contains(variable)) {
            switch (join.getType()) {
                case INNER:
                case LEFT:
                    return isKnownNonNull(join.getLeft(), variable, context);
                default:
                    return false;
            }
        }

        if (join.getRight().getOutputVariables().contains(variable)) {
            switch (join.getType()) {
                case INNER:
                case RIGHT:
                    return isKnownNonNull(join.getRight(), variable, context);
                default:
                    return false;
            }
        }

        return false;
    }

    private static Set<VariableReferenceExpression> getLeftJoinKeys(JoinNode joinNode)
    {
        return joinNode.getCriteria().stream()
                .map(EquiJoinClause::getLeft)
                .collect(ImmutableSet.toImmutableSet());
    }

    private static Set<VariableReferenceExpression> getRightJoinKeys(JoinNode joinNode)
    {
        return joinNode.getCriteria().stream()
                .map(EquiJoinClause::getRight)
                .collect(ImmutableSet.toImmutableSet());
    }
}
