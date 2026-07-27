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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.REMOTE;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

/**
 * Utility for pushing a deterministic projection through an inner join.
 */
final class PushProjectionThroughJoin
{
    private PushProjectionThroughJoin() {}

    public static Optional<JoinNode> pushProjectionThroughJoin(
            ProjectNode project,
            Lookup lookup,
            PlanNodeIdAllocator idAllocator,
            DeterminismEvaluator determinismEvaluator)
    {
        if (project.getLocality() == REMOTE ||
                !project.getAssignments().getExpressions().stream().allMatch(determinismEvaluator::isDeterministic)) {
            return Optional.empty();
        }

        PlanNode source = lookup.resolve(project.getSource());
        if (!(source instanceof JoinNode) || ((JoinNode) source).getType() != INNER) {
            return Optional.empty();
        }

        JoinNode join = (JoinNode) source;
        PlanNode left = join.getLeft();
        PlanNode right = join.getRight();
        Set<VariableReferenceExpression> leftVariables = ImmutableSet.copyOf(left.getOutputVariables());
        Set<VariableReferenceExpression> rightVariables = ImmutableSet.copyOf(right.getOutputVariables());

        Assignments.Builder leftAssignments = Assignments.builder();
        Assignments.Builder rightAssignments = Assignments.builder();
        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : project.getAssignments().entrySet()) {
            Set<VariableReferenceExpression> inputs = VariablesExtractor.extractUnique(assignment.getValue());
            if (leftVariables.containsAll(inputs)) {
                leftAssignments.put(assignment);
            }
            else if (rightVariables.containsAll(inputs)) {
                rightAssignments.put(assignment);
            }
            else {
                return Optional.empty();
            }
        }

        for (VariableReferenceExpression variable : getJoinRequiredVariables(join)) {
            RowExpression projectedExpression = project.getAssignments().get(variable);
            if (projectedExpression != null && !projectedExpression.equals(variable)) {
                return Optional.empty();
            }
            if (leftVariables.contains(variable)) {
                leftAssignments.put(variable, variable);
            }
            else if (rightVariables.contains(variable)) {
                rightAssignments.put(variable, variable);
            }
            else {
                return Optional.empty();
            }
        }

        Assignments leftProjection = leftAssignments.build();
        Assignments rightProjection = rightAssignments.build();
        Set<VariableReferenceExpression> projectedOutputs = ImmutableSet.copyOf(project.getOutputVariables());
        List<VariableReferenceExpression> leftOutputs = leftProjection.getOutputs().stream()
                .filter(projectedOutputs::contains)
                .collect(ImmutableList.toImmutableList());
        List<VariableReferenceExpression> rightOutputs = rightProjection.getOutputs().stream()
                .filter(projectedOutputs::contains)
                .collect(ImmutableList.toImmutableList());

        return Optional.of(new JoinNode(
                join.getSourceLocation(),
                join.getId(),
                Optional.empty(),
                join.getType(),
                new ProjectNode(project.getSourceLocation(), idAllocator.getNextId(), left, leftProjection, project.getLocality()),
                new ProjectNode(project.getSourceLocation(), idAllocator.getNextId(), right, rightProjection, project.getLocality()),
                join.getCriteria(),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(leftOutputs)
                        .addAll(rightOutputs)
                        .build(),
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

    static Set<VariableReferenceExpression> getJoinRequiredVariables(JoinNode node)
    {
        return Streams.concat(
                        node.getCriteria().stream().map(EquiJoinClause::getLeft),
                        node.getCriteria().stream().map(EquiJoinClause::getRight),
                        node.getFilter().map(VariablesExtractor::extractUnique).orElse(ImmutableSet.of()).stream(),
                        node.getLeftHashVariable().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream(),
                        node.getRightHashVariable().map(ImmutableSet::of).orElse(ImmutableSet.of()).stream(),
                        node.getDynamicFilters().values().stream())
                .collect(toImmutableSet());
    }
}
