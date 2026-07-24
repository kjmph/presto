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

package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.VariablesExtractor;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.errorprone.annotations.Immutable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class GroupedScalarFilterNode
        extends InternalPlanNode
{
    private final PlanNode source;
    private final VariableReferenceExpression groupIdVariable;
    private final long groupedGroupId;
    private final long scalarGroupId;
    private final VariableReferenceExpression scalarValueVariable;
    private final VariableReferenceExpression scalarVariable;
    private final RowExpression predicate;

    @JsonCreator
    public GroupedScalarFilterNode(
            Optional<SourceLocation> sourceLocation,
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("groupIdVariable") VariableReferenceExpression groupIdVariable,
            @JsonProperty("groupedGroupId") long groupedGroupId,
            @JsonProperty("scalarGroupId") long scalarGroupId,
            @JsonProperty("scalarValueVariable") VariableReferenceExpression scalarValueVariable,
            @JsonProperty("scalarVariable") VariableReferenceExpression scalarVariable,
            @JsonProperty("predicate") RowExpression predicate)
    {
        this(
                sourceLocation,
                id,
                Optional.empty(),
                source,
                groupIdVariable,
                groupedGroupId,
                scalarGroupId,
                scalarValueVariable,
                scalarVariable,
                predicate);
    }

    public GroupedScalarFilterNode(
            Optional<SourceLocation> sourceLocation,
            PlanNodeId id,
            Optional<PlanNode> statsEquivalentPlanNode,
            PlanNode source,
            VariableReferenceExpression groupIdVariable,
            long groupedGroupId,
            long scalarGroupId,
            VariableReferenceExpression scalarValueVariable,
            VariableReferenceExpression scalarVariable,
            RowExpression predicate)
    {
        super(sourceLocation, id, statsEquivalentPlanNode);
        this.source = requireNonNull(source, "source is null");
        this.groupIdVariable = requireNonNull(groupIdVariable, "groupIdVariable is null");
        this.groupedGroupId = groupedGroupId;
        this.scalarGroupId = scalarGroupId;
        this.scalarValueVariable = requireNonNull(scalarValueVariable, "scalarValueVariable is null");
        this.scalarVariable = requireNonNull(scalarVariable, "scalarVariable is null");
        this.predicate = requireNonNull(predicate, "predicate is null");

        Set<VariableReferenceExpression> available = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(source.getOutputVariables())
                .add(scalarVariable)
                .build();
        checkArgument(
                available.containsAll(VariablesExtractor.extractUnique(predicate)),
                "predicate uses variables not produced by source or scalar variable");
        checkArgument(
                source.getOutputVariables().contains(groupIdVariable),
                "source does not contain groupIdVariable");
        checkArgument(
                source.getOutputVariables().contains(scalarValueVariable),
                "source does not contain scalarValueVariable");
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public VariableReferenceExpression getGroupIdVariable()
    {
        return groupIdVariable;
    }

    @JsonProperty
    public long getGroupedGroupId()
    {
        return groupedGroupId;
    }

    @JsonProperty
    public long getScalarGroupId()
    {
        return scalarGroupId;
    }

    @JsonProperty
    public VariableReferenceExpression getScalarValueVariable()
    {
        return scalarValueVariable;
    }

    @JsonProperty
    public VariableReferenceExpression getScalarVariable()
    {
        return scalarVariable;
    }

    @JsonProperty
    public RowExpression getPredicate()
    {
        return predicate;
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return source.getOutputVariables();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public <R, C> R accept(InternalPlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitGroupedScalarFilter(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 1, "expected one new child");
        return new GroupedScalarFilterNode(
                getSourceLocation(),
                getId(),
                getStatsEquivalentPlanNode(),
                newChildren.get(0),
                groupIdVariable,
                groupedGroupId,
                scalarGroupId,
                scalarValueVariable,
                scalarVariable,
                predicate);
    }

    @Override
    public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
    {
        return new GroupedScalarFilterNode(
                getSourceLocation(),
                getId(),
                statsEquivalentPlanNode,
                source,
                groupIdVariable,
                groupedGroupId,
                scalarGroupId,
                scalarValueVariable,
                scalarVariable,
                predicate);
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
        GroupedScalarFilterNode that = (GroupedScalarFilterNode) o;
        return groupedGroupId == that.groupedGroupId &&
                scalarGroupId == that.scalarGroupId &&
                Objects.equals(source, that.source) &&
                Objects.equals(groupIdVariable, that.groupIdVariable) &&
                Objects.equals(scalarValueVariable, that.scalarValueVariable) &&
                Objects.equals(scalarVariable, that.scalarVariable) &&
                Objects.equals(predicate, that.predicate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(source, groupIdVariable, groupedGroupId, scalarGroupId, scalarValueVariable, scalarVariable, predicate);
    }
}
