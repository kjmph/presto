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

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.plan.GroupIdNode;
import com.facebook.presto.sql.planner.plan.GroupedScalarFilterNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.SystemSessionProperties.REWRITE_REPEATED_SCALAR_SUM_TO_GROUPED_SCALAR_FILTER;
import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.relational.Expressions.comparisonExpression;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestTransformRepeatedScalarSumToGroupedScalarFilter
        extends BaseRuleTest
{
    @Test
    public void testDisabledByDefault()
    {
        rule()
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(this::repeatedScalarSum)
                .doesNotFire();
    }

    @Test
    public void testRequiresNativeExecution()
    {
        rule()
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .setSystemProperty(REWRITE_REPEATED_SCALAR_SUM_TO_GROUPED_SCALAR_FILTER, "true")
                .on(this::repeatedScalarSum)
                .doesNotFire();
    }

    @Test
    public void testRequiresPushAggregationThroughJoin()
    {
        rule()
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "false")
                .setSystemProperty(REWRITE_REPEATED_SCALAR_SUM_TO_GROUPED_SCALAR_FILTER, "true")
                .on(this::repeatedScalarSum)
                .doesNotFire();
    }

    @Test
    public void testRewritesWhenExplicitlyEnabled()
    {
        PlanVariables variables = new PlanVariables();
        PlanNode transformed = rule()
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, "true")
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .setSystemProperty(REWRITE_REPEATED_SCALAR_SUM_TO_GROUPED_SCALAR_FILTER, "true")
                .on(p -> repeatedScalarSum(p, variables))
                .overrideStats("grouped_source", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .build())
                .get();

        assertTrue(transformed instanceof ProjectNode);
        ProjectNode project = (ProjectNode) transformed;
        assertEquals(
                project.getAssignments(),
                Assignments.of(
                        variables.groupedKey, variables.groupedKey,
                        variables.groupedSum, variables.groupedSum));

        PlanNode projectSource = project.getSource();
        assertTrue(projectSource instanceof GroupedScalarFilterNode);

        GroupedScalarFilterNode groupedScalarFilter = (GroupedScalarFilterNode) projectSource;
        assertEquals(groupedScalarFilter.getGroupedGroupId(), 1L);
        assertEquals(groupedScalarFilter.getScalarGroupId(), 0L);
        assertEquals(groupedScalarFilter.getScalarValueVariable(), variables.groupedSum);
        assertEquals(groupedScalarFilter.getScalarVariable().getType(), variables.groupedSum.getType());

        FunctionResolution functionResolution = new FunctionResolution(getFunctionManager().getFunctionAndTypeResolver());
        RowExpression expectedPredicate = and(
                comparisonExpression(
                        functionResolution,
                        EQUAL,
                        groupedScalarFilter.getGroupIdVariable(),
                        new ConstantExpression(1L, BIGINT)),
                comparisonExpression(
                        functionResolution,
                        GREATER_THAN,
                        variables.groupedSum,
                        groupedScalarFilter.getScalarVariable()));
        assertEquals(groupedScalarFilter.getPredicate(), expectedPredicate);

        assertTrue(groupedScalarFilter.getSource() instanceof AggregationNode);
        AggregationNode aggregation = (AggregationNode) groupedScalarFilter.getSource();
        assertEquals(aggregation.getStep(), SINGLE);
        assertEquals(aggregation.getGroupingKeys(), ImmutableList.of(variables.groupedKey));
        assertEquals(
                aggregation.getAggregations().keySet(),
                ImmutableSet.of(variables.groupedSum, groupedScalarFilter.getGroupIdVariable()));
        assertEquals(
                aggregation.getAggregations().get(variables.groupedSum).getArguments(),
                ImmutableList.of(variables.groupedValue));
        assertEquals(aggregation.getAggregations().get(variables.groupedSum).getCall().getDisplayName(), "sum");

        assertTrue(aggregation.getSource() instanceof GroupIdNode);
        GroupIdNode groupId = (GroupIdNode) aggregation.getSource();
        assertEquals(
                aggregation.getAggregations().get(groupedScalarFilter.getGroupIdVariable()).getArguments(),
                ImmutableList.of(groupId.getGroupIdVariable()));
        assertEquals(
                aggregation.getAggregations().get(groupedScalarFilter.getGroupIdVariable()).getCall().getDisplayName(),
                "max");
        assertEquals(
                groupId.getGroupingSets(),
                ImmutableList.of(ImmutableList.of(), ImmutableList.of(variables.groupedKey)));
        assertEquals(
                groupId.getGroupingColumns(),
                ImmutableMap.of(variables.groupedKey, variables.groupedKey));
        assertEquals(groupId.getAggregationArguments(), ImmutableList.of(variables.groupedValue));
    }

    private RuleAssert rule()
    {
        return tester().assertThat(new TransformRepeatedScalarSumToGroupedScalarFilter(
                tester().getMetadata().getFunctionAndTypeManager()));
    }

    private PlanNode repeatedScalarSum(PlanBuilder p)
    {
        return repeatedScalarSum(p, new PlanVariables());
    }

    private PlanNode repeatedScalarSum(PlanBuilder p, PlanVariables variables)
    {
        TestingColumnHandle keyColumn = new TestingColumnHandle("key");
        TestingColumnHandle valueColumn = new TestingColumnHandle("value");

        variables.groupedKey = p.variable("grouped_key", BIGINT);
        variables.groupedValue = p.variable("grouped_value", BIGINT);
        variables.groupedSum = p.variable("grouped_sum", BIGINT);
        TableScanNode groupedScan = p.tableScan(
                ImmutableList.of(variables.groupedKey, variables.groupedValue),
                ImmutableMap.of(
                        variables.groupedKey, keyColumn,
                        variables.groupedValue, valueColumn));
        PlanNode groupedSource = p.filter(
                new PlanNodeId("grouped_source"),
                p.rowExpression("grouped_key IS NOT NULL"),
                groupedScan);
        AggregationNode grouped = p.aggregation(aggregation -> aggregation
                .source(groupedSource)
                .addAggregation(variables.groupedSum, p.rowExpression("sum(grouped_value)"))
                .singleGroupingSet(variables.groupedKey));

        VariableReferenceExpression scalarKey = p.variable("scalar_key", BIGINT);
        VariableReferenceExpression scalarValue = p.variable("scalar_value", BIGINT);
        VariableReferenceExpression scalarSum = p.variable("scalar_sum", BIGINT);
        TableScanNode scalarScan = p.tableScan(
                groupedScan.getTable(),
                ImmutableList.of(scalarKey, scalarValue),
                ImmutableMap.of(
                        scalarKey, keyColumn,
                        scalarValue, valueColumn));
        PlanNode scalarSource = p.filter(
                p.rowExpression("scalar_key IS NOT NULL"),
                scalarScan);
        AggregationNode scalar = p.aggregation(aggregation -> aggregation
                .source(scalarSource)
                .addAggregation(scalarSum, p.rowExpression("sum(scalar_value)"))
                .globalGrouping());

        PlanNode filtered = p.filter(
                p.rowExpression("grouped_sum > scalar_sum"),
                p.join(INNER, grouped, scalar));
        return p.project(
                filtered,
                Assignments.of(
                        variables.groupedKey, variables.groupedKey,
                        variables.groupedSum, variables.groupedSum));
    }

    private static class PlanVariables
    {
        private VariableReferenceExpression groupedKey;
        private VariableReferenceExpression groupedValue;
        private VariableReferenceExpression groupedSum;
    }
}
