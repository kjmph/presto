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
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.semiJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushJoinKeyFilterBelowAggregation
        extends BaseRuleTest
{
    @Test
    public void testPushesJoinKeyFilterBelowFilteredAggregation()
    {
        tester().assertThat(new PushJoinKeyFilterBelowAggregation(getFunctionManager(), new TaskCountEstimator(() -> 4)))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression quantity = p.variable("quantity", DOUBLE);
                    VariableReferenceExpression avgValue = p.variable("avg_value", DOUBLE);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);

                    return p.join(
                            INNER,
                            p.filter(
                                    p.rowExpression("avg_value IS NOT NULL"),
                                    p.aggregation(aggregation -> aggregation
                                            .source(p.values(new PlanNodeId("aggregation_source"), 1_000, leftKey, quantity))
                                            .addAggregation(avgValue, p.rowExpression("avg(quantity)"))
                                            .singleGroupingSet(leftKey))),
                            p.values(new PlanNodeId("filtering_source"), 10, rightKey),
                            ImmutableList.of(new EquiJoinClause(leftKey, rightKey)),
                            ImmutableList.of(leftKey, avgValue, rightKey),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                })
                .overrideStats("aggregation_source", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1_000)
                        .build())
                .overrideStats("filtering_source", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10)
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("left_key", "right_key")),
                        filter(
                                "avg_value IS NOT NULL",
                                aggregation(
                                        singleGroupingSet("left_key"),
                                        ImmutableMap.of(Optional.of("avg_value"), functionCall("avg", ImmutableList.of("quantity"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        filter(semiJoin(
                                                values("left_key", "quantity"),
                                                values(ImmutableMap.of()))))),
                        values("right_key")));
    }

    @Test
    public void testDoesNotPushWhenFilteringSourceIsTooLargeToClone()
    {
        tester().assertThat(new PushJoinKeyFilterBelowAggregation(getFunctionManager(), new TaskCountEstimator(() -> 4)))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "5kB")
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression quantity = p.variable("quantity", DOUBLE);
                    VariableReferenceExpression avgValue = p.variable("avg_value", DOUBLE);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);

                    return p.join(
                            INNER,
                            p.filter(
                                    p.rowExpression("avg_value IS NOT NULL"),
                                    p.aggregation(aggregation -> aggregation
                                            .source(p.values(new PlanNodeId("aggregation_source"), 100_000, leftKey, quantity))
                                            .addAggregation(avgValue, p.rowExpression("avg(quantity)"))
                                            .singleGroupingSet(leftKey))),
                            p.values(new PlanNodeId("filtering_source"), 2_000, rightKey),
                            ImmutableList.of(new EquiJoinClause(leftKey, rightKey)),
                            ImmutableList.of(leftKey, avgValue, rightKey),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty());
                })
                .overrideStats("aggregation_source", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100_000)
                        .build())
                .overrideStats("filtering_source", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(2_000)
                        .build())
                .doesNotFire();
    }
}
