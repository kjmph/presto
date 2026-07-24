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
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.globalAggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushGlobalSumThroughInnerJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesProjectedSumBelowInnerJoin()
    {
        tester().assertThat(new PushGlobalSumThroughInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key");
                    VariableReferenceExpression leftValue = p.variable("left_value", DOUBLE);
                    VariableReferenceExpression leftScale = p.variable("left_scale", DOUBLE);
                    VariableReferenceExpression rightKey = p.variable("right_key");
                    VariableReferenceExpression rightPayload = p.variable("right_payload");
                    VariableReferenceExpression projected = p.variable("projected", DOUBLE);
                    VariableReferenceExpression sum = p.variable("sum", DOUBLE);

                    return p.aggregation(aggregation -> aggregation
                            .globalGrouping()
                            .addAggregation(sum, p.rowExpression("sum(projected)"))
                            .source(p.project(
                                    p.join(
                                            INNER,
                                            p.values(leftKey, leftValue, leftScale),
                                            p.values(rightKey, rightPayload),
                                            ImmutableList.of(new EquiJoinClause(leftKey, rightKey)),
                                            ImmutableList.of(leftKey, leftValue, leftScale, rightKey, rightPayload),
                                            Optional.empty()),
                                    Assignments.builder()
                                            .put(projected, p.rowExpression("left_value * left_scale"))
                                            .build())));
                })
                .matches(aggregation(
                        globalAggregation(),
                        ImmutableMap.of(Optional.of("SUM"), functionCall("sum", ImmutableList.of("PRE_SUM"))),
                        ImmutableMap.of(),
                        Optional.empty(),
                        SINGLE,
                        join(INNER,
                                ImmutableList.of(equiJoinClause("left_key", "right_key")),
                                Optional.empty(),
                                aggregation(
                                        singleGroupingSet("left_key"),
                                        ImmutableMap.of(Optional.of("PRE_SUM"), functionCall("sum", ImmutableList.of("projected"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        project(ImmutableMap.of(
                                                        "left_key", expression("left_key"),
                                                        "projected", expression("left_value * left_scale")),
                                                values("left_key", "left_value", "left_scale"))),
                                values("right_key", "right_payload"))));
    }

    @Test
    public void testDoesNotFireWhenExpressionDependsOnBothSides()
    {
        tester().assertThat(new PushGlobalSumThroughInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key");
                    VariableReferenceExpression leftValue = p.variable("left_value", DOUBLE);
                    VariableReferenceExpression rightKey = p.variable("right_key");
                    VariableReferenceExpression rightValue = p.variable("right_value", DOUBLE);
                    VariableReferenceExpression projected = p.variable("projected", DOUBLE);
                    VariableReferenceExpression sum = p.variable("sum", DOUBLE);

                    return p.aggregation(aggregation -> aggregation
                            .globalGrouping()
                            .addAggregation(sum, p.rowExpression("sum(projected)"))
                            .source(p.project(
                                    p.join(
                                            INNER,
                                            p.values(leftKey, leftValue),
                                            p.values(rightKey, rightValue),
                                            ImmutableList.of(new EquiJoinClause(leftKey, rightKey)),
                                            ImmutableList.of(leftKey, leftValue, rightKey, rightValue),
                                            Optional.empty()),
                                    Assignments.builder()
                                            .put(projected, p.rowExpression("left_value * right_value"))
                                            .build())));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithJoinFilter()
    {
        tester().assertThat(new PushGlobalSumThroughInnerJoin(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key");
                    VariableReferenceExpression leftValue = p.variable("left_value", DOUBLE);
                    VariableReferenceExpression rightKey = p.variable("right_key");
                    VariableReferenceExpression rightValue = p.variable("right_value", DOUBLE);
                    VariableReferenceExpression sum = p.variable("sum", DOUBLE);

                    return p.aggregation(aggregation -> aggregation
                            .globalGrouping()
                            .addAggregation(sum, p.rowExpression("sum(left_value)"))
                            .source(p.join(
                                    INNER,
                                    p.values(leftKey, leftValue),
                                    p.values(rightKey, rightValue),
                                    ImmutableList.of(new EquiJoinClause(leftKey, rightKey)),
                                    ImmutableList.of(leftKey, leftValue, rightKey, rightValue),
                                    Optional.of(p.rowExpression("left_value > right_value")),
                                    Optional.empty(),
                                    Optional.empty())));
                })
                .doesNotFire();
    }
}
