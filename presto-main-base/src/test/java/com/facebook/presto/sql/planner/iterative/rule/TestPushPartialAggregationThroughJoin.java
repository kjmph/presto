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
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.joinWithKeyProperties;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;

public class TestPushPartialAggregationThroughJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesPartialAggregationThroughJoin()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(
                                p.join(
                                        INNER,
                                        p.values(p.variable("LEFT_EQUI"), p.variable("LEFT_NON_EQUI"), p.variable("LEFT_GROUP_BY"), p.variable("LEFT_AGGR"), p.variable("LEFT_HASH")),
                                        p.values(p.variable("RIGHT_EQUI"), p.variable("RIGHT_NON_EQUI"), p.variable("RIGHT_GROUP_BY"), p.variable("RIGHT_HASH")),
                                        ImmutableList.of(new EquiJoinClause(p.variable("LEFT_EQUI"), p.variable("RIGHT_EQUI"))),
                                        ImmutableList.of(p.variable("LEFT_GROUP_BY"), p.variable("LEFT_AGGR"), p.variable("RIGHT_GROUP_BY")),
                                        Optional.of(p.rowExpression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")),
                                        Optional.of(p.variable("LEFT_HASH")),
                                        Optional.of(p.variable("RIGHT_HASH"))))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(LEFT_AGGR)"))
                        .singleGroupingSet(p.variable("LEFT_GROUP_BY"), p.variable("RIGHT_GROUP_BY"))
                        .step(PARTIAL)))
                .matches(project(ImmutableMap.of(
                        "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                        "RIGHT_GROUP_BY", PlanMatchPattern.expression("RIGHT_GROUP_BY"),
                        "AVG", PlanMatchPattern.expression("AVG")),
                        join(INNER, ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI <= RIGHT_NON_EQUI"),
                                aggregation(
                                        singleGroupingSet("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_HASH"),
                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("LEFT_AGGR"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR", "LEFT_HASH")),
                                values("RIGHT_EQUI", "RIGHT_NON_EQUI", "RIGHT_GROUP_BY", "RIGHT_HASH"))));
    }

    @Test
    public void testPushesPartialAggregationThroughProjectionAndJoin()
    {
        PushPartialAggregationThroughJoin rule = new PushPartialAggregationThroughJoin(getFunctionManager());
        tester().assertThat(rule.pushPartialAggregationThroughJoinWithProjection())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftEqui = p.variable("LEFT_EQUI");
                    VariableReferenceExpression leftNonEqui = p.variable("LEFT_NON_EQUI");
                    VariableReferenceExpression leftGroupBy = p.variable("LEFT_GROUP_BY");
                    VariableReferenceExpression leftAggregation = p.variable("LEFT_AGGR");
                    VariableReferenceExpression leftAggregationProjected = p.variable("LEFT_AGGR_PROJECTED");
                    VariableReferenceExpression rightEqui = p.variable("RIGHT_EQUI");
                    VariableReferenceExpression rightNonEqui = p.variable("RIGHT_NON_EQUI");
                    Assignments assignments = Assignments.builder()
                            .put(leftAggregationProjected, p.rowExpression("LEFT_AGGR + LEFT_AGGR"))
                            .put(leftGroupBy, leftGroupBy)
                            .put(leftEqui, leftEqui)
                            .put(leftNonEqui, leftNonEqui)
                            .build();
                    return p.aggregation(ab -> ab
                            .source(p.project(
                                    assignments,
                                    p.join(
                                            INNER,
                                            p.values(leftEqui, leftNonEqui, leftGroupBy, leftAggregation),
                                            p.values(rightEqui, rightNonEqui),
                                            ImmutableList.of(new EquiJoinClause(leftEqui, rightEqui)),
                                            ImmutableList.of(leftEqui, leftNonEqui, leftGroupBy, leftAggregation),
                                            Optional.of(p.rowExpression("LEFT_NON_EQUI <= RIGHT_NON_EQUI")))
                                            .withDistributionType(PARTITIONED)))
                            .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(LEFT_AGGR_PROJECTED)"))
                            .singleGroupingSet(leftGroupBy)
                            .step(PARTIAL));
                })
                .matches(project(ImmutableMap.of(
                                "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                                "AVG", PlanMatchPattern.expression("AVG")),
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("LEFT_EQUI", "RIGHT_EQUI")),
                                Optional.of("LEFT_NON_EQUI <= RIGHT_NON_EQUI"),
                                Optional.of(PARTITIONED),
                                aggregation(
                                        singleGroupingSet("LEFT_GROUP_BY", "LEFT_EQUI", "LEFT_NON_EQUI"),
                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("LEFT_AGGR_PROJECTED"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        project(
                                                ImmutableMap.of(
                                                        "LEFT_AGGR_PROJECTED", PlanMatchPattern.expression("LEFT_AGGR + LEFT_AGGR"),
                                                        "LEFT_GROUP_BY", PlanMatchPattern.expression("LEFT_GROUP_BY"),
                                                        "LEFT_EQUI", PlanMatchPattern.expression("LEFT_EQUI"),
                                                        "LEFT_NON_EQUI", PlanMatchPattern.expression("LEFT_NON_EQUI")),
                                                values("LEFT_EQUI", "LEFT_NON_EQUI", "LEFT_GROUP_BY", "LEFT_AGGR"))),
                                project(
                                        ImmutableMap.of(
                                                "RIGHT_EQUI", PlanMatchPattern.expression("RIGHT_EQUI"),
                                                "RIGHT_NON_EQUI", PlanMatchPattern.expression("RIGHT_NON_EQUI")),
                                        values("RIGHT_EQUI", "RIGHT_NON_EQUI")))));
    }

    @Test
    public void testDoesNotPushPartialAggregationThroughProjectionAndReplicatedJoin()
    {
        assertProjectedJoinDistributionDoesNotFire(Optional.of(REPLICATED));
    }

    @Test
    public void testDoesNotPushPartialAggregationThroughProjectionAndJoinWithoutDistribution()
    {
        assertProjectedJoinDistributionDoesNotFire(Optional.empty());
    }

    @Test
    public void testPreservesJoinKeyProperties()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                        INNER,
                                        p.values(p.variable("LEFT_KEY"), p.variable("LEFT_VALUE")),
                                        p.values(p.variable("RIGHT_KEY")),
                                        new EquiJoinClause(p.variable("LEFT_KEY"), p.variable("RIGHT_KEY")))
                                .withKeyProperties(false, true, true, true, false, true))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(LEFT_VALUE)"))
                        .singleGroupingSet(p.variable("LEFT_KEY"))
                        .step(PARTIAL)))
                .matches(project(
                        joinWithKeyProperties(
                                INNER,
                                ImmutableList.of(equiJoinClause("LEFT_KEY", "RIGHT_KEY")),
                                false,
                                true,
                                true,
                                true,
                                false,
                                true,
                                aggregation(
                                        singleGroupingSet("LEFT_KEY"),
                                        ImmutableMap.of(Optional.of("AVG"), functionCall("avg", ImmutableList.of("LEFT_VALUE"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        values("LEFT_KEY", "LEFT_VALUE")),
                                values("RIGHT_KEY"))));
    }

    @Test
    public void testDoesNotPushCrossSideProjection()
    {
        PushPartialAggregationThroughJoin rule = new PushPartialAggregationThroughJoin(getFunctionManager());
        tester().assertThat(rule.pushPartialAggregationThroughJoinWithProjection())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftEqui = p.variable("LEFT_EQUI");
                    VariableReferenceExpression leftAggregation = p.variable("LEFT_AGGR");
                    VariableReferenceExpression rightEqui = p.variable("RIGHT_EQUI");
                    VariableReferenceExpression rightAggregation = p.variable("RIGHT_AGGR");
                    VariableReferenceExpression projected = p.variable("PROJECTED");
                    return p.aggregation(ab -> ab
                            .source(p.project(
                                    Assignments.of(
                                            projected,
                                            p.rowExpression("LEFT_AGGR + RIGHT_AGGR")),
                                    p.join(
                                            INNER,
                                            p.values(leftEqui, leftAggregation),
                                            p.values(rightEqui, rightAggregation),
                                            new EquiJoinClause(leftEqui, rightEqui))
                                            .withDistributionType(PARTITIONED)))
                            .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(PROJECTED)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushNondeterministicProjection()
    {
        PushPartialAggregationThroughJoin rule = new PushPartialAggregationThroughJoin(getFunctionManager());
        tester().assertThat(rule.pushPartialAggregationThroughJoinWithProjection())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(p.project(
                                Assignments.of(
                                        p.variable("PROJECTED", DOUBLE),
                                        p.rowExpression("random()")),
                                p.join(
                                        INNER,
                                        p.values(p.variable("LEFT_EQUI")),
                                        p.values(p.variable("RIGHT_EQUI")),
                                        new EquiJoinClause(p.variable("LEFT_EQUI"), p.variable("RIGHT_EQUI")))
                                        .withDistributionType(PARTITIONED)))
                        .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(PROJECTED)"))
                        .globalGrouping()
                        .step(PARTIAL)))
                .doesNotFire();
    }

    @Test
    public void testDoesNotPushAggregationWithMaskFromOtherJoinSide()
    {
        tester().assertThat(new PushPartialAggregationThroughJoin(getFunctionManager()))
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(ab -> ab
                        .source(p.join(
                                INNER,
                                p.values(p.variable("LEFT_KEY"), p.variable("LEFT_VALUE")),
                                p.values(p.variable("RIGHT_KEY"), p.variable("RIGHT_MASK", BOOLEAN)),
                                new EquiJoinClause(p.variable("LEFT_KEY"), p.variable("RIGHT_KEY"))))
                        .addAggregation(
                                p.variable("AVG", DOUBLE),
                                p.rowExpression("AVG(LEFT_VALUE)"),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                Optional.of(p.variable("RIGHT_MASK", BOOLEAN)))
                        .globalGrouping()
                        .step(PARTIAL)))
                .doesNotFire();
    }

    private void assertProjectedJoinDistributionDoesNotFire(Optional<JoinDistributionType> distributionType)
    {
        PushPartialAggregationThroughJoin rule = new PushPartialAggregationThroughJoin(getFunctionManager());
        tester().assertThat(rule.pushPartialAggregationThroughJoinWithProjection())
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("LEFT_KEY");
                    VariableReferenceExpression leftValue = p.variable("LEFT_VALUE");
                    VariableReferenceExpression projectedValue = p.variable("PROJECTED_VALUE");
                    VariableReferenceExpression rightKey = p.variable("RIGHT_KEY");
                    return p.aggregation(ab -> ab
                            .source(p.project(
                                    Assignments.of(
                                            projectedValue,
                                            p.rowExpression("LEFT_VALUE + LEFT_VALUE")),
                                    p.join(
                                            INNER,
                                            p.values(leftKey, leftValue),
                                            p.values(rightKey),
                                            ImmutableList.of(new EquiJoinClause(leftKey, rightKey)),
                                            ImmutableList.of(leftValue),
                                            Optional.empty(),
                                            Optional.empty(),
                                            Optional.empty(),
                                            distributionType,
                                            ImmutableMap.of())))
                            .addAggregation(p.variable("AVG", DOUBLE), p.rowExpression("AVG(PROJECTED_VALUE)"))
                            .globalGrouping()
                            .step(PARTIAL));
                })
                .doesNotFire();
    }
}
