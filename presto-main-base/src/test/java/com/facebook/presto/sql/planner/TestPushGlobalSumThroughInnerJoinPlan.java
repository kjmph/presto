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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestPushGlobalSumThroughInnerJoinPlan
        extends BasePlanTest
{
    @Test
    public void testPushesGlobalSumInFullPlan()
    {
        Session session = Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .build();

        assertPlanWithSession(
                "SELECT sum(o.totalprice) " +
                        "FROM orders o JOIN customer c ON o.custkey = c.custkey",
                session,
                true,
                anyTree(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("O_CUSTKEY", "C_CUSTKEY")),
                        project(aggregation(
                                singleGroupingSet("O_CUSTKEY"),
                                ImmutableMap.of(Optional.of("PRE_SUM"), functionCall("sum", ImmutableList.of("PARTIAL_SUM"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                FINAL,
                                exchange(aggregation(
                                        singleGroupingSet("O_CUSTKEY"),
                                        ImmutableMap.of(Optional.of("PARTIAL_SUM"), functionCall("sum", ImmutableList.of("O_TOTALPRICE"))),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        PARTIAL,
                                        tableScan("orders", ImmutableMap.of(
                                                "O_CUSTKEY", "custkey",
                                                "O_TOTALPRICE", "totalprice")))))),
                        anyTree(tableScan("customer", ImmutableMap.of("C_CUSTKEY", "custkey"))))));
    }
}
