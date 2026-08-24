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
package com.facebook.presto.sql.query;

import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.planner.Plan;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.SystemSessionProperties.USE_DEFAULTS_FOR_CORRELATED_AGGREGATION_PUSHDOWN_THROUGH_OUTER_JOINS;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.textLogicalPlan;
import static org.testng.Assert.assertTrue;

public class TestPushAggregationThroughOuterJoinSemantics
{
    private static final ImmutableMap<String, String> ENABLED_SESSION_PROPERTIES = ImmutableMap.of(
            PUSH_AGGREGATION_THROUGH_JOIN, "true",
            USE_DEFAULTS_FOR_CORRELATED_AGGREGATION_PUSHDOWN_THROUGH_OUTER_JOINS, "false");
    private static final ImmutableMap<String, String> DEFAULTS_ENABLED_SESSION_PROPERTIES = ImmutableMap.of(
            PUSH_AGGREGATION_THROUGH_JOIN, "true",
            USE_DEFAULTS_FOR_CORRELATED_AGGREGATION_PUSHDOWN_THROUGH_OUTER_JOINS, "true");

    @Test
    public void testArrayAggReconstructsUnmatchedRow()
    {
        try (QueryAssertions assertions = new QueryAssertions(ENABLED_SESSION_PROPERTIES)) {
            assertQueryAndPushedOuterJoin(
                    assertions,
                    "WITH outer_relation(key) AS (" +
                            "    SELECT key FROM (VALUES BIGINT '1', BIGINT '2') AS t(key) GROUP BY key) " +
                            "SELECT outer_relation.key, array_agg(value) " +
                            "FROM outer_relation " +
                            "LEFT JOIN (VALUES (BIGINT '1', CAST('x' AS VARCHAR))) AS inner_relation(key, value) " +
                            "    ON outer_relation.key = inner_relation.key " +
                            "GROUP BY outer_relation.key",
                    "VALUES " +
                            "    (BIGINT '1', ARRAY['x']), " +
                            "    (BIGINT '2', ARRAY[CAST(NULL AS VARCHAR)])",
                    LEFT);
        }
    }

    @Test
    public void testCountReturnsZeroForUnmatchedRow()
    {
        try (QueryAssertions assertions = new QueryAssertions(ENABLED_SESSION_PROPERTIES)) {
            assertQueryAndPushedOuterJoin(
                    assertions,
                    "SELECT outer_relation.key, count(value) " +
                            "FROM (VALUES BIGINT '2') AS outer_relation(key) " +
                            "LEFT JOIN (VALUES (BIGINT '1', CAST('x' AS VARCHAR))) AS inner_relation(key, value) " +
                            "    ON outer_relation.key = inner_relation.key " +
                            "GROUP BY outer_relation.key",
                    "VALUES (BIGINT '2', BIGINT '0')",
                    LEFT);
        }
    }

    @Test
    public void testBuiltInNullReturningAggregationsPreserveMatchedAndUnmatchedRows()
    {
        try (QueryAssertions assertions = new QueryAssertions(DEFAULTS_ENABLED_SESSION_PROPERTIES)) {
            assertQueryAndPushedOuterJoin(
                    assertions,
                    "WITH outer_relation(key) AS (" +
                            "    SELECT key FROM (VALUES BIGINT '1', BIGINT '2') AS t(key) GROUP BY key) " +
                            "SELECT outer_relation.key, min(value), max(value), sum(value), avg(value), count(value) " +
                            "FROM outer_relation " +
                            "LEFT JOIN (VALUES (BIGINT '1', BIGINT '10'), (BIGINT '1', BIGINT '20')) AS inner_relation(key, value) " +
                            "    ON outer_relation.key = inner_relation.key " +
                            "GROUP BY outer_relation.key",
                    "VALUES " +
                            "    (BIGINT '1', BIGINT '10', BIGINT '20', BIGINT '30', DOUBLE '15.0', BIGINT '2'), " +
                            "    (BIGINT '2', CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS BIGINT), CAST(NULL AS DOUBLE), BIGINT '0')",
                    LEFT);
        }
    }

    private static void assertQueryAndPushedOuterJoin(QueryAssertions assertions, String actual, String expected, JoinType joinType)
    {
        assertions.assertQuery(actual, expected);

        Plan plan = assertions.getQueryRunner().createPlan(
                assertions.getQueryRunner().getDefaultSession(),
                actual,
                WarningCollector.NOOP);
        assertTrue(
                containsPushedAggregationThroughOuterJoin(plan.getRoot(), joinType),
                "Expected plan to contain an aggregation pushed through a " + joinType + " join:\n" +
                        textLogicalPlan(
                                plan.getRoot(),
                                plan.getTypes(),
                                StatsAndCosts.empty(),
                                assertions.getQueryRunner().getMetadata().getFunctionAndTypeManager(),
                                assertions.getQueryRunner().getDefaultSession(),
                                0));
    }

    private static boolean containsPushedAggregationThroughOuterJoin(PlanNode node, JoinType joinType)
    {
        if (node instanceof JoinNode) {
            JoinNode join = (JoinNode) node;
            if (join.getType() == joinType) {
                PlanNode nullProducingSide = joinType == LEFT ? join.getRight() : join.getLeft();
                if (containsNonPartialAggregation(nullProducingSide)) {
                    return true;
                }
            }
        }

        return node.getSources().stream()
                .anyMatch(source -> containsPushedAggregationThroughOuterJoin(source, joinType));
    }

    private static boolean containsNonPartialAggregation(PlanNode node)
    {
        if (node instanceof AggregationNode && ((AggregationNode) node).getStep() != AggregationNode.Step.PARTIAL) {
            return true;
        }

        return node.getSources().stream()
                .anyMatch(TestPushAggregationThroughOuterJoinSemantics::containsNonPartialAggregation);
    }
}
