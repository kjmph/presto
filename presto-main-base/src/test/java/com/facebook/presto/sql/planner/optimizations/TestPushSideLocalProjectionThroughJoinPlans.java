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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.SystemSessionProperties.PUSH_SIDE_LOCAL_PROJECTION_THROUGH_JOIN;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy.NONE;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestPushSideLocalProjectionThroughJoinPlans
        extends BasePlanTest
{
    private static final String Q14 =
            "SELECT 100.00 * sum(CASE WHEN p.type LIKE 'PROMO%' THEN l.extendedprice * (1 - l.discount) ELSE 0 END) / " +
                    "sum(l.extendedprice * (1 - l.discount)) " +
                    "FROM lineitem l JOIN part p ON l.partkey = p.partkey " +
                    "WHERE l.shipdate >= DATE '1995-09-01' " +
                    "AND l.shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH";

    @Test
    public void testQ14ComputesNarrowPayloadBeforeRemoteRepartition()
    {
        PlanMatchPattern lineitemExchange = exchange(
                REMOTE_STREAMING,
                REPARTITION,
                project(
                        ImmutableMap.of("REVENUE", expression("EXTENDED_PRICE * (1E0 - DISCOUNT)")),
                        anyTree(tableScan(
                                "lineitem",
                                ImmutableMap.of(
                                        "LINE_PARTKEY", "partkey",
                                        "EXTENDED_PRICE", "extendedprice",
                                        "DISCOUNT", "discount")))))
                .withExactOutputs("LINE_PARTKEY", "REVENUE");
        PlanMatchPattern partExchange = exchange(
                REMOTE_STREAMING,
                REPARTITION,
                project(
                        ImmutableMap.of("IS_PROMO", expression("substr(PART_TYPE, 1, 5) = 'PROMO'")),
                        tableScan(
                                "part",
                                ImmutableMap.of(
                                        "PART_PARTKEY", "partkey",
                                        "PART_TYPE", "type"))))
                .withExactOutputs("PART_PARTKEY", "IS_PROMO");

        assertDistributedPlan(
                Q14,
                q14Session(true),
                anyTree(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("LINE_PARTKEY", "PART_PARTKEY")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        lineitemExchange,
                        anyTree(partExchange))));
    }

    @Test
    public void testQ14RetainsWidePayloadWhenDisabled()
    {
        PlanMatchPattern lineitemExchange = exchange(
                REMOTE_STREAMING,
                REPARTITION,
                anyTree(tableScan(
                        "lineitem",
                        ImmutableMap.of(
                                "LINE_PARTKEY", "partkey",
                                "EXTENDED_PRICE", "extendedprice",
                                "DISCOUNT", "discount"))))
                .withExactOutputs("LINE_PARTKEY", "EXTENDED_PRICE", "DISCOUNT");
        PlanMatchPattern partExchange = exchange(
                REMOTE_STREAMING,
                REPARTITION,
                tableScan(
                        "part",
                        ImmutableMap.of(
                                "PART_PARTKEY", "partkey",
                                "PART_TYPE", "type")))
                .withExactOutputs("PART_PARTKEY", "PART_TYPE");

        assertDistributedPlan(
                Q14,
                q14Session(false),
                anyTree(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("LINE_PARTKEY", "PART_PARTKEY")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        lineitemExchange,
                        anyTree(partExchange))));
    }

    private Session q14Session(boolean enabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, "false")
                .setSystemProperty(PUSH_SIDE_LOCAL_PROJECTION_THROUGH_JOIN, Boolean.toString(enabled))
                .build();
    }
}
