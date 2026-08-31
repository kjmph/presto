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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinDistributionType;
import com.facebook.presto.sql.analyzer.FeaturesConfig.JoinReorderingStrategy;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_REORDERING_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_HASH_GENERATION;
import static com.facebook.presto.SystemSessionProperties.PUSH_SIDE_LOCAL_PROJECTION_THROUGH_JOIN;

public class TestPushSideLocalProjectionThroughJoinDistributed
        extends AbstractTestQueryFramework
{
    private static final String Q14 = "SELECT " +
            "100.00 * sum(CASE " +
            "    WHEN p.type LIKE 'PROMO%' THEN l.extendedprice * (1 - l.discount) " +
            "    ELSE 0 " +
            "END) / sum(l.extendedprice * (1 - l.discount)) AS promo_revenue " +
            "FROM lineitem l " +
            "JOIN part p ON l.partkey = p.partkey " +
            "WHERE l.shipdate >= DATE '1995-09-01' " +
            "AND l.shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return TpchQueryRunnerBuilder.builder().build();
    }

    @Test
    public void testQ14MatchesWithRuleDisabled()
    {
        assertQueryWithSameQueryRunner(
                q14Session(true),
                Q14,
                q14Session(false));
    }

    private Session q14Session(boolean enabled)
    {
        return Session.builder(getSession())
                .setSystemProperty(JOIN_REORDERING_STRATEGY, JoinReorderingStrategy.NONE.name())
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, JoinDistributionType.PARTITIONED.name())
                .setSystemProperty(OPTIMIZE_HASH_GENERATION, "false")
                .setSystemProperty(PUSH_SIDE_LOCAL_PROJECTION_THROUGH_JOIN, Boolean.toString(enabled))
                .build();
    }
}
