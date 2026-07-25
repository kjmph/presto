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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHivePairedExistsAggregationRewrite
        extends AbstractTestQueryFramework
{
    private static final String TABLE_NAME = "test_paired_exists_fact";
    private static final String NULLABLE_TABLE_NAME = "test_paired_exists_nullable_fact";
    private static final String QUERY = pairedExistsQuery(TABLE_NAME);

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HiveQueryRunner.createQueryRunner(
                ImmutableList.<TpchTable<?>>of(),
                ImmutableList.of(),
                ImmutableMap.of(),
                ImmutableMap.of(),
                "sql-standard",
                ImmutableMap.of(
                        "hive.pushdown-filter-enabled", "true",
                        "hive.parquet.pushdown-filter-enabled", "true"),
                Optional.of(2),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
    }

    @Test
    public void testRewriteSurvivesConnectorFilterPushdown()
    {
        assertUpdate("CREATE TABLE " + TABLE_NAME + " (" +
                "orderkey BIGINT NOT NULL, " +
                "suppkey BIGINT NOT NULL, " +
                "receiptdate DATE NOT NULL, " +
                "commitdate DATE NOT NULL) " +
                "WITH (format = 'PARQUET')");
        try {
            assertUpdate("INSERT INTO " + TABLE_NAME + " VALUES " +
                    "(1, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(1, 10, DATE '1995-01-03', DATE '1995-01-01'), " +
                    "(1, 20, DATE '1995-01-01', DATE '1995-01-02'), " +
                    "(2, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(2, 20, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(3, 10, DATE '1995-01-02', DATE '1995-01-01')",
                    6);

            // The Java Parquet reader cannot execute complex pushed filters,
            // so validate result equivalence independently of the plan-ordering
            // regression exercised below.
            Session optimized = session(true, false);
            Session baseline = session(false, false);
            assertQueryWithSameQueryRunner(optimized, QUERY, baseline);
            assertQuery(optimized, QUERY, "SELECT CAST(2 AS BIGINT)");
            assertEquals(countTableScans(plan(QUERY, session(true, true))), 1);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + TABLE_NAME);
        }
    }

    @Test
    public void testRewriteRequiresNonNullProof()
    {
        assertUpdate("CREATE TABLE " + NULLABLE_TABLE_NAME + " (" +
                "orderkey BIGINT, " +
                "suppkey BIGINT, " +
                "receiptdate DATE, " +
                "commitdate DATE) " +
                "WITH (format = 'PARQUET')");
        try {
            assertEquals(countTableScans(plan(pairedExistsQuery(NULLABLE_TABLE_NAME), session(true, true))), 3);
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + NULLABLE_TABLE_NAME);
        }
    }

    private Session session(boolean pushAggregationThroughJoin, boolean pushdownFilter)
    {
        return Session.builder(getSession())
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, String.valueOf(pushAggregationThroughJoin))
                .setCatalogSessionProperty("hive", "pushdown_filter_enabled", String.valueOf(pushdownFilter))
                .setCatalogSessionProperty("hive", "parquet_pushdown_filter_enabled", String.valueOf(pushdownFilter))
                .build();
    }

    private static int countTableScans(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(TableScanNode.class::isInstance)
                .count();
    }

    private static String pairedExistsQuery(String tableName)
    {
        return "SELECT count(*) " +
                "FROM " + tableName + " l1 " +
                "WHERE l1.receiptdate > l1.commitdate " +
                "AND EXISTS (" +
                "    SELECT * FROM " + tableName + " l2 " +
                "    WHERE l2.orderkey = l1.orderkey " +
                "      AND l2.suppkey <> l1.suppkey) " +
                "AND NOT EXISTS (" +
                "    SELECT * FROM " + tableName + " l3 " +
                "    WHERE l3.orderkey = l1.orderkey " +
                "      AND l3.suppkey <> l1.suppkey " +
                "      AND l3.receiptdate > l3.commitdate)";
    }
}
