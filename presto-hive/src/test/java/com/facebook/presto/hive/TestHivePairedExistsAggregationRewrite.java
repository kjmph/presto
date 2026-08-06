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
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.DISTRIBUTED_DYNAMIC_FILTER_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.JOINS_NOT_NULL_INFERENCE_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN;
import static com.facebook.presto.SystemSessionProperties.PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHivePairedExistsAggregationRewrite
        extends AbstractTestQueryFramework
{
    private static final String TABLE_NAME = "test_paired_exists_fact";
    private static final String NULLABLE_TABLE_NAME = "test_paired_exists_nullable_fact";
    private static final String QUERY = pairedExistsQuery(TABLE_NAME);
    private static final String Q21_LINEITEM_TABLE_NAME = "test_paired_exists_q21_lineitem";
    private static final String Q21_SUPPLIER_TABLE_NAME = "test_paired_exists_q21_supplier";
    private static final String Q21_ORDERS_TABLE_NAME = "test_paired_exists_q21_orders";
    private static final String Q21_NATION_TABLE_NAME = "test_paired_exists_q21_nation";
    private static final String Q21_QUERY = q21Query();

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

    @Test
    public void testCanonicalQ21RewriteSurvivesInferredNotNullAndConnectorFilterPushdown()
    {
        try {
            assertUpdate("CREATE TABLE " + Q21_NATION_TABLE_NAME + " (" +
                    "nationkey BIGINT NOT NULL, " +
                    "name VARCHAR NOT NULL, " +
                    "CONSTRAINT test_q21_nation_pk PRIMARY KEY (nationkey) DISABLED RELY NOT ENFORCED) " +
                    "WITH (format = 'PARQUET')");
            assertUpdate("CREATE TABLE " + Q21_SUPPLIER_TABLE_NAME + " (" +
                    "suppkey BIGINT NOT NULL, " +
                    "name VARCHAR NOT NULL, " +
                    "nationkey BIGINT NOT NULL, " +
                    "CONSTRAINT test_q21_supplier_pk PRIMARY KEY (suppkey) DISABLED RELY NOT ENFORCED) " +
                    "WITH (format = 'PARQUET')");
            assertUpdate("CREATE TABLE " + Q21_ORDERS_TABLE_NAME + " (" +
                    "orderkey BIGINT NOT NULL, " +
                    "orderstatus VARCHAR NOT NULL, " +
                    "CONSTRAINT test_q21_orders_pk PRIMARY KEY (orderkey) DISABLED RELY NOT ENFORCED) " +
                    "WITH (format = 'PARQUET')");
            assertUpdate("CREATE TABLE " + Q21_LINEITEM_TABLE_NAME + " (" +
                    "orderkey BIGINT NOT NULL, " +
                    "suppkey BIGINT NOT NULL, " +
                    "receiptdate DATE NOT NULL, " +
                    "commitdate DATE NOT NULL) " +
                    "WITH (format = 'PARQUET')");

            assertUpdate("INSERT INTO " + Q21_NATION_TABLE_NAME + " VALUES " +
                    "(1, 'SAUDI ARABIA'), " +
                    "(2, 'CANADA')",
                    2);
            assertUpdate("INSERT INTO " + Q21_SUPPLIER_TABLE_NAME + " VALUES " +
                    "(10, 'Supplier#10', 1), " +
                    "(20, 'Supplier#20', 1), " +
                    "(30, 'Supplier#30', 2)",
                    3);
            assertUpdate("INSERT INTO " + Q21_ORDERS_TABLE_NAME + " VALUES " +
                    "(1, 'F'), " +
                    "(2, 'F'), " +
                    "(3, 'F'), " +
                    "(4, 'O'), " +
                    "(5, 'F'), " +
                    "(6, 'F')",
                    6);
            assertUpdate("INSERT INTO " + Q21_LINEITEM_TABLE_NAME + " VALUES " +
                    "(1, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(1, 10, DATE '1995-01-03', DATE '1995-01-01'), " +
                    "(1, 20, DATE '1995-01-01', DATE '1995-01-02'), " +
                    "(2, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(2, 20, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(3, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(4, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(4, 20, DATE '1995-01-01', DATE '1995-01-02'), " +
                    "(5, 30, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(5, 20, DATE '1995-01-01', DATE '1995-01-02'), " +
                    "(6, 20, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(6, 10, DATE '1995-01-01', DATE '1995-01-02'), " +
                    // Preserve the expected Q21 result while giving the cost
                    // model the fact-to-group reduction seen at benchmark scale.
                    "(4, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(4, 20, DATE '1995-01-01', DATE '1995-01-02'), " +
                    "(4, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(4, 20, DATE '1995-01-01', DATE '1995-01-02'), " +
                    "(4, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(4, 20, DATE '1995-01-01', DATE '1995-01-02'), " +
                    "(4, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(4, 20, DATE '1995-01-01', DATE '1995-01-02'), " +
                    "(4, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(4, 20, DATE '1995-01-01', DATE '1995-01-02'), " +
                    "(4, 10, DATE '1995-01-02', DATE '1995-01-01'), " +
                    "(4, 20, DATE '1995-01-01', DATE '1995-01-02')",
                    24);

            // The default unique-lookup pushdown is cost based and fails closed
            // without grouping-key and join-key statistics.
            Session statisticsSession = session(true, false);
            assertUpdate(statisticsSession, "ANALYZE " + Q21_NATION_TABLE_NAME, 2);
            assertUpdate(statisticsSession, "ANALYZE " + Q21_SUPPLIER_TABLE_NAME, 3);
            assertUpdate(statisticsSession, "ANALYZE " + Q21_ORDERS_TABLE_NAME, 6);
            assertUpdate(statisticsSession, "ANALYZE " + Q21_LINEITEM_TABLE_NAME, 24);

            // Keep execution independent of the Java Parquet reader's support
            // for complex pushed filters, while planning with both pushdowns
            // and inferred NOT NULL filters enabled to exercise the production
            // Q21 optimizer ordering.
            Session optimized = q21Session(true, false);
            Session baseline = q21Session(false, false);
            assertQueryWithSameQueryRunner(optimized, Q21_QUERY, baseline);
            assertQueryOrdered(
                    optimized,
                    Q21_QUERY,
                    "VALUES ('Supplier#10', CAST(2 AS BIGINT)), ('Supplier#20', CAST(1 AS BIGINT))");
            assertEquals(
                    countTableScans(plan(Q21_QUERY, optimized), Q21_LINEITEM_TABLE_NAME),
                    1);
            assertEquals(
                    countTableScans(plan(Q21_QUERY, baseline), Q21_LINEITEM_TABLE_NAME),
                    3);
            assertEquals(
                    countTableScans(plan(Q21_QUERY, q21Session(true, true)), Q21_LINEITEM_TABLE_NAME),
                    1);

            Plan partialPushdownDisabled = plan(Q21_QUERY, q21PartitionedSession(false));
            Plan partialPushdownEnabled = plan(Q21_QUERY, q21PartitionedSession(true));
            Plan partialPushdownReplicated = plan(Q21_QUERY, q21BroadcastSession(true));
            assertFalse(hasPartialAggregationBelowOrdersJoinAndExchange(partialPushdownDisabled));
            assertTrue(hasPartialAggregationBelowOrdersJoinAndExchange(partialPushdownEnabled));
            assertFalse(hasPartialAggregationBelowOrdersJoinAndExchange(partialPushdownReplicated));

            Session uniqueLookupPushdownDefaultExecution = q21UniqueLookupAggregationSession(false);
            Session uniqueLookupPushdownDisabledExecution = q21UniqueLookupAggregationDisabledSession(false);
            assertQueryWithSameQueryRunner(uniqueLookupPushdownDefaultExecution, Q21_QUERY, uniqueLookupPushdownDisabledExecution);

            Plan uniqueLookupPushdownDisabled = plan(Q21_QUERY, q21UniqueLookupAggregationDisabledSession(true));
            Plan uniqueLookupPushdownDefault = plan(Q21_QUERY, q21UniqueLookupAggregationSession(true));
            Plan uniqueLookupPushdownWithDynamicFilters = plan(Q21_QUERY, q21UniqueLookupAggregationWithDynamicFiltersSession(true));
            Plan uniqueLookupPushdownWithPartialAggregation = plan(Q21_QUERY, q21UniqueLookupAggregationWithPartialSession(true));
            assertEquals(countTableScans(uniqueLookupPushdownDisabled, Q21_LINEITEM_TABLE_NAME), 1);
            assertEquals(countTableScans(uniqueLookupPushdownDefault, Q21_LINEITEM_TABLE_NAME), 1);
            assertOrdersJoinAndFilterRetained(uniqueLookupPushdownDisabled);
            assertOrdersJoinAndFilterRetained(uniqueLookupPushdownDefault);
            assertFalse(hasCompleteAggregationBelowOrdersJoin(uniqueLookupPushdownDisabled));
            assertTrue(hasCompleteAggregationBelowOrdersJoin(uniqueLookupPushdownDefault));
            assertTrue(hasCompleteAggregationBelowOrdersJoin(uniqueLookupPushdownWithDynamicFilters));
            assertTrue(hasPartialAggregationBelowOrdersJoinAndExchange(uniqueLookupPushdownWithPartialAggregation));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS " + Q21_LINEITEM_TABLE_NAME);
            assertUpdate("DROP TABLE IF EXISTS " + Q21_ORDERS_TABLE_NAME);
            assertUpdate("DROP TABLE IF EXISTS " + Q21_SUPPLIER_TABLE_NAME);
            assertUpdate("DROP TABLE IF EXISTS " + Q21_NATION_TABLE_NAME);
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

    private Session q21Session(boolean pushAggregationThroughJoin, boolean pushdownFilter)
    {
        return Session.builder(session(pushAggregationThroughJoin, pushdownFilter))
                .setSystemProperty(JOINS_NOT_NULL_INFERENCE_STRATEGY, "USE_FUNCTION_METADATA")
                .build();
    }

    private Session q21PartitionedSession(boolean pushPartialAggregationThroughJoin)
    {
        return Session.builder(q21Session(true, true))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "ALWAYS")
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, String.valueOf(pushPartialAggregationThroughJoin))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN, "false")
                .build();
    }

    private Session q21BroadcastSession(boolean pushPartialAggregationThroughJoin)
    {
        return Session.builder(q21Session(true, true))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "ALWAYS")
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, String.valueOf(pushPartialAggregationThroughJoin))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN, "false")
                .build();
    }

    private Session q21UniqueLookupAggregationSession(boolean pushdownFilter)
    {
        return Session.builder(q21Session(true, pushdownFilter))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .setSystemProperty(PUSH_PARTIAL_AGGREGATION_THROUGH_JOIN, "false")
                .build();
    }

    private Session q21UniqueLookupAggregationDisabledSession(boolean pushdownFilter)
    {
        return Session.builder(q21UniqueLookupAggregationSession(pushdownFilter))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN, "false")
                .build();
    }

    private Session q21UniqueLookupAggregationWithDynamicFiltersSession(boolean pushdownFilter)
    {
        return Session.builder(q21UniqueLookupAggregationSession(pushdownFilter))
                .setSystemProperty(DISTRIBUTED_DYNAMIC_FILTER_STRATEGY, "ALWAYS")
                .build();
    }

    private Session q21UniqueLookupAggregationWithPartialSession(boolean pushdownFilter)
    {
        return Session.builder(q21UniqueLookupAggregationSession(pushdownFilter))
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "ALWAYS")
                .build();
    }

    private static int countTableScans(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(TableScanNode.class::isInstance)
                .count();
    }

    private static int countTableScans(Plan plan, String tableName)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof TableScanNode &&
                        ((HiveTableHandle) ((TableScanNode) node).getTable().getConnectorHandle()).getTableName().equals(tableName))
                .count();
    }

    private static boolean hasPartialAggregationBelowOrdersJoinAndExchange(Plan plan)
    {
        JoinNode ordersJoin = findLineitemOrdersJoin(plan);
        PlanNode lineitemSource = containsTable(ordersJoin.getLeft(), Q21_LINEITEM_TABLE_NAME) ?
                ordersJoin.getLeft() :
                ordersJoin.getRight();
        return searchFrom(lineitemSource)
                .where(node -> node instanceof ExchangeNode && ((ExchangeNode) node).getScope().isRemote() &&
                        node.getSources().stream().anyMatch(source ->
                                containsTable(source, Q21_LINEITEM_TABLE_NAME) &&
                                        containsPartialAggregation(source)))
                .matches();
    }

    private static void assertOrdersJoinAndFilterRetained(Plan plan)
    {
        findLineitemOrdersJoin(plan);

        TableScanNode ordersScan = searchFrom(plan.getRoot())
                .where(node -> node instanceof TableScanNode &&
                        ((HiveTableHandle) ((TableScanNode) node).getTable().getConnectorHandle()).getTableName().equals(Q21_ORDERS_TABLE_NAME))
                .<TableScanNode>findOnlyElement();
        assertTrue(ordersScan.getTable().getLayout().isPresent());
        HiveTableLayoutHandle layout = (HiveTableLayoutHandle) ordersScan.getTable().getLayout().get();
        assertTrue(layout.getPredicateColumns().containsKey("orderstatus"));
    }

    private static boolean hasCompleteAggregationBelowOrdersJoin(Plan plan)
    {
        JoinNode ordersJoin = findLineitemOrdersJoin(plan);
        PlanNode lineitemSource = containsTable(ordersJoin.getLeft(), Q21_LINEITEM_TABLE_NAME) ?
                ordersJoin.getLeft() :
                ordersJoin.getRight();
        return searchFrom(lineitemSource)
                .where(node -> node instanceof AggregationNode &&
                        (((AggregationNode) node).getStep() == SINGLE || ((AggregationNode) node).getStep() == FINAL))
                .matches();
    }

    private static JoinNode findLineitemOrdersJoin(Plan plan)
    {
        return searchFrom(plan.getRoot())
                .where(node -> node instanceof JoinNode && isLineitemOrdersJoin((JoinNode) node))
                .<JoinNode>findFirst()
                .orElseThrow(() -> new AssertionError("lineitem-orders join not found"));
    }

    private static boolean containsPartialAggregation(PlanNode root)
    {
        return searchFrom(root)
                .where(node -> node instanceof AggregationNode && ((AggregationNode) node).getStep() == PARTIAL)
                .matches();
    }

    private static boolean isLineitemOrdersJoin(JoinNode join)
    {
        boolean leftHasLineitem = containsTable(join.getLeft(), Q21_LINEITEM_TABLE_NAME);
        boolean leftHasOrders = containsTable(join.getLeft(), Q21_ORDERS_TABLE_NAME);
        boolean rightHasLineitem = containsTable(join.getRight(), Q21_LINEITEM_TABLE_NAME);
        boolean rightHasOrders = containsTable(join.getRight(), Q21_ORDERS_TABLE_NAME);
        return (leftHasLineitem && !leftHasOrders && rightHasOrders && !rightHasLineitem) ||
                (rightHasLineitem && !rightHasOrders && leftHasOrders && !leftHasLineitem);
    }

    private static boolean containsTable(PlanNode root, String tableName)
    {
        return searchFrom(root)
                .where(node -> node instanceof TableScanNode &&
                        ((HiveTableHandle) ((TableScanNode) node).getTable().getConnectorHandle()).getTableName().equals(tableName))
                .matches();
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

    private static String q21Query()
    {
        return "SELECT s.name, count(*) AS numwait " +
                "FROM " + Q21_SUPPLIER_TABLE_NAME + " s, " +
                Q21_LINEITEM_TABLE_NAME + " l1, " +
                Q21_ORDERS_TABLE_NAME + " o, " +
                Q21_NATION_TABLE_NAME + " n " +
                "WHERE s.suppkey = l1.suppkey " +
                "AND o.orderkey = l1.orderkey " +
                "AND o.orderstatus = 'F' " +
                "AND l1.receiptdate > l1.commitdate " +
                "AND EXISTS (" +
                "    SELECT * FROM " + Q21_LINEITEM_TABLE_NAME + " l2 " +
                "    WHERE l2.orderkey = l1.orderkey " +
                "      AND l2.suppkey <> l1.suppkey) " +
                "AND NOT EXISTS (" +
                "    SELECT * FROM " + Q21_LINEITEM_TABLE_NAME + " l3 " +
                "    WHERE l3.orderkey = l1.orderkey " +
                "      AND l3.suppkey <> l1.suppkey " +
                "      AND l3.receiptdate > l3.commitdate) " +
                "AND s.nationkey = n.nationkey " +
                "AND n.name = 'SAUDI ARABIA' " +
                "GROUP BY s.name " +
                "ORDER BY numwait DESC, s.name " +
                "LIMIT 100";
    }
}
