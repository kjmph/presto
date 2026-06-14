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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.constraints.ForeignKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.TestTableConstraintsConnectorFactory;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPushQ10ThroughTrustedLookupJoins
        extends BaseRuleTest
{
    private TableHandle ordersTableHandle;
    private TableHandle lineitemTableHandle;
    private TableHandle customerTableHandle;
    private TableHandle nationTableHandle;

    private ColumnHandle lineitemOrderkeyColumn;
    private ColumnHandle lineitemExtendedpriceColumn;
    private ColumnHandle ordersCustkeyColumn;
    private ColumnHandle ordersOrderkeyColumn;
    private ColumnHandle ordersTotalpriceColumn;
    private ColumnHandle customerCustkeyColumn;
    private ColumnHandle customerNameColumn;
    private ColumnHandle customerAddressColumn;
    private ColumnHandle customerPhoneColumn;
    private ColumnHandle customerCommentColumn;
    private ColumnHandle customerAcctbalColumn;
    private ColumnHandle customerNationkeyColumn;
    private ColumnHandle nationNationkeyColumn;
    private ColumnHandle nationNameColumn;

    @Override
    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(emptyList(), ImmutableMap.of(), Optional.of(1), new TestTableConstraintsConnectorFactory(1));
        ConnectorId connectorId = tester().getCurrentConnectorId();

        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        TpchTableHandle lineitemTpchTableHandle = new TpchTableHandle("lineitem", 1.0);
        TpchTableHandle customerTpchTableHandle = new TpchTableHandle("customer", 1.0);
        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);

        ordersTableHandle = tableHandle(connectorId, ordersTpchTableHandle);
        lineitemTableHandle = tableHandle(connectorId, lineitemTpchTableHandle);
        customerTableHandle = tableHandle(connectorId, customerTpchTableHandle);
        nationTableHandle = tableHandle(connectorId, nationTpchTableHandle);

        lineitemOrderkeyColumn = new TpchColumnHandle("orderkey", BIGINT);
        lineitemExtendedpriceColumn = new TpchColumnHandle("extendedprice", DOUBLE);
        ordersCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        ordersOrderkeyColumn = new TpchColumnHandle("orderkey", BIGINT);
        ordersTotalpriceColumn = new TpchColumnHandle("totalprice", DOUBLE);
        customerCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        customerNameColumn = new TpchColumnHandle("name", VARCHAR);
        customerAddressColumn = new TpchColumnHandle("address", VARCHAR);
        customerPhoneColumn = new TpchColumnHandle("phone", VARCHAR);
        customerCommentColumn = new TpchColumnHandle("comment", VARCHAR);
        customerAcctbalColumn = new TpchColumnHandle("acctbal", DOUBLE);
        customerNationkeyColumn = new TpchColumnHandle("nationkey", BIGINT);
        nationNationkeyColumn = new TpchColumnHandle("nationkey", BIGINT);
        nationNameColumn = new TpchColumnHandle("name", VARCHAR);
    }

    @Test
    public void testQ10TopNIsPushedBelowFullTrustedLookupPayloads()
    {
        VariableReferenceExpression lOrderkey = variable("l_orderkey", BIGINT);
        VariableReferenceExpression lExtendedprice = variable("l_extendedprice", DOUBLE);
        VariableReferenceExpression oOrderkey = variable("o_orderkey", BIGINT);
        VariableReferenceExpression oCustkey = variable("o_custkey", BIGINT);
        VariableReferenceExpression cCustkey = variable("c_custkey", BIGINT);
        VariableReferenceExpression cName = variable("c_name", VARCHAR);
        VariableReferenceExpression cAddress = variable("c_address", VARCHAR);
        VariableReferenceExpression cPhone = variable("c_phone", VARCHAR);
        VariableReferenceExpression cComment = variable("c_comment", VARCHAR);
        VariableReferenceExpression cAcctbal = variable("c_acctbal", DOUBLE);
        VariableReferenceExpression cNationkey = variable("c_nationkey", BIGINT);
        VariableReferenceExpression nNationkey = variable("n_nationkey", BIGINT);
        VariableReferenceExpression nName = variable("n_name", VARCHAR);
        VariableReferenceExpression revenue = variable("revenue", DOUBLE);
        Set<VariableReferenceExpression> lookupPayloads = ImmutableSet.of(cCustkey, cName, cAcctbal, cPhone, nName, cAddress, cComment);

        tester.assertThat(ImmutableSet.of(
                        new PushAggregationThroughCardinalityPreservingLookupJoin(tester.getMetadata()),
                        new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata())))
                .on(p -> {
                    registerVariables(
                            p,
                            lOrderkey,
                            lExtendedprice,
                            oOrderkey,
                            oCustkey,
                            cCustkey,
                            cName,
                            cAddress,
                            cPhone,
                            cComment,
                            cAcctbal,
                            cNationkey,
                            nNationkey,
                            nName,
                            revenue);

                    TableScanNode lineitem = p.tableScan(
                            lineitemTableHandle,
                            ImmutableList.of(lOrderkey, lExtendedprice),
                            ImmutableMap.of(lOrderkey, lineitemOrderkeyColumn, lExtendedprice, lineitemExtendedpriceColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(lineitemTableHandle));
                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(oOrderkey, oCustkey),
                            ImmutableMap.of(oOrderkey, ordersOrderkeyColumn, oCustkey, ordersCustkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            constraintsWith(
                                    ordersTableHandle,
                                    new ForeignKeyConstraint<>(
                                            Optional.of("fk_orders_customer"),
                                            new LinkedHashSet<>(ImmutableList.of(ordersCustkeyColumn)),
                                            new SchemaTableName("sf1.0", "customer"),
                                            new LinkedHashSet<>(ImmutableList.of("custkey")),
                                            false,
                                            true,
                                            false)));
                    TableScanNode customer = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(cCustkey, cName, cAddress, cPhone, cComment, cAcctbal, cNationkey),
                            ImmutableMap.of(
                                    cCustkey, customerCustkeyColumn,
                                    cName, customerNameColumn,
                                    cAddress, customerAddressColumn,
                                    cPhone, customerPhoneColumn,
                                    cComment, customerCommentColumn,
                                    cAcctbal, customerAcctbalColumn,
                                    cNationkey, customerNationkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            constraintsWith(
                                    customerTableHandle,
                                    new ForeignKeyConstraint<>(
                                            Optional.of("fk_customer_nation"),
                                            new LinkedHashSet<>(ImmutableList.of(customerNationkeyColumn)),
                                            new SchemaTableName("sf1.0", "nation"),
                                            new LinkedHashSet<>(ImmutableList.of("nationkey")),
                                            false,
                                            true,
                                            false)));
                    TableScanNode nation = p.tableScan(
                            nationTableHandle,
                            ImmutableList.of(nNationkey, nName),
                            ImmutableMap.of(nNationkey, nationNationkeyColumn, nName, nationNameColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(nationTableHandle));

                    JoinNode ordersJoin = p.join(
                            JoinType.INNER,
                            lineitem,
                            orders,
                            new EquiJoinClause(lOrderkey, oOrderkey));
                    JoinNode customerJoin = p.join(
                            JoinType.INNER,
                            customer,
                            ordersJoin,
                            new EquiJoinClause(cCustkey, oCustkey));
                    JoinNode nationJoin = p.join(
                            JoinType.INNER,
                            customerJoin,
                            nation,
                            new EquiJoinClause(cNationkey, nNationkey));

                    AggregationNode aggregation = p.aggregation(builder -> builder
                            .singleGroupingSet(cCustkey, cName, cAcctbal, cPhone, nName, cAddress, cComment)
                            .addAggregation(revenue, p.rowExpression("sum(l_extendedprice)"))
                            .source(nationJoin));

                    return p.topN(20, ImmutableList.of(revenue), aggregation);
                })
                .validates(plan -> assertQ10Shape(
                        plan,
                        lookupPayloads,
                        oCustkey));
    }

    @Test
    public void testQ10TopNIsPushedBelowTrustedLookupPayloads()
    {
        VariableReferenceExpression oCustkey = variable("o_custkey", BIGINT);
        VariableReferenceExpression revenueInput = variable("revenue_input", DOUBLE);
        VariableReferenceExpression cCustkey = variable("c_custkey", BIGINT);
        VariableReferenceExpression cName = variable("c_name", VARCHAR);
        VariableReferenceExpression cAcctbal = variable("c_acctbal", DOUBLE);
        VariableReferenceExpression cNationkey = variable("c_nationkey", BIGINT);
        VariableReferenceExpression nNationkey = variable("n_nationkey", BIGINT);
        VariableReferenceExpression nName = variable("n_name", VARCHAR);
        VariableReferenceExpression revenue = variable("revenue", DOUBLE);
        Set<VariableReferenceExpression> lookupPayloads = ImmutableSet.of(cCustkey, cName, cAcctbal, nName);

        tester.assertThat(ImmutableSet.of(
                        new PushAggregationThroughCardinalityPreservingLookupJoin(tester.getMetadata()),
                        new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata())))
                .on(p -> {
                    registerVariables(p, oCustkey, revenueInput, cCustkey, cName, cAcctbal, cNationkey, nNationkey, nName, revenue);

                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(oCustkey, revenueInput),
                            ImmutableMap.of(oCustkey, ordersCustkeyColumn, revenueInput, ordersTotalpriceColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            constraintsWith(
                                    ordersTableHandle,
                                    new ForeignKeyConstraint<>(
                                            Optional.of("fk_orders_customer"),
                                            new LinkedHashSet<>(ImmutableList.of(ordersCustkeyColumn)),
                                            new SchemaTableName("sf1.0", "customer"),
                                            new LinkedHashSet<>(ImmutableList.of("custkey")),
                                            false,
                                            true,
                                            false)));
                    TableScanNode customer = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(cCustkey, cName, cAcctbal, cNationkey),
                            ImmutableMap.of(
                                    cCustkey, customerCustkeyColumn,
                                    cName, customerNameColumn,
                                    cAcctbal, customerAcctbalColumn,
                                    cNationkey, customerNationkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            constraintsWith(
                                    customerTableHandle,
                                    new ForeignKeyConstraint<>(
                                            Optional.of("fk_customer_nation"),
                                            new LinkedHashSet<>(ImmutableList.of(customerNationkeyColumn)),
                                            new SchemaTableName("sf1.0", "nation"),
                                            new LinkedHashSet<>(ImmutableList.of("nationkey")),
                                            false,
                                            true,
                                            false)));
                    TableScanNode nation = p.tableScan(
                            nationTableHandle,
                            ImmutableList.of(nNationkey, nName),
                            ImmutableMap.of(nNationkey, nationNationkeyColumn, nName, nationNameColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(nationTableHandle));

                    JoinNode customerJoin = p.join(
                            JoinType.INNER,
                            customer,
                            orders,
                            new EquiJoinClause(cCustkey, oCustkey));
                    JoinNode nationJoin = p.join(
                            JoinType.INNER,
                            customerJoin,
                            nation,
                            new EquiJoinClause(cNationkey, nNationkey));

                    PlanNode project = p.project(
                            Assignments.builder()
                                    .put(cCustkey, cCustkey)
                                    .put(cName, cName)
                                    .put(cAcctbal, cAcctbal)
                                    .put(nName, nName)
                                    .put(revenueInput, revenueInput)
                                    .build(),
                            nationJoin);

                    AggregationNode aggregation = p.aggregation(builder -> builder
                            .singleGroupingSet(cCustkey, cName, cAcctbal, nName)
                            .addAggregation(revenue, p.rowExpression("sum(revenue_input)"))
                            .source(project));

                    return p.topN(20, ImmutableList.of(revenue), aggregation);
                })
                .validates(plan -> assertQ10Shape(
                        plan,
                        lookupPayloads,
                        oCustkey));
    }

    @Test
    public void testQ10ResidualFinalAggregationIsProjectedFromPreAggregation()
    {
        VariableReferenceExpression oCustkey = variable("o_custkey", BIGINT);
        VariableReferenceExpression revenueInput = variable("revenue_input", DOUBLE);
        VariableReferenceExpression preRevenue = variable("pre_revenue", DOUBLE);
        VariableReferenceExpression cCustkey = variable("c_custkey", BIGINT);
        VariableReferenceExpression cName = variable("c_name", VARCHAR);
        VariableReferenceExpression cAcctbal = variable("c_acctbal", DOUBLE);
        VariableReferenceExpression cNationkey = variable("c_nationkey", BIGINT);
        VariableReferenceExpression nNationkey = variable("n_nationkey", BIGINT);
        VariableReferenceExpression nName = variable("n_name", VARCHAR);
        VariableReferenceExpression revenue = variable("revenue", DOUBLE);
        Set<VariableReferenceExpression> lookupPayloads = ImmutableSet.of(cCustkey, cName, cAcctbal, nName);

        tester.assertThat(ImmutableSet.of(new PushAggregationThroughCardinalityPreservingLookupJoin(tester.getMetadata())))
                .on(p -> {
                    registerVariables(p, oCustkey, revenueInput, preRevenue, cCustkey, cName, cAcctbal, cNationkey, nNationkey, nName, revenue);

                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(oCustkey, revenueInput),
                            ImmutableMap.of(oCustkey, ordersCustkeyColumn, revenueInput, ordersTotalpriceColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            constraintsWith(
                                    ordersTableHandle,
                                    new ForeignKeyConstraint<>(
                                            Optional.of("fk_orders_customer"),
                                            new LinkedHashSet<>(ImmutableList.of(ordersCustkeyColumn)),
                                            new SchemaTableName("sf1.0", "customer"),
                                            new LinkedHashSet<>(ImmutableList.of("custkey")),
                                            false,
                                            true,
                                            false)));
                    TableScanNode customer = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(cCustkey, cName, cAcctbal, cNationkey),
                            ImmutableMap.of(
                                    cCustkey, customerCustkeyColumn,
                                    cName, customerNameColumn,
                                    cAcctbal, customerAcctbalColumn,
                                    cNationkey, customerNationkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            constraintsWith(
                                    customerTableHandle,
                                    new ForeignKeyConstraint<>(
                                            Optional.of("fk_customer_nation"),
                                            new LinkedHashSet<>(ImmutableList.of(customerNationkeyColumn)),
                                            new SchemaTableName("sf1.0", "nation"),
                                            new LinkedHashSet<>(ImmutableList.of("nationkey")),
                                            false,
                                            true,
                                            false)));
                    TableScanNode nation = p.tableScan(
                            nationTableHandle,
                            ImmutableList.of(nNationkey, nName),
                            ImmutableMap.of(nNationkey, nationNationkeyColumn, nName, nationNameColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(nationTableHandle));

                    AggregationNode preAggregation = p.aggregation(builder -> builder
                            .singleGroupingSet(oCustkey)
                            .addAggregation(preRevenue, p.rowExpression("sum(revenue_input)"))
                            .source(orders));

                    JoinNode customerJoin = p.join(
                            JoinType.INNER,
                            customer,
                            preAggregation,
                            new EquiJoinClause(cCustkey, oCustkey));
                    JoinNode nationJoin = p.join(
                            JoinType.INNER,
                            customerJoin,
                            nation,
                            new EquiJoinClause(cNationkey, nNationkey));

                    return p.aggregation(builder -> builder
                            .singleGroupingSet(cCustkey, cName, cAcctbal, nName)
                            .addAggregation(revenue, p.rowExpression("sum(pre_revenue)"))
                            .source(nationJoin));
                })
                .validates(plan -> assertNoLookupPayloadAggregation(
                        plan,
                        lookupPayloads));
    }

    @Test
    public void testQ10FirstLookupRewriteDoesNotKeepNationPayloadAggregation()
    {
        VariableReferenceExpression oCustkey = variable("o_custkey", BIGINT);
        VariableReferenceExpression revenueInput = variable("revenue_input", DOUBLE);
        VariableReferenceExpression preRevenue = variable("pre_revenue", DOUBLE);
        VariableReferenceExpression cCustkey = variable("c_custkey", BIGINT);
        VariableReferenceExpression cName = variable("c_name", VARCHAR);
        VariableReferenceExpression cAcctbal = variable("c_acctbal", DOUBLE);
        VariableReferenceExpression cPhone = variable("c_phone", VARCHAR);
        VariableReferenceExpression cAddress = variable("c_address", VARCHAR);
        VariableReferenceExpression cComment = variable("c_comment", VARCHAR);
        VariableReferenceExpression cNationkey = variable("c_nationkey", BIGINT);
        VariableReferenceExpression nNationkey = variable("n_nationkey", BIGINT);
        VariableReferenceExpression nName = variable("n_name", VARCHAR);
        VariableReferenceExpression revenue = variable("revenue", DOUBLE);

        tester.assertThat(ImmutableSet.of(new PushAggregationThroughCardinalityPreservingLookupJoin(tester.getMetadata())))
                .on(p -> {
                    registerVariables(
                            p,
                            oCustkey,
                            revenueInput,
                            preRevenue,
                            cCustkey,
                            cName,
                            cAcctbal,
                            cPhone,
                            cAddress,
                            cComment,
                            cNationkey,
                            nNationkey,
                            nName,
                            revenue);

                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(oCustkey, revenueInput),
                            ImmutableMap.of(oCustkey, ordersCustkeyColumn, revenueInput, ordersTotalpriceColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            constraintsWith(
                                    ordersTableHandle,
                                    new ForeignKeyConstraint<>(
                                            Optional.of("fk_orders_customer"),
                                            new LinkedHashSet<>(ImmutableList.of(ordersCustkeyColumn)),
                                            new SchemaTableName("sf1.0", "customer"),
                                            new LinkedHashSet<>(ImmutableList.of("custkey")),
                                            false,
                                            true,
                                            false)));
                    TableScanNode customer = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(cCustkey, cName, cAcctbal, cPhone, cAddress, cComment, cNationkey),
                            ImmutableMap.of(
                                    cCustkey, customerCustkeyColumn,
                                    cName, customerNameColumn,
                                    cAcctbal, customerAcctbalColumn,
                                    cPhone, customerPhoneColumn,
                                    cAddress, customerAddressColumn,
                                    cComment, customerCommentColumn,
                                    cNationkey, customerNationkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            constraintsWith(
                                    customerTableHandle,
                                    new ForeignKeyConstraint<>(
                                            Optional.of("fk_customer_nation"),
                                            new LinkedHashSet<>(ImmutableList.of(customerNationkeyColumn)),
                                            new SchemaTableName("sf1.0", "nation"),
                                            new LinkedHashSet<>(ImmutableList.of("nationkey")),
                                            false,
                                            true,
                                            false)));
                    TableScanNode nation = p.tableScan(
                            nationTableHandle,
                            ImmutableList.of(nNationkey, nName),
                            ImmutableMap.of(nNationkey, nationNationkeyColumn, nName, nationNameColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(nationTableHandle));

                    JoinNode customerOrders = p.join(
                            JoinType.INNER,
                            customer,
                            orders,
                            new EquiJoinClause(cCustkey, oCustkey));
                    AggregationNode preAggregation = p.aggregation(builder -> builder
                            .singleGroupingSet(cCustkey, cName, cAcctbal, cPhone, cAddress, cComment, cNationkey)
                            .addAggregation(preRevenue, p.rowExpression("sum(revenue_input)"))
                            .source(customerOrders));
                    JoinNode nationJoin = p.join(
                            JoinType.INNER,
                            preAggregation,
                            nation,
                            new EquiJoinClause(cNationkey, nNationkey));

                    return p.aggregation(builder -> builder
                            .singleGroupingSet(cCustkey, cName, cAcctbal, cPhone, nName, cAddress, cComment)
                            .addAggregation(revenue, p.rowExpression("sum(pre_revenue)"))
                            .source(nationJoin));
                })
                .validates(plan -> assertNoLookupPayloadAggregation(
                        plan,
                        ImmutableSet.of(nName)));
    }

    private void assertQ10Shape(
            Plan plan,
            Set<VariableReferenceExpression> lookupPayloads,
            VariableReferenceExpression preAggregationKey)
    {
        assertNoLookupPayloadAggregation(plan, lookupPayloads);

        assertTrue(
                searchFrom(plan.getRoot())
                        .where(node -> node instanceof TopNNode &&
                                ((TopNNode) node).getSource() instanceof AggregationNode &&
                                ((AggregationNode) ((TopNNode) node).getSource()).getGroupingKeys().stream()
                                        .allMatch(preAggregationKey::equals))
                        .matches(),
                "TopN should be applied directly above the narrow pre-aggregation key\n" + formatTree(plan.getRoot(), 0));
    }

    private void assertNoLookupPayloadAggregation(
            Plan plan,
            Set<VariableReferenceExpression> lookupPayloads)
    {
        assertFalse(
                searchFrom(plan.getRoot())
                        .where(node -> node instanceof AggregationNode &&
                                ((AggregationNode) node).getGroupingKeys().stream().anyMatch(lookupPayloads::contains))
                        .matches(),
                "lookup payload columns should not remain in aggregation grouping keys\n" + formatTree(plan.getRoot(), 0));
    }

    private List<TableConstraint<ColumnHandle>> constraintsWith(TableHandle tableHandle, TableConstraint<ColumnHandle> constraint)
    {
        return ImmutableList.<TableConstraint<ColumnHandle>>builder()
                .addAll(tester.getTableConstraints(tableHandle))
                .add(constraint)
                .build();
    }

    private static TableHandle tableHandle(ConnectorId connectorId, TpchTableHandle tpchTableHandle)
    {
        return new TableHandle(
                connectorId,
                tpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(tpchTableHandle, TupleDomain.all())));
    }

    private static VariableReferenceExpression variable(String name, Type type)
    {
        return new VariableReferenceExpression(Optional.empty(), name, type);
    }

    private static void registerVariables(PlanBuilder planBuilder, VariableReferenceExpression... variables)
    {
        for (VariableReferenceExpression variable : variables) {
            planBuilder.registerVariable(variable);
        }
    }

    private static String formatTree(PlanNode node, int indent)
    {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < indent; i++) {
            builder.append("  ");
        }
        builder.append(node.getClass().getSimpleName());
        if (node instanceof AggregationNode) {
            builder.append(" groupBy=")
                    .append(((AggregationNode) node).getGroupingKeys().stream()
                            .map(VariableReferenceExpression::getName)
                            .collect(ImmutableList.toImmutableList()));
        }
        if (node instanceof TopNNode) {
            builder.append(" count=").append(((TopNNode) node).getCount());
        }
        builder.append(" outputs=")
                .append(node.getOutputVariables().stream()
                        .map(VariableReferenceExpression::getName)
                        .collect(ImmutableList.toImmutableList()))
                .append('\n');
        for (PlanNode source : node.getSources()) {
            builder.append(formatTree(source, indent + 1));
        }
        return builder.toString();
    }
}
