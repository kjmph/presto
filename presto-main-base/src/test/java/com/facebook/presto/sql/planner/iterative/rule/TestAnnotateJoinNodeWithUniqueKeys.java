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
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.constraints.ForeignKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TestTableConstraintsConnectorFactory;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.Optional;

import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.IS_NULL;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.joinWithKeyProperties;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestAnnotateJoinNodeWithUniqueKeys
{
    private RuleTester tester;
    private LogicalPropertiesProviderImpl logicalPropertiesProvider;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(emptyList(), ImmutableMap.of("exploit_constraints", Boolean.toString(true)), Optional.of(1), new TestTableConstraintsConnectorFactory(1));
        logicalPropertiesProvider = new LogicalPropertiesProviderImpl(new FunctionResolution(tester.getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver()));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        closeAllRuntimeException(tester);
        tester = null;
        logicalPropertiesProvider = null;
    }

    @Test
    public void testAnnotatesRightJoinKeysUnique()
    {
        tester.assertThat(ImmutableSet.of(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata())), logicalPropertiesProvider)
                .on("SELECT * FROM orders o JOIN customer c ON o.custkey = c.custkey")
                .matches(anyTree(
                        joinWithKeyProperties(
                                INNER,
                                ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                false,
                                true,
                                false,
                                true,
                                tableScan("orders", ImmutableMap.of("custkey", "custkey")),
                                tableScan("customer", ImmutableMap.of("custkey_0", "custkey")))));
    }

    @Test
    public void testAnnotatesProbeKeysCoveredByBuildKeys()
    {
        ConnectorId connectorId = tester.getCurrentConnectorId();
        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        TpchTableHandle customerTpchTableHandle = new TpchTableHandle("customer", 1.0);
        TableHandle ordersTableHandle = tableHandle(connectorId, ordersTpchTableHandle);
        TableHandle customerTableHandle = tableHandle(connectorId, customerTpchTableHandle);
        ColumnHandle ordersCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        ColumnHandle customerCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        TableConstraint<ColumnHandle> ordersCustomerFk = new ForeignKeyConstraint<>(
                Optional.of("fk_orders_customer"),
                new LinkedHashSet<>(ImmutableList.of(ordersCustkeyColumn)),
                new SchemaTableName("sf1.0", "customer"),
                new LinkedHashSet<>(ImmutableList.of("custkey")),
                false,
                true,
                false);

        tester.assertThat(ImmutableSet.of(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata())), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression ordersCustkey = p.variable("o_custkey", BIGINT);
                    VariableReferenceExpression customerCustkey = p.variable("c_custkey", BIGINT);
                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustkey),
                            ImmutableMap.of(ordersCustkey, ordersCustkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            ImmutableList.<TableConstraint<ColumnHandle>>builder()
                                    .addAll(tester.getTableConstraints(ordersTableHandle))
                                    .add(ordersCustomerFk)
                                    .build());
                    TableScanNode customer = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustkey),
                            ImmutableMap.of(customerCustkey, customerCustkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(customerTableHandle));

                    return p.join(
                            INNER,
                            orders,
                            customer,
                            new EquiJoinClause(ordersCustkey, customerCustkey));
                })
                .matches(joinWithKeyProperties(
                        INNER,
                        ImmutableList.of(equiJoinClause("o_custkey", "c_custkey")),
                        false,
                        true,
                        false,
                        true,
                        true,
                        false,
                        tableScan("orders", ImmutableMap.of("o_custkey", "custkey")),
                        tableScan("customer", ImmutableMap.of("c_custkey", "custkey"))));
    }

    @Test
    public void testAnnotatesProbeKeysCoveredByBuildKeysThroughTargetNotNullFilter()
    {
        ConnectorId connectorId = tester.getCurrentConnectorId();
        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        TpchTableHandle customerTpchTableHandle = new TpchTableHandle("customer", 1.0);
        TableHandle ordersTableHandle = tableHandle(connectorId, ordersTpchTableHandle);
        TableHandle customerTableHandle = tableHandle(connectorId, customerTpchTableHandle);
        ColumnHandle ordersCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        ColumnHandle customerCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        TableConstraint<ColumnHandle> ordersCustomerFk = new ForeignKeyConstraint<>(
                Optional.of("fk_orders_customer"),
                new LinkedHashSet<>(ImmutableList.of(ordersCustkeyColumn)),
                new SchemaTableName("sf1.0", "customer"),
                new LinkedHashSet<>(ImmutableList.of("custkey")),
                false,
                true,
                false);

        JoinNode join = (JoinNode) tester.assertThat(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata()), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression ordersCustkey = p.variable("o_custkey", BIGINT);
                    VariableReferenceExpression customerCustkey = p.variable("c_custkey", BIGINT);
                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustkey),
                            ImmutableMap.of(ordersCustkey, ordersCustkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            ImmutableList.<TableConstraint<ColumnHandle>>builder()
                                    .addAll(tester.getTableConstraints(ordersTableHandle))
                                    .add(ordersCustomerFk)
                                    .build());
                    TableScanNode customer = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustkey),
                            ImmutableMap.of(customerCustkey, customerCustkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(customerTableHandle));

                    return p.join(
                            INNER,
                            orders,
                            p.filter(isNotNull(customerCustkey), customer),
                            new EquiJoinClause(ordersCustkey, customerCustkey));
                })
                .get();

        assertTrue(join.isLeftKeysCoveredByRightKeys());
        assertFalse(join.isRightKeysCoveredByLeftKeys());
    }

    @Test
    public void testAnnotatesProbeKeysCoveredByBuildKeysThroughTargetPreservingJoin()
    {
        ConnectorId connectorId = tester.getCurrentConnectorId();
        TpchTableHandle lineitemTpchTableHandle = new TpchTableHandle("lineitem", 1.0);
        TpchTableHandle supplierTpchTableHandle = new TpchTableHandle("supplier", 1.0);
        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        TableHandle lineitemTableHandle = tableHandle(connectorId, lineitemTpchTableHandle);
        TableHandle supplierTableHandle = tableHandle(connectorId, supplierTpchTableHandle);
        TableHandle nationTableHandle = tableHandle(connectorId, nationTpchTableHandle);
        ColumnHandle lineitemSuppkeyColumn = new TpchColumnHandle("suppkey", BIGINT);
        ColumnHandle supplierSuppkeyColumn = new TpchColumnHandle("suppkey", BIGINT);
        ColumnHandle supplierNationkeyColumn = new TpchColumnHandle("nationkey", INTEGER);
        ColumnHandle nationNationkeyColumn = new TpchColumnHandle("nationkey", INTEGER);
        TableConstraint<ColumnHandle> lineitemSupplierFk = new ForeignKeyConstraint<>(
                Optional.of("fk_lineitem_supplier"),
                new LinkedHashSet<>(ImmutableList.of(lineitemSuppkeyColumn)),
                new SchemaTableName("sf1.0", "supplier"),
                new LinkedHashSet<>(ImmutableList.of("suppkey")),
                false,
                true,
                false);
        TableConstraint<ColumnHandle> supplierNationFk = new ForeignKeyConstraint<>(
                Optional.of("fk_supplier_nation"),
                new LinkedHashSet<>(ImmutableList.of(supplierNationkeyColumn)),
                new SchemaTableName("sf1.0", "nation"),
                new LinkedHashSet<>(ImmutableList.of("nationkey")),
                false,
                true,
                false);

        JoinNode join = (JoinNode) tester.assertThat(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata()), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression lineitemSuppkey = p.variable("l_suppkey", BIGINT);
                    VariableReferenceExpression supplierSuppkey = p.variable("s_suppkey", BIGINT);
                    VariableReferenceExpression supplierNationkey = p.variable("s_nationkey", INTEGER);
                    VariableReferenceExpression nationNationkey = p.variable("n_nationkey", INTEGER);
                    TableScanNode lineitem = p.tableScan(
                            lineitemTableHandle,
                            ImmutableList.of(lineitemSuppkey),
                            ImmutableMap.of(lineitemSuppkey, lineitemSuppkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            ImmutableList.<TableConstraint<ColumnHandle>>builder()
                                    .addAll(tester.getTableConstraints(lineitemTableHandle))
                                    .add(lineitemSupplierFk)
                                    .build());
                    TableScanNode supplier = p.tableScan(
                            supplierTableHandle,
                            ImmutableList.of(supplierSuppkey, supplierNationkey),
                            ImmutableMap.of(supplierSuppkey, supplierSuppkeyColumn, supplierNationkey, supplierNationkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            ImmutableList.<TableConstraint<ColumnHandle>>builder()
                                    .addAll(tester.getTableConstraints(supplierTableHandle))
                                    .add(supplierNationFk)
                                    .build());
                    TableScanNode nation = p.tableScan(
                            nationTableHandle,
                            ImmutableList.of(nationNationkey),
                            ImmutableMap.of(nationNationkey, nationNationkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(nationTableHandle));
                    JoinNode supplierNation = p.join(
                            INNER,
                            supplier,
                            p.filter(isNotNull(nationNationkey), nation),
                            new EquiJoinClause(supplierNationkey, nationNationkey));

                    return p.join(
                            INNER,
                            lineitem,
                            supplierNation,
                            new EquiJoinClause(lineitemSuppkey, supplierSuppkey));
                })
                .get();

        assertTrue(join.isLeftKeysCoveredByRightKeys());
        assertFalse(join.isRightKeysCoveredByLeftKeys());
    }

    @Test
    public void testAnnotatesProbeKeysCoveredByBuildKeysThroughSourceJoin()
    {
        ConnectorId connectorId = tester.getCurrentConnectorId();
        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        TpchTableHandle lineitemTpchTableHandle = new TpchTableHandle("lineitem", 1.0);
        TpchTableHandle customerTpchTableHandle = new TpchTableHandle("customer", 1.0);
        TableHandle ordersTableHandle = tableHandle(connectorId, ordersTpchTableHandle);
        TableHandle lineitemTableHandle = tableHandle(connectorId, lineitemTpchTableHandle);
        TableHandle customerTableHandle = tableHandle(connectorId, customerTpchTableHandle);
        ColumnHandle ordersOrderkeyColumn = new TpchColumnHandle("orderkey", BIGINT);
        ColumnHandle ordersCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        ColumnHandle lineitemOrderkeyColumn = new TpchColumnHandle("orderkey", BIGINT);
        ColumnHandle customerCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        TableConstraint<ColumnHandle> ordersCustomerFk = new ForeignKeyConstraint<>(
                Optional.of("fk_orders_customer"),
                new LinkedHashSet<>(ImmutableList.of(ordersCustkeyColumn)),
                new SchemaTableName("sf1.0", "customer"),
                new LinkedHashSet<>(ImmutableList.of("custkey")),
                false,
                true,
                false);

        tester.assertThat(ImmutableSet.of(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata())), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression ordersOrderkey = p.variable("o_orderkey", BIGINT);
                    VariableReferenceExpression ordersCustkey = p.variable("o_custkey", BIGINT);
                    VariableReferenceExpression lineitemOrderkey = p.variable("l_orderkey", BIGINT);
                    VariableReferenceExpression customerCustkey = p.variable("c_custkey", BIGINT);
                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersOrderkey, ordersCustkey),
                            ImmutableMap.of(ordersOrderkey, ordersOrderkeyColumn, ordersCustkey, ordersCustkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            ImmutableList.<TableConstraint<ColumnHandle>>builder()
                                    .addAll(tester.getTableConstraints(ordersTableHandle))
                                    .add(ordersCustomerFk)
                                    .build());
                    TableScanNode lineitem = p.tableScan(
                            lineitemTableHandle,
                            ImmutableList.of(lineitemOrderkey),
                            ImmutableMap.of(lineitemOrderkey, lineitemOrderkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(lineitemTableHandle));
                    TableScanNode customer = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustkey),
                            ImmutableMap.of(customerCustkey, customerCustkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(customerTableHandle));
                    JoinNode ordersLineitem = p.join(
                            INNER,
                            orders,
                            lineitem,
                            new EquiJoinClause(ordersOrderkey, lineitemOrderkey));

                    return p.join(
                            INNER,
                            ordersLineitem,
                            customer,
                            new EquiJoinClause(ordersCustkey, customerCustkey));
                })
                .matches(joinWithKeyProperties(
                        INNER,
                        ImmutableList.of(equiJoinClause("o_custkey", "c_custkey")),
                        false,
                        true,
                        false,
                        true,
                        true,
                        false,
                        join(
                                INNER,
                                ImmutableList.of(equiJoinClause("o_orderkey", "l_orderkey")),
                                tableScan("orders", ImmutableMap.of("o_orderkey", "orderkey", "o_custkey", "custkey")),
                                tableScan("lineitem", ImmutableMap.of("l_orderkey", "orderkey"))),
                        tableScan("customer", ImmutableMap.of("c_custkey", "custkey"))));
    }

    @Test
    public void testAnnotatesLeftJoinKeysUnique()
    {
        tester.assertThat(ImmutableSet.of(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata())), logicalPropertiesProvider)
                .on("SELECT * FROM customer c JOIN orders o ON c.custkey = o.custkey")
                .matches(anyTree(
                        joinWithKeyProperties(
                                INNER,
                                ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                true,
                                false,
                                true,
                                false,
                                tableScan("customer", ImmutableMap.of("custkey", "custkey")),
                                tableScan("orders", ImmutableMap.of("custkey_0", "custkey")))));
    }

    @Test
    public void testAnnotatesCompositeRightJoinKeysUnique()
    {
        tester.assertThat(ImmutableSet.of(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata())), logicalPropertiesProvider)
                .on("SELECT * FROM lineitem l JOIN partsupp ps ON l.partkey = ps.partkey AND l.suppkey = ps.suppkey")
                .matches(anyTree(
                        joinWithKeyProperties(
                                INNER,
                                ImmutableList.of(equiJoinClause("partkey", "partkey_0"), equiJoinClause("suppkey", "suppkey_0")),
                                false,
                                true,
                                false,
                                true,
                                tableScan("lineitem", ImmutableMap.of("partkey", "partkey", "suppkey", "suppkey")),
                                tableScan("partsupp", ImmutableMap.of("partkey_0", "partkey", "suppkey_0", "suppkey")))));
    }

    @Test
    public void testDoesNotAnnotatePartialCompositeJoinKeysUnique()
    {
        tester.assertThat(ImmutableSet.of(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata())), logicalPropertiesProvider)
                .on("SELECT * FROM lineitem l JOIN partsupp ps ON l.partkey = ps.partkey")
                .matches(anyTree(
                        joinWithKeyProperties(
                                INNER,
                                ImmutableList.of(equiJoinClause("partkey", "partkey_0")),
                                false,
                                false,
                                false,
                                true,
                                tableScan("lineitem", ImmutableMap.of("partkey", "partkey")),
                                tableScan("partsupp", ImmutableMap.of("partkey_0", "partkey")))));
    }

    @Test
    public void testAnnotatesNonNullKeysThroughExchange()
    {
        ConnectorId connectorId = tester.getCurrentConnectorId();
        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        TpchTableHandle customerTpchTableHandle = new TpchTableHandle("customer", 1.0);
        TableHandle ordersTableHandle = tableHandle(connectorId, ordersTpchTableHandle);
        TableHandle customerTableHandle = tableHandle(connectorId, customerTpchTableHandle);
        ColumnHandle ordersCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        ColumnHandle customerCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);

        JoinNode join = (JoinNode) tester.assertThat(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata()), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression ordersCustkey = p.variable("o_custkey", BIGINT);
                    VariableReferenceExpression customerCustkey = p.variable("c_custkey", BIGINT);
                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustkey),
                            ImmutableMap.of(ordersCustkey, ordersCustkeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester.getTableConstraints(ordersTableHandle));
                    TableScanNode customer = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustkey),
                            ImmutableMap.of(customerCustkey, customerCustkeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester.getTableConstraints(customerTableHandle));

                    return p.join(
                            INNER,
                            orders,
                            p.gatheringExchange(ExchangeNode.Scope.REMOTE_STREAMING, customer),
                            new EquiJoinClause(ordersCustkey, customerCustkey));
                })
                .get();

        assertFalse(join.isLeftKeysUnique());
        assertTrue(join.isRightKeysUnique());
        assertFalse(join.isLeftKeysNonNull());
        assertTrue(join.isRightKeysNonNull());
    }

    @Test
    public void testAnnotatesNonNullKeysThroughExchangeOverInnerJoin()
    {
        ConnectorId connectorId = tester.getCurrentConnectorId();
        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        TpchTableHandle supplierTpchTableHandle = new TpchTableHandle("supplier", 1.0);
        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        TableHandle ordersTableHandle = tableHandle(connectorId, ordersTpchTableHandle);
        TableHandle supplierTableHandle = tableHandle(connectorId, supplierTpchTableHandle);
        TableHandle nationTableHandle = tableHandle(connectorId, nationTpchTableHandle);
        ColumnHandle ordersCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        ColumnHandle supplierSuppkeyColumn = new TpchColumnHandle("suppkey", BIGINT);
        ColumnHandle supplierNationkeyColumn = new TpchColumnHandle("nationkey", INTEGER);
        ColumnHandle nationNationkeyColumn = new TpchColumnHandle("nationkey", INTEGER);

        JoinNode join = (JoinNode) tester.assertThat(new AnnotateJoinNodeWithUniqueKeys(tester.getMetadata()), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression ordersCustkey = p.variable("o_custkey", BIGINT);
                    VariableReferenceExpression supplierSuppkey = p.variable("s_suppkey", BIGINT);
                    VariableReferenceExpression supplierNationkey = p.variable("s_nationkey", INTEGER);
                    VariableReferenceExpression nationNationkey = p.variable("n_nationkey", INTEGER);
                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustkey),
                            ImmutableMap.of(ordersCustkey, ordersCustkeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester.getTableConstraints(ordersTableHandle));
                    TableScanNode supplier = p.tableScan(
                            supplierTableHandle,
                            ImmutableList.of(supplierSuppkey, supplierNationkey),
                            ImmutableMap.of(supplierSuppkey, supplierSuppkeyColumn, supplierNationkey, supplierNationkeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester.getTableConstraints(supplierTableHandle));
                    TableScanNode nation = p.tableScan(
                            nationTableHandle,
                            ImmutableList.of(nationNationkey),
                            ImmutableMap.of(nationNationkey, nationNationkeyColumn),
                            TupleDomain.none(),
                            TupleDomain.none(),
                            tester.getTableConstraints(nationTableHandle));
                    JoinNode supplierNation = p.join(
                            INNER,
                            supplier,
                            nation,
                            new EquiJoinClause(supplierNationkey, nationNationkey));

                    return p.join(
                            INNER,
                            orders,
                            p.exchange(exchange -> exchange
                                    .type(ExchangeNode.Type.REPLICATE)
                                    .scope(ExchangeNode.Scope.REMOTE_STREAMING)
                                    .singleDistributionPartitioningScheme(supplierNation.getOutputVariables())
                                    .addSource(supplierNation)
                                    .addInputsSet(supplierNation.getOutputVariables())),
                            new EquiJoinClause(ordersCustkey, supplierSuppkey));
                })
                .get();

        assertFalse(join.isLeftKeysUnique());
        assertTrue(join.isRightKeysUnique());
        assertFalse(join.isLeftKeysNonNull());
        assertTrue(join.isRightKeysNonNull());
    }

    @Test
    public void testAnnotatesSemiJoinKeys()
    {
        ConnectorId connectorId = tester.getCurrentConnectorId();
        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        TpchTableHandle customerTpchTableHandle = new TpchTableHandle("customer", 1.0);
        TableHandle ordersTableHandle = tableHandle(connectorId, ordersTpchTableHandle);
        TableHandle customerTableHandle = tableHandle(connectorId, customerTpchTableHandle);
        ColumnHandle ordersCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        ColumnHandle customerCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);

        SemiJoinNode semiJoin = (SemiJoinNode) tester.assertThat(new AnnotateSemiJoinNodeWithUniqueKeys(), logicalPropertiesProvider)
                .on(p -> {
                    VariableReferenceExpression ordersCustkey = p.variable("o_custkey", BIGINT);
                    VariableReferenceExpression customerCustkey = p.variable("c_custkey", BIGINT);
                    VariableReferenceExpression match = p.variable("match", BOOLEAN);
                    TableScanNode orders = p.tableScan(
                            ordersTableHandle,
                            ImmutableList.of(ordersCustkey),
                            ImmutableMap.of(ordersCustkey, ordersCustkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(ordersTableHandle));
                    TableScanNode customer = p.tableScan(
                            customerTableHandle,
                            ImmutableList.of(customerCustkey),
                            ImmutableMap.of(customerCustkey, customerCustkeyColumn),
                            TupleDomain.all(),
                            TupleDomain.all(),
                            tester.getTableConstraints(customerTableHandle));

                    return p.semiJoin(
                            ordersCustkey,
                            customerCustkey,
                            match,
                            Optional.empty(),
                            Optional.empty(),
                            orders,
                            customer);
                })
                .get();

        assertFalse(semiJoin.isSourceKeyUnique());
        assertTrue(semiJoin.isFilteringSourceKeyUnique());
        assertFalse(semiJoin.isSourceKeyNonNull());
        assertTrue(semiJoin.isFilteringSourceKeyNonNull());
    }

    @Test
    public void testFeatureDisabled()
    {
        try (RuleTester disabledTester = new RuleTester(emptyList(), ImmutableMap.of("exploit_constraints", Boolean.toString(false)), Optional.of(1), new TestTableConstraintsConnectorFactory(1))) {
            disabledTester.assertThat(ImmutableSet.of(new AnnotateJoinNodeWithUniqueKeys(disabledTester.getMetadata())), logicalPropertiesProvider)
                    .on("SELECT * FROM orders o JOIN customer c ON o.custkey = c.custkey")
                    .matches(anyTree(
                            joinWithKeyProperties(
                                    INNER,
                                    ImmutableList.of(equiJoinClause("custkey", "custkey_0")),
                                    false,
                                    false,
                                    false,
                                    false,
                                    tableScan("orders", ImmutableMap.of("custkey", "custkey")),
                                    tableScan("customer", ImmutableMap.of("custkey_0", "custkey")))));
        }
    }

    private static TableHandle tableHandle(ConnectorId connectorId, TpchTableHandle tpchTableHandle)
    {
        return new TableHandle(
                connectorId,
                tpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(tpchTableHandle, TupleDomain.all())));
    }

    private RowExpression isNotNull(VariableReferenceExpression variable)
    {
        FunctionResolution functionResolution = new FunctionResolution(tester.getMetadata().getFunctionAndTypeManager().getFunctionAndTypeResolver());
        return new CallExpression(
                variable.getSourceLocation(),
                "not",
                functionResolution.notFunction(),
                BOOLEAN,
                ImmutableList.of(new SpecialFormExpression(variable.getSourceLocation(), IS_NULL, BOOLEAN, variable)));
    }
}
