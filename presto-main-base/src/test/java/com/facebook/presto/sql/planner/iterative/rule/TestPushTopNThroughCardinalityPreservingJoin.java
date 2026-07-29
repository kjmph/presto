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
import com.facebook.presto.cost.CostComparator;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.constraints.ForeignKeyConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
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
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.common.block.SortOrder.DESC_NULLS_LAST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DecimalType.createDecimalType;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static java.util.Collections.emptyList;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestPushTopNThroughCardinalityPreservingJoin
        extends BaseRuleTest
{
    private TableHandle ordersTableHandle;
    private TableHandle customerTableHandle;
    private ColumnHandle ordersCustkeyColumn;
    private ColumnHandle customerCustkeyColumn;

    @Override
    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(emptyList(), ImmutableMap.of(), Optional.of(1), new TestTableConstraintsConnectorFactory(1));
        ConnectorId connectorId = tester().getCurrentConnectorId();

        TpchTableHandle ordersTpchTableHandle = new TpchTableHandle("orders", 1.0);
        TpchTableHandle customerTpchTableHandle = new TpchTableHandle("customer", 1.0);
        ordersTableHandle = tableHandle(connectorId, ordersTpchTableHandle);
        customerTableHandle = tableHandle(connectorId, customerTpchTableHandle);
        ordersCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
        customerCustkeyColumn = new TpchColumnHandle("custkey", BIGINT);
    }

    @DataProvider(name = "directLookupJoinCases")
    public Object[][] directLookupJoinCases()
    {
        return new Object[][] {
                {true, Optional.of(PARTITIONED), DOUBLE},
                {false, Optional.of(PARTITIONED), createDecimalType(12, 2)},
                {true, Optional.of(REPLICATED), createDecimalType(12, 2)},
                {false, Optional.of(REPLICATED), DOUBLE},
                {true, Optional.empty(), DOUBLE},
                {false, Optional.empty(), createDecimalType(12, 2)},
        };
    }

    @Test(dataProvider = "directLookupJoinCases")
    public void testDirectTopNInvalidatesDistributionAndPreservesLogicalMetadata(
            boolean lookupOnLeft,
            Optional<JoinDistributionType> distributionType,
            Type orderingType)
    {
        PlanNode rewrittenPlan = tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(p -> directLookupTopN(p, lookupOnLeft, distributionType, orderingType))
                .get();

        assertTrue(rewrittenPlan instanceof TopNNode);
        assertTrue(((TopNNode) rewrittenPlan).getSource() instanceof JoinNode);
        JoinNode rewrittenJoin = (JoinNode) ((TopNNode) rewrittenPlan).getSource();
        assertEquals(rewrittenJoin.getDistributionType(), Optional.empty());
        assertTrue(rewrittenJoin.getDynamicFilters().isEmpty());
        assertEquals(rewrittenJoin.isLeftKeysUnique(), lookupOnLeft);
        assertEquals(rewrittenJoin.isRightKeysUnique(), !lookupOnLeft);
        assertTrue(rewrittenJoin.isLeftKeysNonNull());
        assertTrue(rewrittenJoin.isRightKeysNonNull());
        assertEquals(rewrittenJoin.isLeftKeysCoveredByRightKeys(), !lookupOnLeft);
        assertEquals(rewrittenJoin.isRightKeysCoveredByLeftKeys(), lookupOnLeft);

        PlanNode rewrittenBase = lookupOnLeft ? rewrittenJoin.getRight() : rewrittenJoin.getLeft();
        assertTrue(rewrittenBase instanceof TopNNode);
        assertEquals(((TopNNode) rewrittenBase).getStep(), TopNNode.Step.PARTIAL);
    }

    @DataProvider(name = "orderingTypes")
    public Object[][] orderingTypes()
    {
        return new Object[][] {
                {DOUBLE},
                {createDecimalType(12, 2)},
        };
    }

    @Test(dataProvider = "orderingTypes")
    public void testDirectTopNIsRecostedAsReplicatedBuild(Type orderingType)
    {
        PlanNode distributedPlan = tester.assertThat(new DetermineJoinDistributionType(
                        new CostComparator(1, 1, 1),
                        new TaskCountEstimator(() -> 4)))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "AUTOMATIC")
                .setSystemProperty(IGNORE_STATS_CALCULATOR_FAILURES, "false")
                .on(p -> {
                    JoinNode rewrittenJoin = directLookupJoinWithPushedTopN(p, orderingType);
                    assertEquals(rewrittenJoin.getDistributionType(), Optional.empty());
                    assertTrue(rewrittenJoin.getLeft() instanceof TopNNode);
                    assertTrue(rewrittenJoin.getRight() instanceof TableScanNode);
                    assertEquals(((TableScanNode) rewrittenJoin.getRight()).getTable(), customerTableHandle);
                    return rewrittenJoin;
                })
                .get();

        assertTrue(distributedPlan instanceof JoinNode);
        JoinNode join = (JoinNode) distributedPlan;
        assertEquals(join.getDistributionType(), Optional.of(REPLICATED));
        assertTrue(join.getLeft().getOutputVariables().stream()
                .anyMatch(variable -> variable.getName().equals("c_custkey")));
        assertTrue(join.getRight().getOutputVariables().stream()
                .anyMatch(variable -> variable.getName().equals("sort_key")));
        assertTrue(join.isLeftKeysUnique());
        assertFalse(join.isRightKeysUnique());
        assertTrue(join.isLeftKeysNonNull());
        assertTrue(join.isRightKeysNonNull());
        assertFalse(join.isLeftKeysCoveredByRightKeys());
        assertTrue(join.isRightKeysCoveredByLeftKeys());
    }

    private JoinNode directLookupJoinWithPushedTopN(PlanBuilder planBuilder, Type orderingType)
    {
        TopNNode originalTopN = (TopNNode) directLookupTopN(planBuilder, false, Optional.empty(), orderingType);
        JoinNode originalJoin = (JoinNode) originalTopN.getSource();
        TopNNode pushedTopN = new TopNNode(
                originalTopN.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                originalJoin.getLeft(),
                originalTopN.getCount(),
                originalTopN.getOrderingScheme(),
                originalTopN.getStep());

        return new JoinNode(
                originalJoin.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                originalJoin.getType(),
                pushedTopN,
                originalJoin.getRight(),
                originalJoin.getCriteria(),
                originalJoin.getOutputVariables(),
                originalJoin.getFilter(),
                originalJoin.getLeftHashVariable(),
                originalJoin.getRightHashVariable(),
                Optional.empty(),
                ImmutableMap.of(),
                false,
                true,
                true,
                true,
                true,
                false);
    }

    private PlanNode directLookupTopN(
            PlanBuilder planBuilder,
            boolean lookupOnLeft,
            Optional<JoinDistributionType> distributionType,
            Type orderingType)
    {
        VariableReferenceExpression oCustkey = planBuilder.variable("o_custkey", BIGINT);
        VariableReferenceExpression sortKey = planBuilder.variable("sort_key", orderingType);
        VariableReferenceExpression cCustkey = planBuilder.variable("c_custkey", BIGINT);

        TableScanNode orders = planBuilder.tableScan(
                ordersTableHandle,
                ImmutableList.of(oCustkey),
                ImmutableMap.of(oCustkey, ordersCustkeyColumn),
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
        PlanNode ordersWithSortKey = planBuilder.project(
                orders,
                Assignments.builder()
                        .put(oCustkey, oCustkey)
                        .put(sortKey, new ConstantExpression(
                                orderingType.equals(DOUBLE) ? (Object) 1.0 : (Object) 100L,
                                orderingType))
                        .build());
        TableScanNode customer = planBuilder.tableScan(
                customerTableHandle,
                ImmutableList.of(cCustkey),
                ImmutableMap.of(cCustkey, customerCustkeyColumn),
                TupleDomain.all(),
                TupleDomain.all(),
                tester.getTableConstraints(customerTableHandle));

        PlanNode left = lookupOnLeft ? customer : ordersWithSortKey;
        PlanNode right = lookupOnLeft ? ordersWithSortKey : customer;
        VariableReferenceExpression leftKey = lookupOnLeft ? cCustkey : oCustkey;
        VariableReferenceExpression rightKey = lookupOnLeft ? oCustkey : cCustkey;
        JoinNode lookupJoin = new JoinNode(
                Optional.empty(),
                planBuilder.getIdAllocator().getNextId(),
                INNER,
                left,
                right,
                ImmutableList.of(new EquiJoinClause(leftKey, rightKey)),
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(left.getOutputVariables())
                        .addAll(right.getOutputVariables())
                        .build(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                distributionType,
                ImmutableMap.of(),
                lookupOnLeft,
                !lookupOnLeft,
                true,
                true,
                true,
                true);

        return new TopNNode(
                Optional.empty(),
                planBuilder.getIdAllocator().getNextId(),
                lookupJoin,
                100,
                new OrderingScheme(ImmutableList.of(new Ordering(sortKey, DESC_NULLS_LAST))),
                TopNNode.Step.PARTIAL);
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
}
