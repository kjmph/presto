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
import com.facebook.presto.cost.PlanNodeStatsEstimate;
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
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
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
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.IGNORE_STATS_CALCULATOR_FAILURES;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
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
    private static final String UNKNOWN_FACT_JOIN_ID = "unknown_fact_join";

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
        TopNNode outerFinal = (TopNNode) rewrittenPlan;
        assertEquals(outerFinal.getStep(), TopNNode.Step.FINAL);
        assertTrue(outerFinal.getSource() instanceof TopNNode);
        TopNNode outerPartial = (TopNNode) outerFinal.getSource();
        assertEquals(outerPartial.getStep(), TopNNode.Step.PARTIAL);
        assertTrue(outerPartial.getSource() instanceof JoinNode);
        JoinNode rewrittenJoin = (JoinNode) outerPartial.getSource();
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
        TopNNode innerFinal = (TopNNode) rewrittenBase;
        assertEquals(innerFinal.getStep(), TopNNode.Step.FINAL);
        assertTrue(innerFinal.getSource() instanceof TopNNode);
        assertEquals(((TopNNode) innerFinal.getSource()).getStep(), TopNNode.Step.PARTIAL);
    }

    @Test
    public void testCompleteTopNPushdownIsIdempotent()
    {
        PushTopNThroughCardinalityPreservingJoin rule = new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata());
        tester.assertThat(rule)
                .on(p -> directLookupTopNWithCompletePushdown(p, DOUBLE))
                .doesNotFire();
    }

    @Test
    public void testPartialTopNPushdownRemainsPartial()
    {
        PlanNode rewrittenPlan = tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(p -> ((TopNNode) directLookupTopN(p, false, Optional.empty(), DOUBLE)).getSource())
                .get();

        assertTrue(rewrittenPlan instanceof TopNNode);
        TopNNode outerPartial = (TopNNode) rewrittenPlan;
        assertEquals(outerPartial.getStep(), TopNNode.Step.PARTIAL);
        assertTrue(outerPartial.getSource() instanceof JoinNode);
        JoinNode rewrittenJoin = (JoinNode) outerPartial.getSource();
        assertTrue(rewrittenJoin.getLeft() instanceof TopNNode);
        assertEquals(((TopNNode) rewrittenJoin.getLeft()).getStep(), TopNNode.Step.PARTIAL);
    }

    @Test
    public void testCompletePushdownReusesExistingPartialTopN()
    {
        PlanNode rewrittenPlan = tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(p -> directLookupTopNWithPartialPushdown(p, DOUBLE))
                .get();

        TopNNode outerFinal = (TopNNode) rewrittenPlan;
        TopNNode outerPartial = (TopNNode) outerFinal.getSource();
        JoinNode rewrittenJoin = (JoinNode) outerPartial.getSource();
        TopNNode innerFinal = (TopNNode) rewrittenJoin.getLeft();
        assertEquals(innerFinal.getStep(), TopNNode.Step.FINAL);
        assertTrue(innerFinal.getSource() instanceof TopNNode);
        TopNNode innerPartial = (TopNNode) innerFinal.getSource();
        assertEquals(innerPartial.getStep(), TopNNode.Step.PARTIAL);
        assertFalse(innerPartial.getSource() instanceof TopNNode);
    }

    @Test
    public void testCompletePushdownDoesNotDuplicateSingleTopN()
    {
        tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(p -> directLookupTopNWithSinglePushdown(p, DOUBLE))
                .doesNotFire();
    }

    @Test(dataProvider = "orderingTypes")
    public void testNestedCompleteTopNPushdown(Type orderingType)
    {
        PlanNode rewrittenPlan = tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(p -> nestedLookupTopN(p, orderingType))
                .get();

        TopNNode outerFinal = (TopNNode) rewrittenPlan;
        assertEquals(outerFinal.getStep(), TopNNode.Step.FINAL);
        TopNNode outerPartial = (TopNNode) outerFinal.getSource();
        assertEquals(outerPartial.getStep(), TopNNode.Step.PARTIAL);
        JoinNode lookupJoin = (JoinNode) outerPartial.getSource();
        assertTrue(lookupJoin.getRight() instanceof TableScanNode);
        assertEquals(((TableScanNode) lookupJoin.getRight()).getTable(), customerTableHandle);

        TopNNode innerFinal = (TopNNode) lookupJoin.getLeft();
        assertEquals(innerFinal.getStep(), TopNNode.Step.FINAL);
        TopNNode innerPartial = (TopNNode) innerFinal.getSource();
        assertEquals(innerPartial.getStep(), TopNNode.Step.PARTIAL);
        assertTrue(innerPartial.getSource() instanceof JoinNode);
    }

    @Test(dataProvider = "orderingTypes")
    public void testCompleteTopNPushdownThroughProjectionBetweenFinalAndPartial(Type orderingType)
    {
        PlanNode rewrittenPlan = tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(p -> projectedCompleteTopN(p, orderingType, false))
                .get();

        TopNNode outerFinal = (TopNNode) rewrittenPlan;
        assertEquals(outerFinal.getStep(), TopNNode.Step.FINAL);
        assertTrue(outerFinal.getSource() instanceof ProjectNode);
        ProjectNode projection = (ProjectNode) outerFinal.getSource();
        assertTrue(projection.getAssignments().getExpressions().stream()
                .anyMatch(ConstantExpression.class::isInstance));

        TopNNode outerPartial = (TopNNode) projection.getSource();
        assertEquals(outerPartial.getStep(), TopNNode.Step.PARTIAL);
        JoinNode lookupJoin = (JoinNode) outerPartial.getSource();
        assertTrue(lookupJoin.getRight() instanceof TableScanNode);
        assertEquals(((TableScanNode) lookupJoin.getRight()).getTable(), customerTableHandle);

        TopNNode innerFinal = (TopNNode) lookupJoin.getLeft();
        assertEquals(innerFinal.getStep(), TopNNode.Step.FINAL);
        assertTrue(innerFinal.getSource() instanceof TopNNode);
        TopNNode innerPartial = (TopNNode) innerFinal.getSource();
        assertEquals(innerPartial.getStep(), TopNNode.Step.PARTIAL);
        assertEquals(innerFinal.getOrderingScheme(), innerPartial.getOrderingScheme());
        assertEquals(innerFinal.getOrderingScheme().getOrderBy().size(), 2);
        assertEquals(innerFinal.getOrderingScheme().getOrderBy().get(0).getSortOrder(), DESC_NULLS_LAST);
        assertEquals(innerFinal.getOrderingScheme().getOrderBy().get(1).getSortOrder(), ASC_NULLS_LAST);
    }

    @Test
    public void testIterativeOptimizerCompletesProjectedPairToFixedPoint()
    {
        tester.assertThat(ImmutableSet.of(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata())))
                .on(p -> projectedCompleteTopN(p, DOUBLE, false))
                .validates(plan -> {
                    TopNNode outerFinal = (TopNNode) plan.getRoot();
                    ProjectNode projection = (ProjectNode) outerFinal.getSource();
                    TopNNode outerPartial = (TopNNode) projection.getSource();
                    JoinNode lookupJoin = (JoinNode) outerPartial.getSource();
                    TopNNode innerFinal = (TopNNode) lookupJoin.getLeft();
                    assertEquals(innerFinal.getStep(), TopNNode.Step.FINAL);
                    assertTrue(innerFinal.getSource() instanceof TopNNode);
                    assertEquals(((TopNNode) innerFinal.getSource()).getStep(), TopNNode.Step.PARTIAL);
                });
    }

    @Test
    public void testCompleteTopNPushdownRejectsComputedOrderingProjection()
    {
        tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(p -> projectedCompleteTopN(p, DOUBLE, true))
                .doesNotFire();
    }

    @Test
    public void testCompleteTopNPushdownRejectsDuplicateProjectedOrderingVariables()
    {
        tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(this::duplicateProjectedOrderingTopN)
                .doesNotFire();
    }

    @Test
    public void testMismatchedFinalPartialPairDoesNotFire()
    {
        tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(p -> {
                    TopNNode finalTopN = (TopNNode) directLookupTopN(p, false, Optional.empty(), DOUBLE);
                    return new TopNNode(
                            finalTopN.getSourceLocation(),
                            p.getIdAllocator().getNextId(),
                            finalTopN.getSource(),
                            finalTopN.getCount() + 1,
                            finalTopN.getOrderingScheme(),
                            TopNNode.Step.FINAL);
                })
                .doesNotFire();
    }

    @Test(dataProvider = "orderingTypes")
    public void testSingleTopNPushdownRemainsLogical(Type orderingType)
    {
        PlanNode rewrittenPlan = tester.assertThat(new PushTopNThroughCardinalityPreservingJoin(tester.getMetadata()))
                .on(p -> {
                    TopNNode finalTopN = (TopNNode) directLookupTopN(p, false, Optional.empty(), orderingType);
                    TopNNode partialTopN = (TopNNode) finalTopN.getSource();
                    return new TopNNode(
                            partialTopN.getSourceLocation(),
                            p.getIdAllocator().getNextId(),
                            partialTopN.getSource(),
                            partialTopN.getCount(),
                            partialTopN.getOrderingScheme(),
                            TopNNode.Step.SINGLE);
                })
                .get();

        assertTrue(rewrittenPlan instanceof TopNNode);
        TopNNode outerTopN = (TopNNode) rewrittenPlan;
        assertEquals(outerTopN.getStep(), TopNNode.Step.SINGLE);
        JoinNode rewrittenJoin = (JoinNode) outerTopN.getSource();
        assertTrue(rewrittenJoin.getLeft() instanceof TopNNode);
        assertEquals(((TopNNode) rewrittenJoin.getLeft()).getStep(), TopNNode.Step.SINGLE);
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
                .overrideStats(UNKNOWN_FACT_JOIN_ID, PlanNodeStatsEstimate.unknown())
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
        TopNNode originalFinal = (TopNNode) directLookupTopN(planBuilder, false, Optional.empty(), orderingType);
        TopNNode originalPartial = (TopNNode) originalFinal.getSource();
        JoinNode originalJoin = (JoinNode) originalPartial.getSource();
        PlanNode unknownStatsSource = new JoinNode(
                Optional.empty(),
                new PlanNodeId(UNKNOWN_FACT_JOIN_ID),
                INNER,
                originalJoin.getLeft(),
                planBuilder.values(1),
                ImmutableList.of(),
                originalJoin.getLeft().getOutputVariables(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of());
        TopNNode pushedPartial = new TopNNode(
                originalPartial.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                unknownStatsSource,
                originalPartial.getCount(),
                originalPartial.getOrderingScheme(),
                originalPartial.getStep());
        TopNNode pushedFinal = new TopNNode(
                originalFinal.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                pushedPartial,
                originalFinal.getCount(),
                originalFinal.getOrderingScheme(),
                originalFinal.getStep());

        return new JoinNode(
                originalJoin.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                originalJoin.getType(),
                pushedFinal,
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

    private PlanNode directLookupTopNWithPartialPushdown(PlanBuilder planBuilder, Type orderingType)
    {
        TopNNode originalFinal = (TopNNode) directLookupTopN(planBuilder, false, Optional.empty(), orderingType);
        TopNNode originalPartial = (TopNNode) originalFinal.getSource();
        JoinNode originalJoin = (JoinNode) originalPartial.getSource();
        TopNNode pushedPartial = new TopNNode(
                originalPartial.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                originalJoin.getLeft(),
                originalPartial.getCount(),
                originalPartial.getOrderingScheme(),
                TopNNode.Step.PARTIAL);
        JoinNode lookupJoin = replaceLeft(planBuilder, originalJoin, pushedPartial);
        return topNPair(planBuilder, lookupJoin, originalPartial.getCount(), originalPartial.getOrderingScheme());
    }

    private PlanNode nestedLookupTopN(PlanBuilder planBuilder, Type orderingType)
    {
        TopNNode directFinal = (TopNNode) directLookupTopN(planBuilder, false, Optional.empty(), orderingType);
        TopNNode directPartial = (TopNNode) directFinal.getSource();
        JoinNode nestedLookup = (JoinNode) directPartial.getSource();
        VariableReferenceExpression factKey = planBuilder.variable("fact_key", BIGINT);
        VariableReferenceExpression ordersKey = nestedLookup.getCriteria().get(0).getLeft();
        JoinNode rootJoin = planBuilder.join(
                INNER,
                planBuilder.values(1, factKey),
                nestedLookup,
                new EquiJoinClause(factKey, ordersKey));
        return topNPair(
                planBuilder,
                rootJoin,
                directPartial.getCount(),
                directPartial.getOrderingScheme());
    }

    private PlanNode projectedCompleteTopN(PlanBuilder planBuilder, Type orderingType, boolean computedOrdering)
    {
        TopNNode originalFinal = (TopNNode) directLookupTopN(planBuilder, false, Optional.empty(), orderingType);
        TopNNode partialTopN = (TopNNode) originalFinal.getSource();
        List<Ordering> partialOrderings = partialTopN.getOrderingScheme().getOrderBy();
        VariableReferenceExpression orderingVariable = partialOrderings.get(0).getVariable();
        VariableReferenceExpression orderingAlias = planBuilder.variable("ordering_alias", orderingType);
        VariableReferenceExpression payload = planBuilder.variable("payload", BIGINT);

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression output : partialTopN.getOutputVariables()) {
            if (!output.equals(orderingVariable)) {
                assignments.put(output, output);
            }
        }
        assignments.put(
                orderingAlias,
                computedOrdering ?
                        new ConstantExpression(orderingType.equals(DOUBLE) ? (Object) 1.0 : (Object) 100L, orderingType) :
                        orderingVariable);
        assignments.put(payload, new ConstantExpression(1L, BIGINT));
        ProjectNode projection = planBuilder.project(partialTopN, assignments.build());

        ImmutableList.Builder<Ordering> projectedOrderings = ImmutableList.builder();
        projectedOrderings.add(new Ordering(orderingAlias, partialOrderings.get(0).getSortOrder()));
        projectedOrderings.addAll(partialOrderings.subList(1, partialOrderings.size()));
        return new TopNNode(
                originalFinal.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                projection,
                originalFinal.getCount(),
                new OrderingScheme(projectedOrderings.build()),
                TopNNode.Step.FINAL);
    }

    private PlanNode duplicateProjectedOrderingTopN(PlanBuilder planBuilder)
    {
        TopNNode originalFinal = (TopNNode) directLookupTopN(planBuilder, false, Optional.empty(), DOUBLE);
        TopNNode partialTopN = (TopNNode) originalFinal.getSource();
        VariableReferenceExpression orderingVariable = partialTopN.getOrderingScheme().getOrderByVariables().get(0);
        VariableReferenceExpression firstAlias = planBuilder.variable("first_ordering_alias", DOUBLE);
        VariableReferenceExpression secondAlias = planBuilder.variable("second_ordering_alias", DOUBLE);

        Assignments.Builder assignments = Assignments.builder();
        for (VariableReferenceExpression output : partialTopN.getOutputVariables()) {
            if (!output.equals(orderingVariable)) {
                assignments.put(output, output);
            }
        }
        assignments.put(firstAlias, orderingVariable);
        assignments.put(secondAlias, orderingVariable);
        ProjectNode projection = planBuilder.project(partialTopN, assignments.build());

        return new TopNNode(
                originalFinal.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                projection,
                originalFinal.getCount(),
                new OrderingScheme(ImmutableList.of(
                        new Ordering(firstAlias, DESC_NULLS_LAST),
                        new Ordering(secondAlias, DESC_NULLS_LAST))),
                TopNNode.Step.FINAL);
    }

    private PlanNode directLookupTopNWithSinglePushdown(PlanBuilder planBuilder, Type orderingType)
    {
        TopNNode originalFinal = (TopNNode) directLookupTopN(planBuilder, false, Optional.empty(), orderingType);
        TopNNode originalPartial = (TopNNode) originalFinal.getSource();
        JoinNode originalJoin = (JoinNode) originalPartial.getSource();
        TopNNode pushedSingle = new TopNNode(
                originalPartial.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                originalJoin.getLeft(),
                originalPartial.getCount(),
                originalPartial.getOrderingScheme(),
                TopNNode.Step.SINGLE);
        JoinNode lookupJoin = replaceLeft(planBuilder, originalJoin, pushedSingle);
        return topNPair(planBuilder, lookupJoin, originalPartial.getCount(), originalPartial.getOrderingScheme());
    }

    private PlanNode directLookupTopNWithCompletePushdown(PlanBuilder planBuilder, Type orderingType)
    {
        JoinNode lookupJoin = directLookupJoinWithPushedTopN(planBuilder, orderingType);
        TopNNode innerFinal = (TopNNode) lookupJoin.getLeft();
        return topNPair(planBuilder, lookupJoin, innerFinal.getCount(), innerFinal.getOrderingScheme());
    }

    private PlanNode topNPair(
            PlanBuilder planBuilder,
            PlanNode source,
            long count,
            OrderingScheme orderingScheme)
    {
        TopNNode outerPartial = new TopNNode(
                Optional.empty(),
                planBuilder.getIdAllocator().getNextId(),
                source,
                count,
                orderingScheme,
                TopNNode.Step.PARTIAL);
        return new TopNNode(
                Optional.empty(),
                planBuilder.getIdAllocator().getNextId(),
                outerPartial,
                outerPartial.getCount(),
                outerPartial.getOrderingScheme(),
                TopNNode.Step.FINAL);
    }

    private JoinNode replaceLeft(PlanBuilder planBuilder, JoinNode join, PlanNode left)
    {
        return new JoinNode(
                join.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                join.getType(),
                left,
                join.getRight(),
                join.getCriteria(),
                join.getOutputVariables(),
                join.getFilter(),
                join.getLeftHashVariable(),
                join.getRightHashVariable(),
                join.getDistributionType(),
                join.getDynamicFilters(),
                join.isLeftKeysUnique(),
                join.isRightKeysUnique(),
                join.isLeftKeysNonNull(),
                join.isRightKeysNonNull(),
                join.isLeftKeysCoveredByRightKeys(),
                join.isRightKeysCoveredByLeftKeys());
    }

    private PlanNode directLookupTopN(
            PlanBuilder planBuilder,
            boolean lookupOnLeft,
            Optional<JoinDistributionType> distributionType,
            Type orderingType)
    {
        VariableReferenceExpression oCustkey = planBuilder.variable("o_custkey", BIGINT);
        VariableReferenceExpression sortKey = planBuilder.variable("sort_key", orderingType);
        VariableReferenceExpression sortTieKey = planBuilder.variable("sort_tie_key", BIGINT);
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
                        .put(sortTieKey, new ConstantExpression(1L, BIGINT))
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

        TopNNode partialTopN = new TopNNode(
                Optional.empty(),
                planBuilder.getIdAllocator().getNextId(),
                lookupJoin,
                100,
                new OrderingScheme(ImmutableList.of(
                        new Ordering(sortKey, DESC_NULLS_LAST),
                        new Ordering(sortTieKey, ASC_NULLS_LAST))),
                TopNNode.Step.PARTIAL);
        return new TopNNode(
                Optional.empty(),
                planBuilder.getIdAllocator().getNextId(),
                partialTopN,
                partialTopN.getCount(),
                partialTopN.getOrderingScheme(),
                TopNNode.Step.FINAL);
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
