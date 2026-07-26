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

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.TestingColumnHandle;
import com.facebook.presto.spi.constraints.NotNullConstraint;
import com.facebook.presto.spi.constraints.TableConstraint;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.spi.plan.JoinType.LEFT;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identityAssignments;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestTransformCountOverPairedNotEqualExistsToGroupedAggregation
        extends BaseRuleTest
{
    @Test
    public void testEnabledByPushAggregationThroughJoin()
    {
        TransformCountOverPairedNotEqualExistsToGroupedAggregation rule =
                new TransformCountOverPairedNotEqualExistsToGroupedAggregation(getMetadata().getFunctionAndTypeManager());

        assertTrue(rule.isEnabled(session(true)));
        assertFalse(rule.isEnabled(session(false)));
    }

    @Test
    public void testRewritesThroughConsecutiveIdentityProjects()
    {
        rule()
                .on(planBuilder -> pairedExistsPlan(planBuilder, false, 2))
                .get();
    }

    @Test
    public void testRejectsMismatchedMinMaxArgumentsWhenMaxAppearsFirst()
    {
        rule()
                .on(planBuilder -> pairedExistsPlan(planBuilder, true, 0))
                .doesNotFire();
    }

    private RuleAssert rule()
    {
        return tester().assertThat(new TransformCountOverPairedNotEqualExistsToGroupedAggregation(
                        getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true");
    }

    private PlanNode pairedExistsPlan(PlanBuilder planBuilder, boolean mismatchedAllMaxArgument, int identityProjectCount)
    {
        TestingColumnHandle keyColumn = new TestingColumnHandle("key");
        TestingColumnHandle valueColumn = new TestingColumnHandle("value");
        TestingColumnHandle otherColumn = new TestingColumnHandle("other");
        TestingColumnHandle receiptColumn = new TestingColumnHandle("receipt");
        TestingColumnHandle commitColumn = new TestingColumnHandle("commit");
        List<TableConstraint<ColumnHandle>> constraints = ImmutableList.of(
                new NotNullConstraint<>(keyColumn),
                new NotNullConstraint<>(valueColumn));

        VariableReferenceExpression seedKey = planBuilder.variable("seed_key", BIGINT);
        TableScanNode seed = planBuilder.tableScan(
                ImmutableList.of(seedKey),
                ImmutableMap.of(seedKey, keyColumn));

        VariableReferenceExpression allKey = planBuilder.variable("all_key", BIGINT);
        VariableReferenceExpression allValue = planBuilder.variable("all_value", BIGINT);
        VariableReferenceExpression allOther = planBuilder.variable("all_other", BIGINT);
        TableScanNode allScan = planBuilder.tableScan(
                seed.getTable(),
                ImmutableList.of(allKey, allValue, allOther),
                ImmutableMap.of(
                        allKey, keyColumn,
                        allValue, valueColumn,
                        allOther, otherColumn),
                TupleDomain.all(),
                TupleDomain.all(),
                constraints);
        VariableReferenceExpression allMin = planBuilder.variable("all_min", BIGINT);
        VariableReferenceExpression allMax = planBuilder.variable("all_max", BIGINT);
        PlanNode allFacts = planBuilder.aggregation(aggregation -> aggregation
                .source(allScan)
                // MAX must be first to guard against order-dependent argument validation.
                .addAggregation(allMax, planBuilder.rowExpression("max(" +
                        (mismatchedAllMaxArgument ? "all_other" : "all_value") + ")"))
                .addAggregation(allMin, planBuilder.rowExpression("min(all_value)"))
                .singleGroupingSet(allKey));
        allFacts = wrapInIdentityProjects(planBuilder, allFacts, identityProjectCount);

        VariableReferenceExpression lateKey = planBuilder.variable("late_key", BIGINT);
        VariableReferenceExpression lateValue = planBuilder.variable("late_value", BIGINT);
        VariableReferenceExpression lateReceipt = planBuilder.variable("late_receipt", BIGINT);
        VariableReferenceExpression lateCommit = planBuilder.variable("late_commit", BIGINT);
        TableScanNode lateScan = planBuilder.tableScan(
                seed.getTable(),
                ImmutableList.of(lateKey, lateValue, lateReceipt, lateCommit),
                ImmutableMap.of(
                        lateKey, keyColumn,
                        lateValue, valueColumn,
                        lateReceipt, receiptColumn,
                        lateCommit, commitColumn),
                TupleDomain.all(),
                TupleDomain.all(),
                constraints);
        VariableReferenceExpression lateMin = planBuilder.variable("late_min", BIGINT);
        VariableReferenceExpression lateMax = planBuilder.variable("late_max", BIGINT);
        PlanNode lateFacts = planBuilder.aggregation(aggregation -> aggregation
                .source(planBuilder.filter(planBuilder.rowExpression("late_receipt > late_commit"), lateScan))
                .addAggregation(lateMax, planBuilder.rowExpression("max(late_value)"))
                .addAggregation(lateMin, planBuilder.rowExpression("min(late_value)"))
                .singleGroupingSet(lateKey));
        lateFacts = wrapInIdentityProjects(planBuilder, lateFacts, identityProjectCount);

        VariableReferenceExpression outerKey = planBuilder.variable("outer_key", BIGINT);
        VariableReferenceExpression outerValue = planBuilder.variable("outer_value", BIGINT);
        VariableReferenceExpression outerReceipt = planBuilder.variable("outer_receipt", BIGINT);
        VariableReferenceExpression outerCommit = planBuilder.variable("outer_commit", BIGINT);
        TableScanNode outerScan = planBuilder.tableScan(
                seed.getTable(),
                ImmutableList.of(outerKey, outerValue, outerReceipt, outerCommit),
                ImmutableMap.of(
                        outerKey, keyColumn,
                        outerValue, valueColumn,
                        outerReceipt, receiptColumn,
                        outerCommit, commitColumn),
                TupleDomain.all(),
                TupleDomain.all(),
                constraints);
        PlanNode outerFact = planBuilder.filter(
                planBuilder.rowExpression("outer_receipt > outer_commit"),
                outerScan);

        PlanNode existsJoin = planBuilder.join(
                INNER,
                outerFact,
                allFacts,
                planBuilder.rowExpression("coalesce(all_min <> outer_value OR all_max <> outer_value, false)"),
                new EquiJoinClause(outerKey, allKey));
        PlanNode notExistsJoin = planBuilder.join(
                LEFT,
                existsJoin,
                lateFacts,
                new EquiJoinClause(outerKey, lateKey));
        PlanNode filtered = planBuilder.filter(
                planBuilder.rowExpression("NOT coalesce(late_min <> outer_value OR late_max <> outer_value, false)"),
                notExistsJoin);
        PlanNode projected = planBuilder.project(
                filtered,
                Assignments.of(outerKey, outerKey, outerValue, outerValue));

        VariableReferenceExpression count = planBuilder.variable("count", BIGINT);
        return planBuilder.aggregation(aggregation -> aggregation
                .source(projected)
                .addAggregation(count, planBuilder.rowExpression("count()"))
                .globalGrouping());
    }

    private static PlanNode wrapInIdentityProjects(PlanBuilder planBuilder, PlanNode source, int count)
    {
        PlanNode result = source;
        for (int i = 0; i < count; i++) {
            result = planBuilder.project(result, identityAssignments(result.getOutputVariables()));
        }
        return result;
    }

    private Session session(boolean pushAggregationThroughJoin)
    {
        return Session.builder(tester().getSession())
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, String.valueOf(pushAggregationThroughJoin))
                .build();
    }
}
