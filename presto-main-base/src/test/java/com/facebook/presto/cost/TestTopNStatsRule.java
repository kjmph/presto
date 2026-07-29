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
package com.facebook.presto.cost;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_LAST;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.plan.TopNNode.Step.FINAL;
import static com.facebook.presto.spi.plan.TopNNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.TopNNode.Step.SINGLE;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.HIGH;

public class TestTopNStatsRule
        extends BaseStatsCalculatorTest
{
    private static final VariableReferenceExpression ORDERING_VARIABLE = new VariableReferenceExpression(Optional.empty(), "ordering", DOUBLE);
    private static final VariableReferenceExpression PAYLOAD_VARIABLE = new VariableReferenceExpression(Optional.empty(), "payload", DOUBLE);

    @Test
    public void testSingleAndFinalTopN()
    {
        PlanNodeStatsEstimate sourceStats = sourceStats(100, 0);

        for (TopNNode.Step step : ImmutableList.of(SINGLE, FINAL)) {
            tester().assertStatsFor(pb -> topN(pb, 10, step, ASC_NULLS_FIRST))
                    .withSourceStats(0, sourceStats)
                    .check(check -> check
                            .outputRowsCount(10)
                            .confident(HIGH)
                            .variableStats(ORDERING_VARIABLE, assertion -> assertion
                                    .lowValue(1)
                                    .highValue(10)
                                    .distinctValuesCount(5)
                                    .nullsFraction(0))
                            .variableStats(PAYLOAD_VARIABLE, assertion -> assertion
                                    .lowValue(0)
                                    .highValue(3)
                                    .distinctValuesCount(4)
                                    .nullsFraction(0.5)));
        }
    }

    @Test
    public void testPartialTopN()
    {
        PlanNodeStatsEstimate sourceStats = sourceStats(100, 0);

        tester().assertStatsFor(pb -> topN(pb, 10, PARTIAL, ASC_NULLS_FIRST))
                .withSourceStats(0, sourceStats)
                .check(check -> check.outputRowsCount(10));

        tester().assertStatsFor(pb -> topN(pb, 10, PARTIAL, ASC_NULLS_FIRST))
                .withSourceStats(0, sourceStats(5_000_000, 0))
                .check(check -> check.outputRowsCount(50));

        tester().assertStatsFor(pb -> topN(pb, 10, PARTIAL, ASC_NULLS_FIRST))
                .withSourceStats(0, sourceStats(5, 0))
                .check(check -> check.outputRowsCount(5));
    }

    @Test
    public void testUnknownInput()
    {
        tester().assertStatsFor(pb -> topN(pb, 10, PARTIAL, ASC_NULLS_FIRST))
                .withSourceStats(0, PlanNodeStatsEstimate.unknown())
                .check(PlanNodeStatsAssertion::outputRowsCountUnknown);

        for (TopNNode.Step step : ImmutableList.of(SINGLE, FINAL)) {
            tester().assertStatsFor(pb -> topN(pb, 10, step, ASC_NULLS_FIRST))
                    .withSourceStats(0, PlanNodeStatsEstimate.unknown())
                    .check(check -> check.outputRowsCount(10));
        }
    }

    @Test
    public void testTopNDoesNotLimitSmallInput()
    {
        PlanNodeStatsEstimate sourceStats = sourceStats(100, 0);

        for (TopNNode.Step step : ImmutableList.of(SINGLE, PARTIAL, FINAL)) {
            tester().assertStatsFor(pb -> topN(pb, 1_000, step, ASC_NULLS_FIRST))
                    .withSourceStats(0, sourceStats)
                    .check(check -> check.equalTo(sourceStats));
        }
    }

    @Test
    public void testNullOrdering()
    {
        tester().assertStatsFor(pb -> topN(pb, 10, SINGLE, ASC_NULLS_LAST))
                .withSourceStats(0, sourceStats(100, 0.3))
                .check(check -> check
                        .outputRowsCount(10)
                        .variableStats(ORDERING_VARIABLE, assertion -> assertion.nullsFraction(0)));

        tester().assertStatsFor(pb -> topN(pb, 50, SINGLE, ASC_NULLS_LAST))
                .withSourceStats(0, sourceStats(100, 0.6))
                .check(check -> check
                        .outputRowsCount(50)
                        .variableStats(ORDERING_VARIABLE, assertion -> assertion.nullsFraction(0.2)));

        tester().assertStatsFor(pb -> topN(pb, 50, SINGLE, ASC_NULLS_FIRST))
                .withSourceStats(0, sourceStats(100, 0.2))
                .check(check -> check
                        .outputRowsCount(50)
                        .variableStats(ORDERING_VARIABLE, assertion -> assertion.nullsFraction(0.4)));

        tester().assertStatsFor(pb -> topN(pb, 50, SINGLE, ASC_NULLS_FIRST))
                .withSourceStats(0, sourceStats(100, 0.6))
                .check(check -> check
                        .outputRowsCount(50)
                        .variableStats(ORDERING_VARIABLE, assertion -> assertion
                                .distinctValuesCount(2.5)
                                .nullsFraction(0.95)));
    }

    @Test
    public void testZeroCount()
    {
        tester().assertStatsFor(pb -> topN(pb, 0, SINGLE, ASC_NULLS_FIRST))
                .withSourceStats(0, sourceStats(100, 0.6))
                .check(check -> check.outputRowsCount(0));
    }

    private static TopNNode topN(PlanBuilder planBuilder, long count, TopNNode.Step step, SortOrder sortOrder)
    {
        PlanNode source = planBuilder.values(ORDERING_VARIABLE, PAYLOAD_VARIABLE);
        return new TopNNode(
                source.getSourceLocation(),
                planBuilder.getIdAllocator().getNextId(),
                source,
                count,
                new OrderingScheme(ImmutableList.of(new Ordering(ORDERING_VARIABLE, sortOrder))),
                step);
    }

    private static PlanNodeStatsEstimate sourceStats(double rowCount, double orderingNullsFraction)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rowCount)
                .setConfidence(HIGH)
                .addVariableStatistics(ORDERING_VARIABLE, VariableStatsEstimate.builder()
                        .setLowValue(1)
                        .setHighValue(10)
                        .setDistinctValuesCount(5)
                        .setNullsFraction(orderingNullsFraction)
                        .build())
                .addVariableStatistics(PAYLOAD_VARIABLE, VariableStatsEstimate.builder()
                        .setLowValue(0)
                        .setHighValue(3)
                        .setDistinctValuesCount(4)
                        .setNullsFraction(0.5)
                        .build())
                .build();
    }
}
