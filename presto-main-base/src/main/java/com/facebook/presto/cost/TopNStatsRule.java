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

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;

import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.topN;

public class TopNStatsRule
        extends SimpleStatsRule<TopNNode>
{
    private static final int ESTIMATED_PARTIAL_TOPN_INPUT_PER_DRIVER = 1_000_000;
    private static final Pattern<TopNNode> PATTERN = topN();

    public TopNStatsRule(StatsNormalizer normalizer)
    {
        super(normalizer);
    }

    @Override
    public Pattern<TopNNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(TopNNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        double sourceRowCount = sourceStats.getOutputRowCount();

        /*
         * A partial TopN runs independently in each driver. The number of drivers is
         * not known while planning, so estimate one partial result per million input
         * rows. This gives distribution selection a bounded estimate instead of
         * treating a partial TopN as having unknown cardinality.
         */
        if (node.getStep() == TopNNode.Step.PARTIAL) {
            double estimatedOutputRowCount = Math.max(sourceRowCount / ESTIMATED_PARTIAL_TOPN_INPUT_PER_DRIVER, 1) * node.getCount();
            return Optional.of(PlanNodeStatsEstimate.buildFrom(sourceStats)
                    .setOutputRowCount(Math.min(estimatedOutputRowCount, sourceRowCount))
                    .build());
        }

        if (sourceRowCount <= node.getCount()) {
            return Optional.of(sourceStats);
        }

        long count = node.getCount();
        PlanNodeStatsEstimate resultStats = PlanNodeStatsEstimate.buildFrom(sourceStats)
                .setOutputRowCount(count)
                .build();
        if (count == 0) {
            return Optional.of(resultStats);
        }

        VariableReferenceExpression firstOrderVariable = node.getOrderingScheme().getOrderBy().get(0).getVariable();
        SortOrder sortOrder = node.getOrderingScheme().getOrdering(firstOrderVariable);
        resultStats = resultStats.mapVariableColumnStatistics(firstOrderVariable, variableStats -> {
            VariableStatsEstimate.Builder newStats = VariableStatsEstimate.buildFrom(variableStats);
            double nullCount = sourceRowCount * variableStats.getNullsFraction();
            if (sortOrder.isNullsFirst()) {
                if (nullCount > count) {
                    newStats.setNullsFraction(1.0);
                }
                else {
                    newStats.setNullsFraction(nullCount / count);
                }
            }
            else {
                double nonNullCount = sourceRowCount - nullCount;
                if (nonNullCount > count) {
                    newStats.setNullsFraction(0.0);
                }
                else {
                    newStats.setNullsFraction((count - nonNullCount) / count);
                }
            }
            return newStats.build();
        });

        return Optional.of(resultStats);
    }
}
