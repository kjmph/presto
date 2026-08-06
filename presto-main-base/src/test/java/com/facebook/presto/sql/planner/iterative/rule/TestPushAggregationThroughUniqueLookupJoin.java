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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.cost.TaskCountEstimator;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.properties.LogicalPropertiesProviderImpl;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.EXPLOIT_CONSTRAINTS;
import static com.facebook.presto.SystemSessionProperties.JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.SystemSessionProperties.JOIN_MAX_BROADCAST_TABLE_SIZE;
import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_BYTE_REDUCTION_THRESHOLD;
import static com.facebook.presto.SystemSessionProperties.PARTIAL_AGGREGATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN;
import static com.facebook.presto.SystemSessionProperties.QUERY_MAX_MEMORY_PER_NODE;
import static com.facebook.presto.SystemSessionProperties.SINGLE_NODE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.SIZE_BASED_JOIN_DISTRIBUTION_TYPE;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.aggregation;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.functionCall;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.joinWithKeyProperties;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.singleGroupingSet;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.optimizations.PredicatePushDown.createDynamicFilterExpression;

public class TestPushAggregationThroughUniqueLookupJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesBelowFilteredUniqueLookup()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factValue = p.variable("fact_value");
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression sum = p.variable("sum");

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(sum, p.rowExpression("sum(fact_value)"))
                            .step(SINGLE)
                            .source(p.join(
                                    INNER,
                                    p.values(new PlanNodeId("fact"), 10, factKey, factValue),
                                    filteredUniqueLookup(p, lookupKey),
                                    new EquiJoinClause(factKey, lookupKey))));
                })
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("fact_key", "lookup_key")),
                        aggregation(
                                singleGroupingSet("fact_key"),
                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("fact_value"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                values("fact_key", "fact_value")),
                        filter(
                                aggregation(
                                        singleGroupingSet("lookup_key"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("lookup_key")))));
    }

    @Test
    public void testPushesForBroadcastLookupWithSufficientReduction()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .on(this::directPlanWithUniqueLookup)
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("fact_key", "lookup_key")),
                        aggregation(
                                singleGroupingSet("fact_key"),
                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("fact_value"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                values("fact_key", "fact_value")),
                        filter(
                                aggregation(
                                        singleGroupingSet("lookup_key"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("lookup_key")))));
    }

    @Test
    public void testPushesProjectedDeterministicExpression()
    {
        PushAggregationThroughUniqueLookupJoin rule = new PushAggregationThroughUniqueLookupJoin(getFunctionManager());
        assertRule(rule.pushAggregationThroughUniqueLookupJoinWithProjection())
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factValue = p.variable("fact_value");
                    VariableReferenceExpression projected = p.variable("projected");
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression sum = p.variable("sum");

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(sum, p.rowExpression("sum(projected)"))
                            .step(SINGLE)
                            .source(p.project(
                                    Assignments.builder()
                                            .put(factKey, factKey)
                                            .put(projected, p.rowExpression("fact_value + fact_value"))
                                            .build(),
                                    p.join(
                                            INNER,
                                            p.values(new PlanNodeId("fact"), 10, factKey, factValue),
                                            filteredUniqueLookup(p, lookupKey),
                                            new EquiJoinClause(factKey, lookupKey)))));
                })
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("fact_key", "lookup_key")),
                        aggregation(
                                singleGroupingSet("fact_key"),
                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("projected"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                project(
                                        ImmutableMap.of(
                                                "fact_key", expression("fact_key"),
                                                "projected", expression("fact_value + fact_value")),
                                        values("fact_key", "fact_value"))),
                        project(
                                ImmutableMap.of("lookup_key", expression("lookup_key")),
                                filter(
                                        aggregation(
                                                singleGroupingSet("lookup_key"),
                                                ImmutableMap.of(),
                                                ImmutableMap.of(),
                                                Optional.empty(),
                                                SINGLE,
                                                values("lookup_key"))))));
    }

    @Test
    public void testPushesWhenFactIsRightJoinInput()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .overrideStats("2", rowCountStats(150))
                .on(p -> {
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factValue = p.variable("fact_value");
                    VariableReferenceExpression sum = p.variable("sum");

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(sum, p.rowExpression("sum(fact_value)"))
                            .step(SINGLE)
                            .source(p.join(
                                    INNER,
                                    filteredUniqueLookup(p, lookupKey),
                                    p.values(new PlanNodeId("fact"), 10, factKey, factValue),
                                    new EquiJoinClause(lookupKey, factKey))));
                })
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("lookup_key", "fact_key")),
                        filter(
                                aggregation(
                                        singleGroupingSet("lookup_key"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("lookup_key"))),
                        aggregation(
                                singleGroupingSet("fact_key"),
                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("fact_value"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                values("fact_key", "fact_value"))));
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN, "false")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(500))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithUnknownStatistics()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .overrideStats("fact", PlanNodeStatsEstimate.unknown())
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenPartialAggregationIsDisabled()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "NEVER")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(500))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithVariableWidthIntermediateState()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .overrideStats("fact", factStats(100))
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factValue = p.variable("fact_value", VARCHAR);
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression minimum = p.variable("minimum", VARCHAR);

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(minimum, p.rowExpression("min(fact_value)"))
                            .step(SINGLE)
                            .source(p.join(
                                    INNER,
                                    p.values(new PlanNodeId("fact"), 10, factKey, factValue),
                                    filteredUniqueLookup(p, lookupKey),
                                    new EquiJoinClause(factKey, lookupKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithUnknownVariableWidthSourceSize()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(500))
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factValue = p.variable("fact_value", VARCHAR);
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression count = p.variable("count");

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(count, p.rowExpression("count(fact_value)"))
                            .step(SINGLE)
                            .source(p.join(
                                    INNER,
                                    p.values(new PlanNodeId("fact"), 10, factKey, factValue),
                                    filteredUniqueLookup(p, lookupKey),
                                    new EquiJoinClause(factKey, lookupKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithInvalidPartialAggregationThreshold()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(PARTIAL_AGGREGATION_BYTE_REDUCTION_THRESHOLD, "NaN")
                .setSystemProperty(PARTIAL_AGGREGATION_STRATEGY, "AUTOMATIC")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(500))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithoutSufficientGroupingReduction()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .overrideStats("fact", factStats(600))
                .overrideStats("2", rowCountStats(1_000))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testAllowsSelectivePartitionedLookupWithinBound()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .overrideStats("2", rowCountStats(130))
                .on(this::directPlanWithUniqueLookup)
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("fact_key", "lookup_key")),
                        aggregation(
                                singleGroupingSet("fact_key"),
                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("fact_value"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                values("fact_key", "fact_value")),
                        filter(
                                aggregation(
                                        singleGroupingSet("lookup_key"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("lookup_key")))));
    }

    @Test
    public void testAutomaticDistributionUsesBroadcastThreshold()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "1PB")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(150))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testAutomaticDistributionAllowsNonBroadcastLookupWithinPartitionedBound()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_MAX_BROADCAST_TABLE_SIZE, "1B")
                .setSystemProperty(SIZE_BASED_JOIN_DISTRIBUTION_TYPE, "false")
                .overrideStats("fact", factStats(100))
                .overrideStats("1", rowCountStats(1_000))
                .overrideStats("2", rowCountStats(130))
                .on(this::directPlanWithUniqueLookup)
                .matches(expectedDirectPushdown());
    }

    @Test
    public void testPreassignedReplicatedDistributionUsesBroadcastThreshold()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                // The JoinNode decision takes precedence over the session setting.
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(150))
                .on(p -> directPlanWithUniqueLookup(p, Optional.of(JoinDistributionType.REPLICATED)))
                .doesNotFire();
    }

    @Test
    public void testPreassignedPartitionedDistributionUsesPartitionedThreshold()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                // The JoinNode decision takes precedence over the session setting.
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(130))
                .on(p -> directPlanWithUniqueLookup(p, Optional.of(JoinDistributionType.PARTITIONED)))
                .matches(expectedDirectPushdown(Optional.of(JoinDistributionType.PARTITIONED)));
    }

    @Test
    public void testDoesNotFireWhenPartitionedLookupReducesBelowGroupedCardinality()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .overrideStats("fact", factStats(100))
                // Models an aggregate-output filter such as Q18's HAVING:
                // pushing first would create more groups than the join retains.
                .overrideStats("2", rowCountStats(90))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenAggregationStateExceedsPerNodeMemoryBudget()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "1GB")
                .overrideStats("fact", factStats(1_000_000_000, 100_000_000))
                .overrideStats("2", rowCountStats(200_000_000))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotUnderestimateMemoryForSmallDistinctState()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "1GB")
                .overrideStats("fact", factStats(INTEGER, 1_000_000_000, 50_000_000))
                .overrideStats("1", rowCountStats(100_000_000))
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key", INTEGER);
                    VariableReferenceExpression factPayload = p.variable("fact_payload");
                    VariableReferenceExpression lookupKey = p.variable("lookup_key", INTEGER);

                    PlanNode lookup = p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(lookupKey)
                            .step(SINGLE)
                            .source(p.values(new PlanNodeId("lookup"), 10, lookupKey)));
                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .step(SINGLE)
                            .source(p.join(
                                    INNER,
                                    p.values(new PlanNodeId("fact"), 10, factKey, factPayload),
                                    lookup,
                                    new EquiJoinClause(factKey, lookupKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDistributedExecutionDividesMemoryAcrossTasks()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager(), new TaskCountEstimator(() -> 10)), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "1GB")
                .overrideStats("fact", factStats(1_000_000_000, 100_000_000))
                .overrideStats("2", rowCountStats(200_000_000))
                .on(this::directPlanWithUniqueLookup)
                .matches(expectedDirectPushdown());
    }

    @Test
    public void testSingleNodeExecutionUsesOneTaskForMemoryBudget()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager(), new TaskCountEstimator(() -> 10)), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .setSystemProperty(QUERY_MAX_MEMORY_PER_NODE, "1GB")
                .setSystemProperty(SINGLE_NODE_EXECUTION_ENABLED, "true")
                .overrideStats("fact", factStats(1_000_000_000, 100_000_000))
                .overrideStats("2", rowCountStats(200_000_000))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForSelectiveBroadcastLookup()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(100))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithUnknownJoinStatistics()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .overrideStats("fact", factStats(100))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testUsesStricterReductionForBroadcastLookup()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "BROADCAST")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(150))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenConstraintExploitationIsDisabled()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "false")
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN, "true")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(500))
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testPreservesDynamicFilters()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .setSystemProperty(JOIN_DISTRIBUTION_TYPE, "PARTITIONED")
                .overrideStats("fact_filter", factStats(100))
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factValue = p.variable("fact_value");
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression sum = p.variable("sum");

                    PlanNode fact = p.filter(
                            new PlanNodeId("fact_filter"),
                            createDynamicFilterExpression("DF", factKey, getFunctionManager()),
                            p.values(new PlanNodeId("fact"), 10, factKey, factValue));
                    PlanNode lookup = filteredUniqueLookup(p, lookupKey);
                    PlanNode join = p.join(
                            INNER,
                            fact,
                            lookup,
                            ImmutableList.of(new EquiJoinClause(factKey, lookupKey)),
                            ImmutableList.of(factKey, factValue, lookupKey),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            ImmutableMap.of("DF", lookupKey));

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(sum, p.rowExpression("sum(fact_value)"))
                            .step(SINGLE)
                            .source(join));
                })
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("fact_key", "lookup_key")),
                        aggregation(
                                singleGroupingSet("fact_key"),
                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("fact_value"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                PlanMatchPattern.node(FilterNode.class, values("fact_key", "fact_value"))),
                        filter(
                                aggregation(
                                        singleGroupingSet("lookup_key"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("lookup_key"))))
                        .with(new NonEmptyDynamicFiltersMatcher()));
    }

    @Test
    public void testPreservesJoinKeyProperties()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factValue = p.variable("fact_value");
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression sum = p.variable("sum");

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(sum, p.rowExpression("sum(fact_value)"))
                            .step(SINGLE)
                            .source(p.join(
                                            INNER,
                                            p.values(new PlanNodeId("fact"), 10, factKey, factValue),
                                            filteredUniqueLookup(p, lookupKey),
                                            new EquiJoinClause(factKey, lookupKey))
                                    .withKeyProperties(false, true, true, true, false, true)));
                })
                .matches(joinWithKeyProperties(
                        INNER,
                        ImmutableList.of(equiJoinClause("fact_key", "lookup_key")),
                        false,
                        true,
                        true,
                        true,
                        false,
                        true,
                        aggregation(
                                singleGroupingSet("fact_key"),
                                ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("fact_value"))),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                values("fact_key", "fact_value")),
                        filter(
                                aggregation(
                                        singleGroupingSet("lookup_key"),
                                        ImmutableMap.of(),
                                        ImmutableMap.of(),
                                        Optional.empty(),
                                        SINGLE,
                                        values("lookup_key")))));
    }

    @Test
    public void testDoesNotFireWithoutUniqueLookup()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factValue = p.variable("fact_value");
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression sum = p.variable("sum");

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(sum, p.rowExpression("sum(fact_value)"))
                            .step(SINGLE)
                            .source(p.join(
                                    INNER,
                                    p.values(new PlanNodeId("fact"), 10, factKey, factValue),
                                    p.values(new PlanNodeId("lookup"), 10, lookupKey),
                                    new EquiJoinClause(factKey, lookupKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenGroupingOmitsFactJoinKey()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factGroup = p.variable("fact_group");
                    VariableReferenceExpression factValue = p.variable("fact_value");
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression sum = p.variable("sum");

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factGroup)
                            .addAggregation(sum, p.rowExpression("sum(fact_value)"))
                            .step(SINGLE)
                            .source(p.join(
                                    INNER,
                                    p.values(new PlanNodeId("fact"), 10, factKey, factGroup, factValue),
                                    filteredUniqueLookup(p, lookupKey),
                                    new EquiJoinClause(factKey, lookupKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenAggregationUsesLookupInput()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression lookupValue = p.variable("lookup_value");
                    VariableReferenceExpression rawLookupValue = p.variable("raw_lookup_value");
                    VariableReferenceExpression sum = p.variable("sum");

                    PlanNode lookup = p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(lookupKey)
                            .addAggregation(lookupValue, p.rowExpression("max(raw_lookup_value)"))
                            .step(SINGLE)
                            .source(p.values(new PlanNodeId("lookup"), 10, lookupKey, rawLookupValue)));

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(sum, p.rowExpression("sum(lookup_value)"))
                            .step(SINGLE)
                            .source(p.join(
                                    INNER,
                                    p.values(new PlanNodeId("fact"), 10, factKey),
                                    lookup,
                                    new EquiJoinClause(factKey, lookupKey))));
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithJoinFilter()
    {
        assertRule(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression factKey = p.variable("fact_key");
                    VariableReferenceExpression factValue = p.variable("fact_value");
                    VariableReferenceExpression lookupKey = p.variable("lookup_key");
                    VariableReferenceExpression sum = p.variable("sum");

                    return p.aggregation(aggregation -> aggregation
                            .singleGroupingSet(factKey)
                            .addAggregation(sum, p.rowExpression("sum(fact_value)"))
                            .step(SINGLE)
                            .source(p.join(
                                    INNER,
                                    p.values(new PlanNodeId("fact"), 10, factKey, factValue),
                                    filteredUniqueLookup(p, lookupKey),
                                    p.rowExpression("fact_value > lookup_key"),
                                    new EquiJoinClause(factKey, lookupKey))));
                })
                .doesNotFire();
    }

    private RuleAssert assertRule(Rule<?> rule)
    {
        return tester().assertThat(rule, logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "true")
                .overrideStats("fact", factStats(100))
                .overrideStats("2", rowCountStats(500));
    }

    private LogicalPropertiesProviderImpl logicalPropertiesProvider()
    {
        return new LogicalPropertiesProviderImpl(new FunctionResolution(getFunctionManager().getFunctionAndTypeResolver()));
    }

    private PlanNode directPlanWithUniqueLookup(PlanBuilder p)
    {
        return directPlanWithUniqueLookup(p, Optional.empty());
    }

    private PlanNode directPlanWithUniqueLookup(PlanBuilder p, Optional<JoinDistributionType> distributionType)
    {
        VariableReferenceExpression factKey = p.variable("fact_key");
        VariableReferenceExpression factValue = p.variable("fact_value");
        VariableReferenceExpression lookupKey = p.variable("lookup_key");
        VariableReferenceExpression sum = p.variable("sum");

        JoinNode join = p.join(
                INNER,
                p.values(new PlanNodeId("fact"), 10, factKey, factValue),
                filteredUniqueLookup(p, lookupKey),
                new EquiJoinClause(factKey, lookupKey));
        JoinNode distributedJoin = distributionType.map(join::withDistributionType).orElse(join);

        return p.aggregation(aggregation -> aggregation
                .singleGroupingSet(factKey)
                .addAggregation(sum, p.rowExpression("sum(fact_value)"))
                .step(SINGLE)
                .source(distributedJoin));
    }

    private static PlanNodeStatsEstimate factStats(double distinctKeys)
    {
        return factStats(1_000, distinctKeys);
    }

    private static PlanNodeStatsEstimate factStats(double rows, double distinctKeys)
    {
        return factStats(BIGINT, rows, distinctKeys);
    }

    private static PlanNodeStatsEstimate factStats(Type keyType, double rows, double distinctKeys)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rows)
                .addVariableStatistics(
                        new VariableReferenceExpression(Optional.empty(), "fact_key", keyType),
                        VariableStatsEstimate.builder()
                                .setDistinctValuesCount(distinctKeys)
                                .setLowValue(1)
                                .setHighValue(1_000)
                                .setNullsFraction(0)
                                .build())
                .build();
    }

    private static PlanNodeStatsEstimate rowCountStats(double rows)
    {
        return PlanNodeStatsEstimate.builder()
                .setOutputRowCount(rows)
                .build();
    }

    private static PlanMatchPattern expectedDirectPushdown()
    {
        return expectedDirectPushdown(Optional.empty());
    }

    private static PlanMatchPattern expectedDirectPushdown(Optional<JoinDistributionType> distributionType)
    {
        return join(
                INNER,
                ImmutableList.of(equiJoinClause("fact_key", "lookup_key")),
                Optional.empty(),
                distributionType,
                aggregation(
                        singleGroupingSet("fact_key"),
                        ImmutableMap.of(Optional.of("sum"), functionCall("sum", ImmutableList.of("fact_value"))),
                        ImmutableMap.of(),
                        Optional.empty(),
                        SINGLE,
                        values("fact_key", "fact_value")),
                filter(
                        aggregation(
                                singleGroupingSet("lookup_key"),
                                ImmutableMap.of(),
                                ImmutableMap.of(),
                                Optional.empty(),
                                SINGLE,
                                values("lookup_key"))));
    }

    private static class NonEmptyDynamicFiltersMatcher
            implements Matcher
    {
        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof JoinNode && !((JoinNode) node).getDynamicFilters().isEmpty();
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            return new MatchResult(true);
        }
    }

    private static PlanNode filteredUniqueLookup(PlanBuilder p, VariableReferenceExpression lookupKey)
    {
        return p.filter(
                p.rowExpression("lookup_key > BIGINT '0'"),
                p.aggregation(aggregation -> aggregation
                        .singleGroupingSet(lookupKey)
                        .step(SINGLE)
                        .source(p.values(new PlanNodeId("lookup"), 10, lookupKey))));
    }
}
