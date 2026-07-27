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

import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
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
import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
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

public class TestPushAggregationThroughUniqueLookupJoin
        extends BaseRuleTest
{
    @Test
    public void testPushesBelowFilteredUniqueLookup()
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
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenConstraintExploitationIsDisabled()
    {
        tester().assertThat(new PushAggregationThroughUniqueLookupJoin(getFunctionManager()), logicalPropertiesProvider())
                .setSystemProperty(EXPLOIT_CONSTRAINTS, "false")
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN, "true")
                .on(this::directPlanWithUniqueLookup)
                .doesNotFire();
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
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_UNIQUE_LOOKUP_JOIN, "true");
    }

    private LogicalPropertiesProviderImpl logicalPropertiesProvider()
    {
        return new LogicalPropertiesProviderImpl(new FunctionResolution(getFunctionManager().getFunctionAndTypeResolver()));
    }

    private PlanNode directPlanWithUniqueLookup(PlanBuilder p)
    {
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
