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
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.MatchResult;
import com.facebook.presto.sql.planner.assertions.Matcher;
import com.facebook.presto.sql.planner.assertions.SymbolAliases;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.PUSH_SIDE_LOCAL_PROJECTION_THROUGH_JOIN;
import static com.facebook.presto.common.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.plan.JoinDistributionType.PARTITIONED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.ProjectNode.Locality.REMOTE;
import static com.facebook.presto.sql.planner.VariablesExtractor.extractUnique;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.strictProject;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;

public class TestPushSideLocalProjectionThroughJoin
        extends BaseRuleTest
{
    @Test
    public void testExtractsQ14ExpressionsAndReusesRevenue()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression partKey = p.variable("part_key", BIGINT);
                    VariableReferenceExpression partType = p.variable("part_type", VARCHAR);
                    VariableReferenceExpression lineKey = p.variable("line_key", BIGINT);
                    VariableReferenceExpression extendedPrice = p.variable("extended_price", DOUBLE);
                    VariableReferenceExpression discount = p.variable("discount", DOUBLE);
                    VariableReferenceExpression promoRevenue = p.variable("promo_revenue", DOUBLE);
                    VariableReferenceExpression revenue = p.variable("revenue", DOUBLE);

                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    remoteRepartition(p, p.values(partKey, partType), partKey),
                                    remoteRepartition(p, p.values(lineKey, extendedPrice, discount), lineKey),
                                    new EquiJoinClause(partKey, lineKey))
                            .withDistributionType(PARTITIONED);

                    return p.project(
                            Assignments.builder()
                                    .put(promoRevenue, p.rowExpression("CASE WHEN part_type LIKE 'PROMO%' THEN extended_price * (1E0 - discount) ELSE 0E0 END"))
                                    .put(revenue, p.rowExpression("extended_price * (1E0 - discount)"))
                                    .build(),
                            join);
                })
                .matches(
                        strictProject(
                                ImmutableMap.of(
                                        "promo_revenue", expression("CASE WHEN is_promo THEN revenue ELSE 0E0 END"),
                                        "revenue_output", expression("revenue")),
                                join(
                                        strictProject(
                                                ImmutableMap.of(
                                                        "part_key", expression("part_key"),
                                                        "is_promo", expression("substr(part_type, BIGINT '1', BIGINT '5') = 'PROMO'")),
                                                exchange(REMOTE_STREAMING, REPARTITION, values("part_key", "part_type"))),
                                        strictProject(
                                                ImmutableMap.of(
                                                        "line_key", expression("line_key"),
                                                        "revenue", expression("extended_price * (1E0 - discount)")),
                                                exchange(REMOTE_STREAMING, REPARTITION, values("line_key", "extended_price", "discount"))))));
    }

    @Test
    public void testDoesNotMoveWidthNeutralExpression()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    remoteRepartition(p, p.values(leftKey), leftKey),
                                    remoteRepartition(p, p.values(rightKey), rightKey),
                                    new EquiJoinClause(leftKey, rightKey))
                            .withDistributionType(PARTITIONED);
                    return p.project(
                            Assignments.of(p.variable("result", BIGINT), p.rowExpression("left_key + BIGINT '1'")),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPrecomputePotentiallyFailingExpression()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression numerator = p.variable("numerator", DOUBLE);
                    VariableReferenceExpression denominator = p.variable("denominator", DOUBLE);
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    remoteRepartition(p, p.values(leftKey), leftKey),
                                    remoteRepartition(p, p.values(rightKey, numerator, denominator), rightKey),
                                    new EquiJoinClause(leftKey, rightKey))
                            .withDistributionType(PARTITIONED);
                    return p.project(
                            Assignments.of(p.variable("result", DOUBLE), p.rowExpression("numerator / denominator")),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotExtractConditionalResultBranch()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression leftValue1 = p.variable("left_value_1", DOUBLE);
                    VariableReferenceExpression leftValue2 = p.variable("left_value_2", DOUBLE);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression rightFlag = p.variable("right_flag", BOOLEAN);
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    remoteRepartition(p, p.values(leftKey, leftValue1, leftValue2), leftKey),
                                    remoteRepartition(p, p.values(rightKey, rightFlag), rightKey),
                                    new EquiJoinClause(leftKey, rightKey))
                            .withDistributionType(PARTITIONED);
                    return p.project(
                            Assignments.of(
                                    p.variable("result", DOUBLE),
                                    p.rowExpression("CASE WHEN right_flag THEN left_value_1 + left_value_2 ELSE DOUBLE '0' END")),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotExtractTryArgument()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression leftValue1 = p.variable("left_value_1", DOUBLE);
                    VariableReferenceExpression leftValue2 = p.variable("left_value_2", DOUBLE);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression rightValue = p.variable("right_value", DOUBLE);
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    remoteRepartition(p, p.values(leftKey, leftValue1, leftValue2), leftKey),
                                    remoteRepartition(p, p.values(rightKey, rightValue), rightKey),
                                    new EquiJoinClause(leftKey, rightKey))
                            .withDistributionType(PARTITIONED);
                    return p.project(
                            Assignments.of(
                                    p.variable("result", DOUBLE),
                                    p.rowExpression("TRY(left_value_1 + left_value_2) + right_value")),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotProduceVariableWidthExchangeExpression()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression leftText1 = p.variable("left_text_1", VARCHAR);
                    VariableReferenceExpression leftText2 = p.variable("left_text_2", VARCHAR);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    remoteRepartition(p, p.values(leftKey, leftText1, leftText2), leftKey),
                                    remoteRepartition(p, p.values(rightKey), rightKey),
                                    new EquiJoinClause(leftKey, rightKey))
                            .withDistributionType(PARTITIONED);
                    return p.project(
                            Assignments.of(p.variable("result", VARCHAR), p.rowExpression("concat(left_text_1, left_text_2)")),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPrecomputeComplexTypeComparison()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression leftArray1 = p.variable("left_array_1", new ArrayType(BIGINT));
                    VariableReferenceExpression leftArray2 = p.variable("left_array_2", new ArrayType(BIGINT));
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    remoteRepartition(p, p.values(leftKey, leftArray1, leftArray2), leftKey),
                                    remoteRepartition(p, p.values(rightKey), rightKey),
                                    new EquiJoinClause(leftKey, rightKey))
                            .withDistributionType(PARTITIONED);
                    return p.project(
                            Assignments.of(p.variable("result", BOOLEAN), p.rowExpression("left_array_1 < left_array_2")),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPrecomputeUnsafeSubstringBounds()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression leftText = p.variable("left_text", VARCHAR);
                    VariableReferenceExpression expectedText = p.variable("expected_text", VARCHAR);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    remoteRepartition(p, p.values(leftKey, leftText, expectedText), leftKey),
                                    remoteRepartition(p, p.values(rightKey), rightKey),
                                    new EquiJoinClause(leftKey, rightKey))
                            .withDistributionType(PARTITIONED);
                    return p.project(
                            Assignments.of(
                                    p.variable("result", BOOLEAN),
                                    p.rowExpression("substr(left_text, BIGINT '-1', BIGINT '9223372036854775807') = expected_text")),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotPrecomputeVarbinarySubstring()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression leftBinary = p.variable("left_binary", VARBINARY);
                    VariableReferenceExpression expectedBinary = p.variable("expected_binary", VARBINARY);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    remoteRepartition(p, p.values(leftKey, leftBinary, expectedBinary), leftKey),
                                    remoteRepartition(p, p.values(rightKey), rightKey),
                                    new EquiJoinClause(leftKey, rightKey))
                            .withDistributionType(PARTITIONED);
                    return p.project(
                            Assignments.of(
                                    p.variable("result", BOOLEAN),
                                    p.rowExpression("substr(left_binary, BIGINT '2', BIGINT '9223372036854775807') = expected_binary")),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testAccountsForExchangeRequiredVariablesWhenCheckingWidth()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression partitionKey = p.variable("partition_key", BIGINT);
                    VariableReferenceExpression hashKey = p.variable("hash_key", BIGINT);
                    VariableReferenceExpression orderingKey = p.variable("ordering_key", BIGINT);
                    VariableReferenceExpression leftValue1 = p.variable("left_value_1", DOUBLE);
                    VariableReferenceExpression leftValue2 = p.variable("left_value_2", DOUBLE);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    PlanNode leftSource = p.values(leftKey, partitionKey, hashKey, orderingKey, leftValue1, leftValue2);
                    PlanNode left = p.exchange(exchange -> exchange
                            .type(REPARTITION)
                            .scope(REMOTE_STREAMING)
                            .fixedHashDistributionPartitioningScheme(leftSource.getOutputVariables(), ImmutableList.of(partitionKey), hashKey)
                            .addSource(leftSource)
                            .addInputsSet(leftSource.getOutputVariables())
                            .setEnsureSourceOrdering(true)
                            .orderingScheme(new OrderingScheme(ImmutableList.of(new Ordering(orderingKey, ASC_NULLS_FIRST)))));
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    left,
                                    remoteRepartition(p, p.values(rightKey), rightKey),
                                    new EquiJoinClause(leftKey, rightKey))
                            .withDistributionType(PARTITIONED);
                    return p.project(
                            Assignments.builder()
                                    .put(p.variable("sum", DOUBLE), p.rowExpression("left_value_1 + left_value_2"))
                                    .put(p.variable("product", DOUBLE), p.rowExpression("left_value_1 * left_value_2"))
                                    .build(),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testPreservesJoinPropertiesAndRequiredInputs()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
                    VariableReferenceExpression leftValue1 = p.variable("left_value_1", DOUBLE);
                    VariableReferenceExpression leftValue2 = p.variable("left_value_2", DOUBLE);
                    VariableReferenceExpression leftFilter = p.variable("left_filter", DOUBLE);
                    VariableReferenceExpression leftHash = p.variable("left_hash", BIGINT);
                    VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
                    VariableReferenceExpression rightFilter = p.variable("right_filter", DOUBLE);
                    VariableReferenceExpression rightHash = p.variable("right_hash", BIGINT);

                    PlanNode left = remoteRepartition(
                            p,
                            p.values(leftKey, leftValue1, leftValue2, leftFilter, leftHash),
                            leftKey);
                    PlanNode right = remoteRepartition(
                            p,
                            p.values(rightKey, rightFilter, rightHash),
                            rightKey);
                    JoinNode join = p.join(
                                    JoinType.INNER,
                                    left,
                                    right,
                                    ImmutableList.of(new EquiJoinClause(leftKey, rightKey)),
                                    ImmutableList.of(leftKey, leftValue1, leftValue2, leftFilter, leftHash, rightKey, rightFilter, rightHash),
                                    Optional.of(p.rowExpression("left_filter > right_filter")),
                                    Optional.of(leftHash),
                                    Optional.of(rightHash),
                                    Optional.of(PARTITIONED),
                                    ImmutableMap.of("dynamic_filter", rightKey))
                            .withKeyProperties(true, false, true, false, true, false);

                    return p.project(
                            Assignments.of(p.variable("result", DOUBLE), p.rowExpression("left_value_1 + left_value_2")),
                            join);
                })
                .matches(strictProject(
                        ImmutableMap.of("result", expression("left_sum")),
                        node(
                                JoinNode.class,
                                strictProject(
                                        ImmutableMap.of(
                                                "left_key", expression("left_key"),
                                                "left_filter", expression("left_filter"),
                                                "left_hash", expression("left_hash"),
                                                "left_sum", expression("left_value_1 + left_value_2")),
                                        exchange(REMOTE_STREAMING, REPARTITION, values("left_key", "left_value_1", "left_value_2", "left_filter", "left_hash"))),
                                strictProject(
                                        ImmutableMap.of(
                                                "right_key", expression("right_key"),
                                                "right_filter", expression("right_filter"),
                                                "right_hash", expression("right_hash")),
                                        exchange(REMOTE_STREAMING, REPARTITION, values("right_key", "right_filter", "right_hash"))))
                                .with(new PreservedJoinPropertiesMatcher())));
    }

    @Test
    public void testDoesNotFireForCrossJoin()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> {
                    VariableReferenceExpression leftValue1 = p.variable("left_value_1", DOUBLE);
                    VariableReferenceExpression leftValue2 = p.variable("left_value_2", DOUBLE);
                    VariableReferenceExpression rightValue = p.variable("right_value", BIGINT);
                    JoinNode join = p.join(
                            JoinType.INNER,
                            remoteRepartition(p, p.values(leftValue1, leftValue2), leftValue1),
                            remoteRepartition(p, p.values(rightValue), rightValue));
                    return p.project(
                            Assignments.of(p.variable("result", DOUBLE), p.rowExpression("left_value_1 + left_value_2")),
                            join);
                })
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithoutDistribution()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> q14Shape(p, Optional.empty(), JoinType.INNER, ProjectNode.Locality.LOCAL))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithoutRemoteRepartition()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> q14Shape(p, Optional.of(PARTITIONED), JoinType.INNER, ProjectNode.Locality.LOCAL, false))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForReplicatedJoin()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> q14Shape(p, Optional.of(REPLICATED), JoinType.INNER, ProjectNode.Locality.LOCAL, true))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForOuterJoin()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> q14Shape(p, Optional.of(PARTITIONED), JoinType.LEFT, ProjectNode.Locality.LOCAL, true))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireForRemoteProjection()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .on(p -> q14Shape(p, Optional.of(PARTITIONED), JoinType.INNER, REMOTE, true))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenDisabled()
    {
        tester().assertThat(new PushSideLocalProjectionThroughJoin(getFunctionManager()))
                .setSystemProperty(PUSH_SIDE_LOCAL_PROJECTION_THROUGH_JOIN, "false")
                .on(p -> q14Shape(p, Optional.of(PARTITIONED), JoinType.INNER, ProjectNode.Locality.LOCAL, true))
                .doesNotFire();
    }

    private static ProjectNode q14Shape(
            PlanBuilder p,
            Optional<JoinDistributionType> distributionType,
            JoinType joinType,
            ProjectNode.Locality locality)
    {
        return q14Shape(p, distributionType, joinType, locality, true);
    }

    private static ProjectNode q14Shape(
            PlanBuilder p,
            Optional<JoinDistributionType> distributionType,
            JoinType joinType,
            ProjectNode.Locality locality,
            boolean addRemoteRepartitions)
    {
        VariableReferenceExpression leftKey = p.variable("left_key", BIGINT);
        VariableReferenceExpression leftText = p.variable("left_text", VARCHAR);
        VariableReferenceExpression rightKey = p.variable("right_key", BIGINT);
        VariableReferenceExpression rightValue1 = p.variable("right_value_1", DOUBLE);
        VariableReferenceExpression rightValue2 = p.variable("right_value_2", DOUBLE);

        PlanNode left = p.values(leftKey, leftText);
        PlanNode right = p.values(rightKey, rightValue1, rightValue2);
        if (addRemoteRepartitions) {
            left = remoteRepartition(p, left, leftKey);
            right = remoteRepartition(p, right, rightKey);
        }

        JoinNode join = p.join(
                joinType,
                left,
                right,
                new EquiJoinClause(leftKey, rightKey));
        if (distributionType.isPresent()) {
            join = join.withDistributionType(distributionType.get());
        }

        return new ProjectNode(
                Optional.empty(),
                p.getIdAllocator().getNextId(),
                join,
                Assignments.builder()
                        .put(p.variable("mixed", DOUBLE), p.rowExpression("CASE WHEN left_text LIKE 'X%' THEN right_value_1 + right_value_2 ELSE DOUBLE '0' END"))
                        .put(p.variable("right_sum", DOUBLE), p.rowExpression("right_value_1 + right_value_2"))
                        .build(),
                locality);
    }

    private static PlanNode remoteRepartition(PlanBuilder p, PlanNode source, VariableReferenceExpression key)
    {
        return p.exchange(exchange -> exchange
                .type(REPARTITION)
                .scope(REMOTE_STREAMING)
                .fixedHashDistributionPartitioningScheme(source.getOutputVariables(), ImmutableList.of(key))
                .addSource(source)
                .addInputsSet(source.getOutputVariables()));
    }

    private static class PreservedJoinPropertiesMatcher
            implements Matcher
    {
        @Override
        public boolean shapeMatches(PlanNode node)
        {
            return node instanceof JoinNode;
        }

        @Override
        public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
        {
            JoinNode join = (JoinNode) node;
            return new MatchResult(
                    join.getType() == JoinType.INNER &&
                            join.getCriteria().size() == 1 &&
                            join.getCriteria().get(0).getLeft().getName().equals("left_key") &&
                            join.getCriteria().get(0).getRight().getName().equals("right_key") &&
                            join.getFilter().isPresent() &&
                            extractUnique(join.getFilter().get()).stream()
                                    .map(VariableReferenceExpression::getName)
                                    .collect(ImmutableSet.toImmutableSet())
                                    .equals(ImmutableSet.of("left_filter", "right_filter")) &&
                            join.getLeftHashVariable().map(VariableReferenceExpression::getName).equals(Optional.of("left_hash")) &&
                            join.getRightHashVariable().map(VariableReferenceExpression::getName).equals(Optional.of("right_hash")) &&
                            join.getDistributionType().equals(Optional.of(PARTITIONED)) &&
                            join.getDynamicFilters().size() == 1 &&
                            join.getDynamicFilters().containsKey("dynamic_filter") &&
                            join.getDynamicFilters().get("dynamic_filter").getName().equals("right_key") &&
                            join.isLeftKeysUnique() &&
                            !join.isRightKeysUnique() &&
                            join.isLeftKeysNonNull() &&
                            !join.isRightKeysNonNull() &&
                            join.isLeftKeysCoveredByRightKeys() &&
                            !join.isRightKeysCoveredByLeftKeys());
        }
    }
}
