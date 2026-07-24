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
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.GroupedScalarFilterNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isNativeExecutionEnabled;
import static com.facebook.presto.SystemSessionProperties.isStreamingForPartialAggregationEnabled;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.operator.aggregation.AggregationUtils.isDecomposable;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Splits the aggregation feeding GroupedScalarFilter into partial/final form and
 * broadcasts only the partial scalar rows to every final-aggregation partition.
 *
 * GroupedScalarFilter needs the scalar grouping-set result on every partition
 * where grouped rows are filtered. Broadcasting below the single aggregation
 * would replicate raw input rows from the empty grouping set. This rule runs
 * after exchange planning and local exchange planning, so it can introduce the
 * required partial/final shape directly:
 *
 * <pre>
 *   GroupedScalarFilter
 *     Aggregation(FINAL)
 *       Exchange(REPARTITION, replicateNulls)
 *         Aggregation(PARTIAL)
 *           source
 * </pre>
 */
public class SplitGroupedScalarFilterAggregation
        implements Rule<GroupedScalarFilterNode>
{
    private static final Pattern<GroupedScalarFilterNode> PATTERN = typeOf(GroupedScalarFilterNode.class);

    private final FunctionAndTypeManager functionAndTypeManager;

    public SplitGroupedScalarFilterAggregation(FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
    }

    @Override
    public Pattern<GroupedScalarFilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isNativeExecutionEnabled(session);
    }

    @Override
    public Result apply(GroupedScalarFilterNode node, Captures captures, Context context)
    {
        PlanNode source = context.getLookup().resolve(node.getSource());
        if (!(source instanceof AggregationNode)) {
            return Result.empty();
        }

        AggregationNode aggregation = (AggregationNode) source;
        if (aggregation.getStep() != SINGLE ||
                aggregation.getGroupingKeys().size() != 1 ||
                aggregation.getHashVariable().isPresent() ||
                aggregation.getGroupIdVariable().isPresent() ||
                aggregation.getAggregationId().isPresent() ||
                !isDecomposable(aggregation, functionAndTypeManager)) {
            return Result.empty();
        }

        List<ExchangeNode> exchanges = new ArrayList<>();
        PlanNode partialSource = peelRepartitionExchanges(
                aggregation.getSource(),
                aggregation.getGroupingKeys(),
                exchanges,
                context);
        if (exchanges.isEmpty()) {
            return Result.empty();
        }
        Integer aggregationId = Integer.parseInt(context.getIdAllocator().getNextId().getId());

        Map<VariableReferenceExpression, Aggregation> partialAggregations = new LinkedHashMap<>();
        Map<VariableReferenceExpression, Aggregation> finalAggregations = new LinkedHashMap<>();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : aggregation.getAggregations().entrySet()) {
            Aggregation originalAggregation = entry.getValue();
            FunctionHandle functionHandle = originalAggregation.getFunctionHandle();
            String functionName = functionAndTypeManager.getFunctionMetadata(functionHandle).getName().getObjectName();
            AggregationFunctionImplementation function = functionAndTypeManager.getAggregateFunctionImplementation(functionHandle);
            VariableReferenceExpression intermediateVariable = context.getVariableAllocator().newVariable(
                    originalAggregation.getCall().getSourceLocation(),
                    functionName,
                    function.getIntermediateType());

            checkState(!originalAggregation.getOrderBy().isPresent(), "Aggregate with ORDER BY does not support partial aggregation");
            partialAggregations.put(intermediateVariable, new Aggregation(
                    new CallExpression(
                            originalAggregation.getCall().getSourceLocation(),
                            functionName,
                            functionHandle,
                            function.getIntermediateType(),
                            originalAggregation.getArguments()),
                    originalAggregation.getFilter(),
                    originalAggregation.getOrderBy(),
                    originalAggregation.isDistinct(),
                    originalAggregation.getMask()));

            finalAggregations.put(entry.getKey(), new Aggregation(
                    new CallExpression(
                            originalAggregation.getCall().getSourceLocation(),
                            functionName,
                            functionHandle,
                            function.getFinalType(),
                            ImmutableList.<RowExpression>builder()
                                    .add(intermediateVariable)
                                    .addAll(originalAggregation.getArguments().stream()
                                            .filter(SplitGroupedScalarFilterAggregation::isLambda)
                                            .collect(toImmutableList()))
                                    .build()),
                    Optional.empty(),
                    Optional.empty(),
                    false,
                    Optional.empty()));
        }

        ImmutableList<VariableReferenceExpression> preGroupedSymbols = ImmutableList.of();
        if (isStreamingForPartialAggregationEnabled(context.getSession())) {
            preGroupedSymbols = ImmutableList.copyOf(aggregation.getGroupingSets().getGroupingKeys());
        }

        AggregationNode partial = new AggregationNode(
                aggregation.getSourceLocation(),
                context.getIdAllocator().getNextId(),
                partialSource,
                partialAggregations,
                aggregation.getGroupingSets(),
                preGroupedSymbols,
                PARTIAL,
                Optional.empty(),
                Optional.empty(),
                Optional.of(aggregationId));

        PlanNode replicatedExchange = partial;
        for (int i = exchanges.size() - 1; i >= 0; i--) {
            ExchangeNode exchange = exchanges.get(i);
            replicatedExchange = new ExchangeNode(
                    exchange.getSourceLocation(),
                    context.getIdAllocator().getNextId(),
                    exchange.getType(),
                    exchange.getScope(),
                    new PartitioningScheme(
                            exchange.getPartitioningScheme().getPartitioning(),
                            replicatedExchange.getOutputVariables(),
                            Optional.empty(),
                            false,
                            true,
                            exchange.getPartitioningScheme().isScaleWriters(),
                            exchange.getPartitioningScheme().getEncoding(),
                            exchange.getPartitioningScheme().getBucketToPartition()),
                    ImmutableList.of(replicatedExchange),
                    ImmutableList.of(replicatedExchange.getOutputVariables()),
                    exchange.isEnsureSourceOrdering(),
                    exchange.getOrderingScheme());
        }

        AggregationNode finalAggregation = new AggregationNode(
                aggregation.getSourceLocation(),
                aggregation.getId(),
                replicatedExchange,
                finalAggregations,
                aggregation.getGroupingSets(),
                ImmutableList.of(),
                FINAL,
                Optional.empty(),
                Optional.empty(),
                Optional.of(aggregationId));

        return Result.ofPlanNode(new GroupedScalarFilterNode(
                node.getSourceLocation(),
                node.getId(),
                node.getStatsEquivalentPlanNode(),
                finalAggregation,
                node.getGroupIdVariable(),
                node.getGroupedGroupId(),
                node.getScalarGroupId(),
                node.getScalarValueVariable(),
                node.getScalarVariable(),
                node.getPredicate()));
    }

    private PlanNode peelRepartitionExchanges(
            PlanNode source,
            List<VariableReferenceExpression> groupingKeys,
            List<ExchangeNode> exchanges,
            Context context)
    {
        PlanNode current = context.getLookup().resolve(source);
        while (current instanceof ExchangeNode) {
            ExchangeNode exchange = (ExchangeNode) current;
            if (exchange.getType() != REPARTITION ||
                    !exchange.getPartitioningScheme().getPartitioning().getHandle().equals(FIXED_HASH_DISTRIBUTION) ||
                    exchange.getPartitioningScheme().getHashColumn().isPresent() ||
                    exchange.getSources().size() != 1 ||
                    !exchange.getInputs().get(0).equals(exchange.getOutputVariables()) ||
                    !exchange.getPartitioningScheme().getPartitioning().getVariableReferences().equals(ImmutableSet.copyOf(groupingKeys))) {
                break;
            }

            exchanges.add(exchange);
            current = context.getLookup().resolve(exchange.getSources().get(0));
        }
        return current;
    }

    private static boolean isLambda(RowExpression rowExpression)
    {
        return rowExpression instanceof LambdaDefinitionExpression;
    }
}
