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
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.isExploitConstraints;
import static com.facebook.presto.sql.planner.plan.Patterns.semiJoin;

/**
 * Carries trusted semijoin-key facts through the plan for native execution.
 */
public class AnnotateSemiJoinNodeWithUniqueKeys
        implements Rule<SemiJoinNode>
{
    private static final Pattern<SemiJoinNode> PATTERN = semiJoin();

    @Override
    public Pattern<SemiJoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return isExploitConstraints(session);
    }

    @Override
    public Result apply(SemiJoinNode semiJoinNode, Captures captures, Context context)
    {
        if (!isExploitConstraints(context.getSession())) {
            return Result.empty();
        }

        Set<VariableReferenceExpression> sourceKeys = ImmutableSet.of(semiJoinNode.getSourceJoinVariable());
        Set<VariableReferenceExpression> filteringSourceKeys = ImmutableSet.of(semiJoinNode.getFilteringSourceJoinVariable());

        boolean sourceKeyUnique = AnnotateJoinNodeWithUniqueKeys.areJoinKeysUnique(semiJoinNode.getSource(), sourceKeys);
        boolean filteringSourceKeyUnique = AnnotateJoinNodeWithUniqueKeys.areJoinKeysUnique(semiJoinNode.getFilteringSource(), filteringSourceKeys);
        boolean sourceKeyNonNull = AnnotateJoinNodeWithUniqueKeys.areJoinKeysNonNull(semiJoinNode.getSource(), sourceKeys, context);
        boolean filteringSourceKeyNonNull = AnnotateJoinNodeWithUniqueKeys.areJoinKeysNonNull(semiJoinNode.getFilteringSource(), filteringSourceKeys, context);

        if (semiJoinNode.isSourceKeyUnique() == sourceKeyUnique &&
                semiJoinNode.isFilteringSourceKeyUnique() == filteringSourceKeyUnique &&
                semiJoinNode.isSourceKeyNonNull() == sourceKeyNonNull &&
                semiJoinNode.isFilteringSourceKeyNonNull() == filteringSourceKeyNonNull) {
            return Result.empty();
        }

        return Result.ofPlanNode(semiJoinNode.withKeyProperties(sourceKeyUnique, filteringSourceKeyUnique, sourceKeyNonNull, filteringSourceKeyNonNull));
    }
}
