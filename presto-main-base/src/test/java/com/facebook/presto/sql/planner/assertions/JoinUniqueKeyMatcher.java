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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.PlanNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.google.common.base.MoreObjects.toStringHelper;

final class JoinUniqueKeyMatcher
        implements Matcher
{
    private final boolean leftKeysUnique;
    private final boolean rightKeysUnique;
    private final Optional<Boolean> leftKeysNonNull;
    private final Optional<Boolean> rightKeysNonNull;

    JoinUniqueKeyMatcher(boolean leftKeysUnique, boolean rightKeysUnique)
    {
        this.leftKeysUnique = leftKeysUnique;
        this.rightKeysUnique = rightKeysUnique;
        this.leftKeysNonNull = Optional.empty();
        this.rightKeysNonNull = Optional.empty();
    }

    JoinUniqueKeyMatcher(boolean leftKeysUnique, boolean rightKeysUnique, boolean leftKeysNonNull, boolean rightKeysNonNull)
    {
        this.leftKeysUnique = leftKeysUnique;
        this.rightKeysUnique = rightKeysUnique;
        this.leftKeysNonNull = Optional.of(leftKeysNonNull);
        this.rightKeysNonNull = Optional.of(rightKeysNonNull);
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return node instanceof JoinNode;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        JoinNode joinNode = (JoinNode) node;
        if (joinNode.isLeftKeysUnique() != leftKeysUnique ||
                joinNode.isRightKeysUnique() != rightKeysUnique ||
                leftKeysNonNull.map(expected -> joinNode.isLeftKeysNonNull() != expected).orElse(false) ||
                rightKeysNonNull.map(expected -> joinNode.isRightKeysNonNull() != expected).orElse(false)) {
            return NO_MATCH;
        }
        return MatchResult.match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("leftKeysUnique", leftKeysUnique)
                .add("rightKeysUnique", rightKeysUnique)
                .add("leftKeysNonNull", leftKeysNonNull)
                .add("rightKeysNonNull", rightKeysNonNull)
                .toString();
    }
}
