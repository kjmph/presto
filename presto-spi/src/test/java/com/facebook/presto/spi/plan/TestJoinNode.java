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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestJoinNode
{
    private static final VariableReferenceExpression LEFT_KEY =
            new VariableReferenceExpression(Optional.empty(), "left_key", BIGINT);
    private static final VariableReferenceExpression RIGHT_KEY =
            new VariableReferenceExpression(Optional.empty(), "right_key", BIGINT);

    @Test
    public void testFlipChildrenSwapsJoinKeyMetadata()
    {
        JoinNode join = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join"),
                INNER,
                valuesNode("left", LEFT_KEY),
                valuesNode("right", RIGHT_KEY),
                ImmutableList.of(new EquiJoinClause(LEFT_KEY, RIGHT_KEY)),
                ImmutableList.of(LEFT_KEY, RIGHT_KEY),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableMap.of())
                .withKeyProperties(true, false, true, false);

        JoinNode flipped = join.flipChildren();

        assertFalse(flipped.isLeftKeysUnique());
        assertTrue(flipped.isRightKeysUnique());
        assertFalse(flipped.isLeftKeysNonNull());
        assertTrue(flipped.isRightKeysNonNull());
    }

    private static ValuesNode valuesNode(
            String id,
            VariableReferenceExpression variable)
    {
        return new ValuesNode(
                Optional.empty(),
                new PlanNodeId(id),
                ImmutableList.of(variable),
                ImmutableList.of(),
                Optional.empty());
    }
}
