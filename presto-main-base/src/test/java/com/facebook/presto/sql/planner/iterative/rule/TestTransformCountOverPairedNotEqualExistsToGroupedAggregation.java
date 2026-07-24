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
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
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

    private Session session(boolean pushAggregationThroughJoin)
    {
        return Session.builder(tester().getSession())
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, String.valueOf(pushAggregationThroughJoin))
                .build();
    }
}
