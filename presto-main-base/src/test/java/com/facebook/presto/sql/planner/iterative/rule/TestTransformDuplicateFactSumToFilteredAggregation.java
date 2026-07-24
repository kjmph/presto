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

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.PUSH_AGGREGATION_THROUGH_JOIN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;

public class TestTransformDuplicateFactSumToFilteredAggregation
        extends BaseRuleTest
{
    @Test
    public void testDoesNotThrowForBooleanInputAggregation()
    {
        tester().assertThat(new TransformDuplicateFactSumToFilteredAggregation(getMetadata().getFunctionAndTypeManager()))
                .setSystemProperty(PUSH_AGGREGATION_THROUGH_JOIN, "true")
                .on(p -> p.aggregation(aggregation -> {
                    VariableReferenceExpression flag = p.variable("flag", BOOLEAN);
                    aggregation.globalGrouping()
                            .addAggregation(p.variable("count", BIGINT), p.rowExpression("count(flag)"))
                            .source(p.values(flag));
                }))
                .doesNotFire();
    }
}
