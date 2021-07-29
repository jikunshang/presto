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

package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class CiderAggregationOperator implements Operator
{
    public static class CiderAggregationOperatorFactory
            implements OperatorFactory
    {
        private final int operatorId;
        private final PlanNodeId planNodeId;
        private final Step step;
        private final List<AccumulatorFactory> accumulatorFactories;
        private final boolean useSystemMemory;
        private boolean closed;

        public CiderAggregationOperatorFactory(int operatorId, PlanNodeId planNodeId, Step step, List<AccumulatorFactory> accumulatorFactories, boolean useSystemMemory)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.useSystemMemory = useSystemMemory;
        }

        @Override
        public Operator createOperator(DriverContext driverContext) {
            return new CiderAggregationOperator();
        }

        @Override
        public void noMoreOperators() {

        }

        @Override
        public OperatorFactory duplicate() {
            return null;
        }
    }

    public CiderAggregationOperator() {
        // Do init cider runtime work here, I guess we need build schema info,
        // query info here, so current processBlock API may not work.
    }

    @Override
    public OperatorContext getOperatorContext() {
        return null;
    }

    @Override
    public boolean needsInput() {
        return false;
    }

    @Override
    public void addInput(Page page) {
        // ****************************
        // feed data to cider and do compute(optional, depends on execution model(TBD))
    }

    @Override
    public Page getOutput() {
        // get final cider compute result
        return null;
    }

    @Override
    public void finish() {
        // equal to close? not sure.

    }

    @Override
    public boolean isFinished() {
        return false;
    }
}
