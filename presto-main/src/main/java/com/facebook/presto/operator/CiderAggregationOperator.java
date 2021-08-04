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
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.google.common.collect.ImmutableList;
import com.mapd.CiderJNI;

import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class CiderAggregationOperator
        implements Operator
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
        private AggregationNode node;

        public CiderAggregationOperatorFactory(int operatorId, PlanNodeId planNodeId, Step step,
                                               List<AccumulatorFactory> accumulatorFactories,
                                               boolean useSystemMemory, AggregationNode node)
        {
            this.operatorId = operatorId;
            this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            this.step = step;
            this.accumulatorFactories = ImmutableList.copyOf(accumulatorFactories);
            this.useSystemMemory = useSystemMemory;
            this.node = node;
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            checkState(!closed, "Factory is already closed");
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, planNodeId, AggregationOperator.class.getSimpleName());
            // TODO: add more case
            boolean useCiderPlan = step == Step.PARTIAL;
            if (useCiderPlan) {
                return new CiderAggregationOperator(operatorContext, node);
            }
            else { // fallback, same as `AggregationOperatorFactory`
                return new AggregationOperator(operatorContext, step, accumulatorFactories, useSystemMemory);
            }
        }

        @Override
        public void noMoreOperators()
        {
            closed = true;
        }

        @Override
        public OperatorFactory duplicate()
        {
            return new CiderAggregationOperatorFactory(operatorId, planNodeId, step,
                    accumulatorFactories, useSystemMemory, node);
        }
    }

    private AggregationNode node;
    private String ciderPlan;

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }
    private State state = State.NEEDS_INPUT;

    private final OperatorContext operatorContext;
    private long ciderPtr = 0;

    public CiderAggregationOperator(OperatorContext operatorContext, AggregationNode node)
    {
        // Do init cider runtime work here, I guess we need build schema info,
        // query info here, so current processBlock API may not work.
        this.node = node;
        this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
        // make sure your env is ready, otherwise it will throw UnsatisfiedLinkError
        ciderPtr = CiderJNI.getPtr();
        // ciderPlan = PrestoPlanBuilder.toSchemaJson(node);
        // CiderJNI.setSchema(ciderPlan);

    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    @Override
    public boolean needsInput()
    {
        return state == State.NEEDS_INPUT;
    }

    @Override
    public void addInput(Page page)
    {
        // convert Page to off-heap buffer (blocking)

        // call JNI API and get batch result

        // merge batch result
    }

    @Override
    public Page getOutput()
    {
        // get final result
        throw new UnsupportedOperationException("Not implement yet");
    }

    @Override
    public void finish()
    {
        // equal to close? not sure.
        if (state == State.NEEDS_INPUT) {
            state = State.HAS_OUTPUT;
        }
    }

    @Override
    public boolean isFinished()
    {
        return state == State.FINISHED;
    }
}
