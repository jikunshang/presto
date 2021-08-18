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
import com.facebook.presto.common.PageConvertor;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.mapd.CiderJNI;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Collections.unmodifiableList;
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
    private String partialCiderPlan = "{\"rels\":[{\"id\":\"0\",\"relOp\":\"LogicalTableScan\"," +
            "\"fieldNames\":[\"a\",\"rowid\"],\"table\":[\"omnisci\",\"TEST_TABLE\"],\"inputs\":[]},{\"id\":\"1\"," +
            "\"relOp\":\"LogicalProject\",\"fields\":[\"a\"],\"exprs\":[{\"input\":0}]},{\"id\":\"2\",\"relOp\":\"LogicalAggregate\",\"fields\":[\"EXPR$0\"],\"group\":[],\"aggs\":[{\"agg\":\"SUM\",\"type\":{\"type\":\"BIGINT\",\"nullable\":true},\"distinct\":false,\"operands\":[0]}]}]}";

    private String finalCiderPlan = "{\"rels\":[{\"id\":\"0\",\"relOp\":\"LogicalTableScan\",\"fieldNames\":[\"EXPR$0\",\"rowid\"],\"table\":[\"omnisci\",\"TEST_TABLE\"],\"inputs\":[]},{\"id\":\"1\",\"relOp\":\"LogicalProject\",\"fields\":[\"EXPR$0\"],\"exprs\":[{\"input\":0}]},{\"id\":\"2\",\"relOp\":\"LogicalAggregate\",\"fields\":[\"EXPR$0\"],\"group\":[],\"aggs\":[{\"agg\":\"SUM\",\"type\":{\"type\":\"BIGINT\",\"nullable\":true},\"distinct\":false,\"operands\":[0]}]}]}";

    private int targetRetRows = 0;
    private boolean finished = false;

    private enum State
    {
        NEEDS_INPUT,
        HAS_OUTPUT,
        FINISHED
    }
    private State state = State.NEEDS_INPUT;

    private final OperatorContext operatorContext;
    private long ciderPtr;
    private List<AggregateInfo> aggregateInfos;

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

        List aggs = new ArrayList<AggregateInfo>();
        node.getAggregations();
        for (Map.Entry<VariableReferenceExpression, AggregationNode.Aggregation> entry :
                node.getAggregations().entrySet()) {
            Type columnType = entry.getValue().getArguments().get(0).getType();
            String columnName =
                    ((VariableReferenceExpression) entry.getValue().getCall().getArguments().get(0)).getName();
            String aggregateName = entry.getValue().getCall().getDisplayName();
            Type returnType = entry.getValue().getCall().getType();

            aggs.add(new AggregateInfo(aggregateName, columnName, columnType, returnType));
        }
        this.aggregateInfos = unmodifiableList(aggs);

        for (AggregateInfo info : aggregateInfos) {
            System.err.println(info.toString());
        }
    }

    // TODO: how to deconstruct Cider Instance.
    protected void finalize()
    {
        System.err.println("yes we are closing");
        CiderJNI.close(ciderPtr);
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
        long[] dataBuffers = PageConvertor.toOffHeap(page);
        long[] nullBuffers = new long[page.getChannelCount()];
        long[] resultDataBuffers = PageConvertor.allocateBuffers(page.getChannelCount(), 1024 * 16);
        long[] resultNullBuffers = PageConvertor.allocateBuffers(page.getChannelCount(), 1024 * 16);
        String tableSchema = "{\"Table\":\"TEST_TABLE\",\"Columns\":[{\"a\":\"LONG\"}]}";

        // call JNI API and get batch result
        targetRetRows += CiderJNI.processBlocks(
                ciderPtr,
                partialCiderPlan,
                tableSchema,
                dataBuffers,
                nullBuffers,
                resultDataBuffers,
                resultNullBuffers,
                page.getPositionCount(),
                Thread.currentThread().getId(),
                1);
        // merge batch result
    }

    @Override
    public Page getOutput()
    {
        if (finished) {
            return null;
        }
        if (targetRetRows > 0) {
            finished = true;
            state = State.FINISHED;
            long[] dataBuffers = PageConvertor.allocateBuffers(1, 1024 * 16);
            long[] nullBuffers = PageConvertor.allocateBuffers(1, 1024 * 16);
            long[] resultDataBuffers = PageConvertor.allocateBuffers(1, 1024 * 16);
            long[] resultNullBuffers = PageConvertor.allocateBuffers(1, 1024 * 16);
            String tableSchema = "";

            // get final result
            int ret = CiderJNI.processBlocks(
                ciderPtr,
                finalCiderPlan,
                tableSchema,
                dataBuffers,
                nullBuffers,
                resultDataBuffers,
                resultNullBuffers,
                0,
                Thread.currentThread().getId(),
                2);

            String[] types = {"Long"};
            Page page = PageConvertor.toPage(resultDataBuffers, types, ret);
            return page;
        }
        return null;
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

    class AggregateInfo
    {
        private String aggregateName;
        private String columnName;
        private Type columnType;
        private Type returnType;

        AggregateInfo(String aggregateName,
                      String columnName,
                      Type columnType,
                      Type returnType)
        {
            this.aggregateName = aggregateName;
            this.columnName = columnName;
            this.columnType = columnType;
            this.returnType = returnType;
        }

        @Override
        public String toString()
        {
            return "AggregateInfo{" +
                    "aggregateName='" + aggregateName + '\'' +
                    ", columnName='" + columnName + '\'' +
                    ", columnType='" + columnType.toString() + '\'' +
                    ", returnType='" + returnType.toString() + '\'' +
                    '}';
        }
    }
}
