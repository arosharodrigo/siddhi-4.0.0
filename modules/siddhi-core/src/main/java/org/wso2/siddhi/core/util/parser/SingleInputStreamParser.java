/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.core.util.parser;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.MetaComplexEvent;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.exception.ExecutionPlanCreationException;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.gpu.config.GpuQueryContext;
import org.wso2.siddhi.core.gpu.query.input.GpuProcessStreamReceiver;
import org.wso2.siddhi.core.gpu.query.input.stream.GpuStreamRuntime;
import org.wso2.siddhi.core.gpu.util.parser.GpuExpressionParser;
import org.wso2.siddhi.core.query.input.ProcessStreamReceiver;
import org.wso2.siddhi.core.query.input.stream.single.EntryValveProcessor;
import org.wso2.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.filter.FilterProcessor;
import org.wso2.siddhi.core.query.processor.stream.AbstractStreamProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.LengthWindowProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.SiddhiClassLoader;
import org.wso2.siddhi.core.util.SiddhiConstants;
import org.wso2.siddhi.core.util.SystemTimeBasedScheduler;
import org.wso2.siddhi.core.util.extension.holder.StreamFunctionProcessorExtensionHolder;
import org.wso2.siddhi.core.util.extension.holder.StreamProcessorExtensionHolder;
import org.wso2.siddhi.core.util.extension.holder.WindowProcessorExtensionHolder;
import org.wso2.siddhi.gpu.jni.SiddhiGpu;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.execution.query.input.handler.Filter;
import org.wso2.siddhi.query.api.execution.query.input.handler.StreamFunction;
import org.wso2.siddhi.query.api.execution.query.input.handler.StreamHandler;
import org.wso2.siddhi.query.api.execution.query.input.handler.Window;
import org.wso2.siddhi.query.api.execution.query.input.stream.SingleInputStream;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.query.api.expression.Variable;
import org.wso2.siddhi.query.api.extension.Extension;

import java.util.List;
import java.util.Map;

public class SingleInputStreamParser {

    private static final Logger log = Logger.getLogger(SingleInputStreamParser.class);

    public static SingleStreamRuntime parseInputStream(SingleInputStream inputStream, ExecutionPlanContext executionPlanContext,
                                                       List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, AbstractDefinition> streamDefinitionMap,
                                                       Map<String, AbstractDefinition> tableDefinitionMap, Map<String, AbstractDefinition> windowDefinitionMap, Map<String, EventTable> eventTableMap, MetaComplexEvent metaComplexEvent,
                                                       ProcessStreamReceiver processStreamReceiver, boolean supportsBatchProcessing, boolean outputExpectsExpiredEvents, String queryName, GpuQueryContext gpuQueryContext) {

        Processor processor = null;
        EntryValveProcessor singleThreadValve = null;
        boolean first = true;

        MetaStreamEvent metaStreamEvent;
        if (metaComplexEvent instanceof MetaStateEvent) {
            metaStreamEvent = new MetaStreamEvent();
            ((MetaStateEvent) metaComplexEvent).addEvent(metaStreamEvent);
            initMetaStreamEvent(inputStream, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, metaStreamEvent);
        } else {
            metaStreamEvent = (MetaStreamEvent) metaComplexEvent;
            initMetaStreamEvent(inputStream, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, metaStreamEvent);
        }

        if (!inputStream.getStreamHandlers().isEmpty()) {
            /* create processor chain from StreamHandlers */
            for (StreamHandler handler : inputStream.getStreamHandlers()) {

                Processor currentProcessor = generateProcessor(handler, metaComplexEvent, variableExpressionExecutors, executionPlanContext,
                        eventTableMap, supportsBatchProcessing, outputExpectsExpiredEvents, queryName, ((GpuProcessStreamReceiver)processStreamReceiver), gpuQueryContext);

                if (currentProcessor instanceof SchedulingProcessor) {
                    if (singleThreadValve == null) {

                        singleThreadValve = new EntryValveProcessor(executionPlanContext);
                        if (first) {
                            processor = singleThreadValve;
                            first = false;
                        } else {
                            processor.setToLast(singleThreadValve);
                        }
                    }
                    Scheduler scheduler = new SystemTimeBasedScheduler(executionPlanContext.getScheduledExecutorService(), singleThreadValve, executionPlanContext);
                    ((SchedulingProcessor) currentProcessor).setScheduler(scheduler);
                }

                if (first) {
                    processor = currentProcessor;
                    first = false;
                } else {
                    processor.setToLast(currentProcessor);
                }
            }
        }

        metaStreamEvent.initializeAfterWindowData();
        return new GpuStreamRuntime(processStreamReceiver, processor, metaComplexEvent);
    }

    /**
     * Parse single InputStream and return SingleStreamRuntime
     *
     * @param inputStream                 single input stream to be parsed
     * @param executionPlanContext        query to be parsed
     * @param variableExpressionExecutors List to hold VariableExpressionExecutors to update after query parsing
     * @param streamDefinitionMap         Stream Definition Map
     * @param tableDefinitionMap          Table Definition Map
     * @param eventTableMap               EventTable Map
     * @param metaComplexEvent            MetaComplexEvent
     * @param processStreamReceiver       ProcessStreamReceiver
     * @param supportsBatchProcessing     supports batch processing
     * @param outputExpectsExpiredEvents  is output expects ExpiredEvents
     * @return SingleStreamRuntime
     */
    public static SingleStreamRuntime parseInputStream(SingleInputStream inputStream, ExecutionPlanContext executionPlanContext,
                                                       List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, AbstractDefinition> streamDefinitionMap,
                                                       Map<String, AbstractDefinition> tableDefinitionMap, Map<String, AbstractDefinition> windowDefinitionMap, Map<String, EventTable> eventTableMap, MetaComplexEvent metaComplexEvent,
                                                       ProcessStreamReceiver processStreamReceiver, boolean supportsBatchProcessing, boolean outputExpectsExpiredEvents, String queryName) {
        Processor processor = null;
        EntryValveProcessor entryValveProcessor = null;
        boolean first = true;
        MetaStreamEvent metaStreamEvent;
        if (metaComplexEvent instanceof MetaStateEvent) {
            metaStreamEvent = new MetaStreamEvent();
            ((MetaStateEvent) metaComplexEvent).addEvent(metaStreamEvent);
            initMetaStreamEvent(inputStream, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, metaStreamEvent);
        } else {
            metaStreamEvent = (MetaStreamEvent) metaComplexEvent;
            initMetaStreamEvent(inputStream, streamDefinitionMap, tableDefinitionMap, windowDefinitionMap, metaStreamEvent);
        }

        // A window cannot be defined for a window stream
        if (!inputStream.getStreamHandlers().isEmpty() && windowDefinitionMap != null && windowDefinitionMap.containsKey(inputStream.getStreamId())) {
            for (StreamHandler handler : inputStream.getStreamHandlers()) {
                if (handler instanceof Window) {
                    throw new OperationNotSupportedException("Cannot create " + ((Window) handler).getFunction() + " window for the window stream " + inputStream.getStreamId());
                }
            }
        }

        if (!inputStream.getStreamHandlers().isEmpty()) {
            for (StreamHandler handler : inputStream.getStreamHandlers()) {
                Processor currentProcessor = generateProcessor(handler, metaComplexEvent, variableExpressionExecutors, executionPlanContext, eventTableMap, supportsBatchProcessing, outputExpectsExpiredEvents, queryName);
                if (currentProcessor instanceof SchedulingProcessor) {
                    if (entryValveProcessor == null) {

                        entryValveProcessor = new EntryValveProcessor(executionPlanContext);
                        if (first) {
                            processor = entryValveProcessor;
                            first = false;
                        } else {
                            processor.setToLast(entryValveProcessor);
                        }
                    }
                    Scheduler scheduler = SchedulerParser.parse(executionPlanContext.getScheduledExecutorService(), entryValveProcessor, executionPlanContext);
                    ((SchedulingProcessor) currentProcessor).setScheduler(scheduler);
                }
                if (first) {
                    processor = currentProcessor;
                    first = false;
                } else {
                    processor.setToLast(currentProcessor);
                }
            }
        }

        metaStreamEvent.initializeAfterWindowData();
        return new SingleStreamRuntime(processStreamReceiver, processor, metaComplexEvent);

    }

    public static Processor generateProcessor(StreamHandler handler, MetaComplexEvent metaEvent, List<VariableExpressionExecutor> variableExpressionExecutors,
                                              ExecutionPlanContext executionPlanContext, Map<String, EventTable> eventTableMap,
                                              boolean supportsBatchProcessing, boolean outputExpectsExpiredEvents, String queryName,
                                              GpuProcessStreamReceiver gpuProcessStreamReceiver, GpuQueryContext gpuQueryContext) {

        ExpressionExecutor[] inputExpressions = new ExpressionExecutor[handler.getParameters().length];
        Expression[] parameters = handler.getParameters();
        MetaStreamEvent metaStreamEvent;
        int stateIndex = SiddhiConstants.UNKNOWN_STATE;
        if (metaEvent instanceof MetaStateEvent) {
            stateIndex = ((MetaStateEvent) metaEvent).getStreamEventCount() - 1;
            metaStreamEvent = ((MetaStateEvent) metaEvent).getMetaStreamEvent(stateIndex);
        } else {
            metaStreamEvent = (MetaStreamEvent) metaEvent;
        }
        for (int i = 0, parametersLength = parameters.length; i < parametersLength; i++) {
            inputExpressions[i] = ExpressionParser.parseExpression(parameters[i], metaEvent, stateIndex, eventTableMap,
                    variableExpressionExecutors, executionPlanContext, false, SiddhiConstants.LAST, queryName);
        }

//        String streamId = gpuProcessStreamReceiver.getStreamId();
//        GpuQueryProcessor gpuQueryProcessor = gpuProcessStreamReceiver.getGpuQueryProcessor();
//        SiddhiGpu.GpuQueryRuntime gpuQueryRuntime = gpuQueryProcessor.getGpuQueryRuntime();

        if (handler instanceof Filter) {

            // add filter processor
            GpuExpressionParser gpuExpressionParser = new GpuExpressionParser();
            SiddhiGpu.GpuFilterProcessor gpuFilterProcessor = gpuExpressionParser.parseFilterExpression(parameters[0], metaEvent, stateIndex, executionPlanContext, gpuQueryContext);
            gpuFilterProcessor.SetThreadBlockSize(gpuQueryContext.getThreadsPerBlock());
            gpuProcessStreamReceiver.addGpuProcessor(gpuFilterProcessor);
//            gpuQueryRuntime.AddProcessor(streamId, gpuFilterProcessor);
//            gpuQueryProcessor.AddGpuProcessor(gpuFilterProcessor);

            return new FilterProcessor(inputExpressions[0]);

        } else if (handler instanceof Window) {
            WindowProcessor windowProcessor = (WindowProcessor) SiddhiClassLoader.loadSiddhiImplementation(((Window) handler).getFunction(),
                    WindowProcessor.class);
            windowProcessor.initProcessor(metaStreamEvent.getLastInputDefinition(), inputExpressions, executionPlanContext, outputExpectsExpiredEvents, queryName);

            // TODO: only support length window
            if(windowProcessor instanceof LengthWindowProcessor)
            {
                LengthWindowProcessor lengthWindowProcessor = (LengthWindowProcessor) windowProcessor;

                SiddhiGpu.GpuLengthSlidingWindowProcessor gpuLengthWindowProcessor =
                        new SiddhiGpu.GpuLengthSlidingWindowProcessor(lengthWindowProcessor.getLength());
                gpuLengthWindowProcessor.SetThreadBlockSize(gpuQueryContext.getThreadsPerBlock());
                gpuProcessStreamReceiver.addGpuProcessor(gpuLengthWindowProcessor);
//                gpuQueryRuntime.AddProcessor(streamId, gpuLengthWindowProcessor);
//                gpuQueryProcessor.AddGpuProcessor(gpuLengthWindowProcessor);
            }
            else
            {
                throw new OperationNotSupportedException(windowProcessor.getClass().getSimpleName() + " not supported in GPU processing");
            }

            return windowProcessor;

        } else if (handler instanceof StreamFunction) {
            StreamFunctionProcessor streamProcessor = (StreamFunctionProcessor) SiddhiClassLoader.loadSiddhiImplementation(
                    ((StreamFunction) handler).getFunction(), StreamFunctionProcessor.class);
            metaStreamEvent.addInputDefinition(streamProcessor.initProcessor(metaStreamEvent.getLastInputDefinition(),
                    inputExpressions, executionPlanContext, outputExpectsExpiredEvents, queryName));
            return streamProcessor;

        } else {
            throw new IllegalStateException(handler.getClass().getName() + " is not supported");
        }
    }

    public static Processor generateProcessor(StreamHandler streamHandler, MetaComplexEvent metaEvent, List<VariableExpressionExecutor> variableExpressionExecutors, ExecutionPlanContext executionPlanContext, Map<String, EventTable> eventTableMap, boolean supportsBatchProcessing, boolean outputExpectsExpiredEvents, String queryName) {
        Expression[] parameters = streamHandler.getParameters();
        MetaStreamEvent metaStreamEvent;
        int stateIndex = SiddhiConstants.UNKNOWN_STATE;
        if (metaEvent instanceof MetaStateEvent) {
            stateIndex = ((MetaStateEvent) metaEvent).getStreamEventCount() - 1;
            metaStreamEvent = ((MetaStateEvent) metaEvent).getMetaStreamEvent(stateIndex);
        } else {
            metaStreamEvent = (MetaStreamEvent) metaEvent;
        }

        ExpressionExecutor[] attributeExpressionExecutors;
        if (parameters != null) {
            if (parameters.length > 0) {
                attributeExpressionExecutors = new ExpressionExecutor[parameters.length];
                for (int i = 0, parametersLength = parameters.length; i < parametersLength; i++) {
                    attributeExpressionExecutors[i] = ExpressionParser.parseExpression(parameters[i], metaEvent, stateIndex, eventTableMap, variableExpressionExecutors,
                            executionPlanContext, false, SiddhiConstants.CURRENT, queryName);
                }
            } else {
                List<Attribute> attributeList = metaStreamEvent.getLastInputDefinition().getAttributeList();
                int parameterSize = attributeList.size();
                attributeExpressionExecutors = new ExpressionExecutor[parameterSize];
                for (int i = 0; i < parameterSize; i++) {
                    attributeExpressionExecutors[i] = ExpressionParser.parseExpression(new Variable(attributeList.get(i).getName()), metaEvent, stateIndex, eventTableMap, variableExpressionExecutors,
                            executionPlanContext, false, SiddhiConstants.CURRENT, queryName);
                }
            }
        } else {
            attributeExpressionExecutors = new ExpressionExecutor[0];
        }

        if (streamHandler instanceof Filter) {
            return new FilterProcessor(attributeExpressionExecutors[0]);

        } else if (streamHandler instanceof Window) {
            WindowProcessor windowProcessor;
            if (streamHandler instanceof Extension) {
                windowProcessor = (WindowProcessor) SiddhiClassLoader.loadExtensionImplementation((Extension) streamHandler,
                        WindowProcessorExtensionHolder.getInstance(executionPlanContext));
            } else {
                windowProcessor = (WindowProcessor) SiddhiClassLoader.loadSiddhiImplementation(((Window) streamHandler).getFunction(),
                        WindowProcessor.class);
            }
            windowProcessor.initProcessor(metaStreamEvent.getLastInputDefinition(), attributeExpressionExecutors, executionPlanContext, outputExpectsExpiredEvents, queryName);
            return windowProcessor;

        } else if (streamHandler instanceof StreamFunction) {
            AbstractStreamProcessor abstractStreamProcessor;
            if (supportsBatchProcessing) {
                try {
                    if (streamHandler instanceof Extension) {
                        abstractStreamProcessor = (StreamProcessor) SiddhiClassLoader.loadExtensionImplementation((Extension) streamHandler,
                                StreamProcessorExtensionHolder.getInstance(executionPlanContext));
                    } else {
                        abstractStreamProcessor = (StreamProcessor) SiddhiClassLoader.loadSiddhiImplementation(
                                ((StreamFunction) streamHandler).getFunction(), StreamProcessor.class);
                    }
                    metaStreamEvent.addInputDefinition(abstractStreamProcessor.initProcessor(metaStreamEvent.getLastInputDefinition(),
                            attributeExpressionExecutors, executionPlanContext, outputExpectsExpiredEvents, queryName));
                    return abstractStreamProcessor;
                } catch (ExecutionPlanCreationException e) {
                    if (!e.isClassLoadingIssue()) {
                        throw e;
                    }
                }
            }
            if (streamHandler instanceof Extension) {
                abstractStreamProcessor = (StreamFunctionProcessor) SiddhiClassLoader.loadExtensionImplementation((Extension) streamHandler,
                        StreamFunctionProcessorExtensionHolder.getInstance(executionPlanContext));
            } else {
                abstractStreamProcessor = (StreamFunctionProcessor) SiddhiClassLoader.loadSiddhiImplementation(
                        ((StreamFunction) streamHandler).getFunction(), StreamFunctionProcessor.class);
            }
            metaStreamEvent.addInputDefinition(abstractStreamProcessor.initProcessor(metaStreamEvent.getLastInputDefinition(),
                    attributeExpressionExecutors, executionPlanContext, outputExpectsExpiredEvents, queryName));
            return abstractStreamProcessor;
        } else {
            throw new IllegalStateException(streamHandler.getClass().getName() + " is not supported");
        }
    }

    /**
     * Method to generate MetaStreamEvent reagent to the given input stream. Empty definition will be created and
     * definition and reference is will be set accordingly in this method.
     *
     * @param inputStream         InputStream
     * @param streamDefinitionMap StreamDefinition Map
     * @param tableDefinitionMap  TableDefinition Map
     * @param metaStreamEvent     MetaStreamEvent
     */
    private static void initMetaStreamEvent(SingleInputStream inputStream, Map<String,
            AbstractDefinition> streamDefinitionMap, Map<String, AbstractDefinition> tableDefinitionMap, Map<String, AbstractDefinition> windowDefinitionMap, MetaStreamEvent metaStreamEvent) {

        String streamId = inputStream.getStreamId();

        if (!inputStream.isInnerStream() && windowDefinitionMap != null && windowDefinitionMap.containsKey(streamId)) {
            AbstractDefinition inputDefinition = windowDefinitionMap.get(streamId);
            if (!metaStreamEvent.getInputDefinitions().contains(inputDefinition)) {
                metaStreamEvent.addInputDefinition(inputDefinition);
            }
        } else if (streamDefinitionMap != null && streamDefinitionMap.containsKey(streamId)) {
            AbstractDefinition inputDefinition = streamDefinitionMap.get(streamId);
            metaStreamEvent.addInputDefinition(inputDefinition);
        } else if (!inputStream.isInnerStream() && tableDefinitionMap != null && tableDefinitionMap.containsKey(streamId)) {
            AbstractDefinition inputDefinition = tableDefinitionMap.get(streamId);
            metaStreamEvent.addInputDefinition(inputDefinition);
        } else {
            throw new ExecutionPlanCreationException("Stream/table definition with ID '" + inputStream.getStreamId() + "' has not been defined");
        }

        if ((inputStream.getStreamReferenceId() != null) &&
                !(inputStream.getStreamId()).equals(inputStream.getStreamReferenceId())) { //if ref id is provided
            metaStreamEvent.setInputReferenceId(inputStream.getStreamReferenceId());
        }
    }


}
