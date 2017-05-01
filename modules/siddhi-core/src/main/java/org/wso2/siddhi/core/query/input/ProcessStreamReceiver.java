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
package org.wso2.siddhi.core.query.input;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.descriptive.SynchronizedSummaryStatistics;
import org.apache.log4j.Logger;
import org.wso2.siddhi.core.debugger.SiddhiDebugger;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverterFactory;
import org.wso2.siddhi.core.query.input.stream.state.PreStateProcessor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.stream.StreamJunction;
import org.wso2.siddhi.core.util.lock.LockWrapper;
import org.wso2.siddhi.core.util.statistics.LatencyTracker;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class ProcessStreamReceiver implements StreamJunction.Receiver {

    private static final Logger log = Logger.getLogger(ProcessStreamReceiver.class);

    protected String streamId;
    protected Processor next;
    protected StreamEventConverter streamEventConverter;
    protected MetaStreamEvent metaStreamEvent;
    protected StreamEventPool streamEventPool;
    protected List<PreStateProcessor> stateProcessors = new ArrayList<PreStateProcessor>();
    protected int stateProcessorsSize;
    protected LatencyTracker latencyTracker;
    protected LockWrapper lockWrapper;
    protected ComplexEventChunk<StreamEvent> batchingStreamEventChunk = new ComplexEventChunk<StreamEvent>(false);
    protected boolean batchProcessingAllowed;
    protected SiddhiDebugger siddhiDebugger;
    protected String queryName;

    private AtomicLong currentEventCount = new AtomicLong(0);
    private AtomicLong totalDuration = new AtomicLong(0);
    private final SynchronizedSummaryStatistics throughputStatistics = new SynchronizedSummaryStatistics();
    protected int performanceCalculateBatchCount;

    public static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("###.##");

    public ProcessStreamReceiver(String streamId, LatencyTracker latencyTracker, String queryName) {
        this.streamId = streamId;
        this.latencyTracker = latencyTracker;
        this.queryName = queryName;
        this.performanceCalculateBatchCount = 1000;
    }

    @Override
    public String getStreamId() {
        return streamId;
    }

    public ProcessStreamReceiver clone(String key) {
        ProcessStreamReceiver processStreamReceiver = new ProcessStreamReceiver(streamId + key, latencyTracker, queryName);
        processStreamReceiver.batchProcessingAllowed = this.batchProcessingAllowed;
        return processStreamReceiver;
    }

    public void setSiddhiDebugger(SiddhiDebugger siddhiDebugger) {
        this.siddhiDebugger = siddhiDebugger;
    }

    private void process(ComplexEventChunk<StreamEvent> streamEventChunk) {
        if (lockWrapper != null) {
            lockWrapper.lock();
        }
        try {
            if (latencyTracker != null) {
                try {
                    latencyTracker.markIn();
                    processAndClear(streamEventChunk);
                } finally {
                    latencyTracker.markOut();
                }
            } else {
                processAndClear(streamEventChunk);
            }
        } finally {
            if (lockWrapper != null) {
                lockWrapper.unlock();
            }
        }
    }

    @Override
    public void receive(ComplexEvent complexEvents) {
//        log.info("Calling void receive(ComplexEvent complexEvents): " + complexEvents);
        long startTime = System.nanoTime();
        if (siddhiDebugger != null) {
            siddhiDebugger.checkBreakPoint(queryName, SiddhiDebugger.QueryTerminal.IN, complexEvents);
        }
        StreamEvent firstEvent = streamEventPool.borrowEvent();
        streamEventConverter.convertComplexEvent(complexEvents, firstEvent);
        StreamEvent currentEvent = firstEvent;
        complexEvents = complexEvents.getNext();
        while (complexEvents != null) {
            StreamEvent nextEvent = streamEventPool.borrowEvent();
            streamEventConverter.convertComplexEvent(complexEvents, nextEvent);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
            complexEvents = complexEvents.getNext();
        }
        process(new ComplexEventChunk<StreamEvent>(firstEvent, currentEvent, this.batchProcessingAllowed));
        long endTime = System.nanoTime();
        markStat(startTime, endTime, 1);
    }

    @Override
    public void receive(Event event) {
//        log.info("Calling void receive(Event event): " + event);
        if (event != null) {
            long startTime = System.nanoTime();
            StreamEvent borrowedEvent = streamEventPool.borrowEvent();
            streamEventConverter.convertEvent(event, borrowedEvent);
            if (siddhiDebugger != null) {
                siddhiDebugger.checkBreakPoint(queryName, SiddhiDebugger.QueryTerminal.IN, borrowedEvent);
            }
            process(new ComplexEventChunk<StreamEvent>(borrowedEvent, borrowedEvent, this.batchProcessingAllowed));
            long endTime = System.nanoTime();
            markStat(startTime, endTime, 1);
        }
    }

    @Override
    public void receive(Event[] events) {
//        log.info("Calling void receive(Event[] events): " + events);
        StreamEvent firstEvent = streamEventPool.borrowEvent();
        streamEventConverter.convertEvent(events[0], firstEvent);
        StreamEvent currentEvent = firstEvent;
        for (int i = 1, eventsLength = events.length; i < eventsLength; i++) {
            StreamEvent nextEvent = streamEventPool.borrowEvent();
            streamEventConverter.convertEvent(events[i], nextEvent);
            currentEvent.setNext(nextEvent);
            currentEvent = nextEvent;
        }
        if (siddhiDebugger != null) {
            siddhiDebugger.checkBreakPoint(queryName, SiddhiDebugger.QueryTerminal.IN, firstEvent);
        }
        long startTime = System.nanoTime();
        process(new ComplexEventChunk<StreamEvent>(firstEvent, currentEvent, this.batchProcessingAllowed));
        long endTime = System.nanoTime();
        markStat(startTime, endTime, events.length);
    }


    @Override
    public void receive(Event event, boolean endOfBatch) {
//        log.info("Calling void receive(Event event, boolean endOfBatch): " + event + ", " + endOfBatch);
        StreamEvent borrowedEvent = streamEventPool.borrowEvent();
        streamEventConverter.convertEvent(event, borrowedEvent);
        ComplexEventChunk<StreamEvent> streamEventChunk = null;
        synchronized (this) {
            batchingStreamEventChunk.add(borrowedEvent);
            if (endOfBatch) {
                streamEventChunk = batchingStreamEventChunk;
                batchingStreamEventChunk = new ComplexEventChunk<StreamEvent>(this.batchProcessingAllowed);
            }
        }
        long tempCurrentEventCount = currentEventCount.incrementAndGet();
        if (streamEventChunk != null) {
            if (siddhiDebugger != null) {
                siddhiDebugger.checkBreakPoint(queryName, SiddhiDebugger.QueryTerminal.IN, streamEventChunk.getFirst());
            }
            long startTime = System.nanoTime();
            process(streamEventChunk);
            long endTime = System.nanoTime();
            double avgThroughput = tempCurrentEventCount * 1000000000 / (endTime - startTime);
            log.info("<" + queryName + "> " + tempCurrentEventCount + " Batch Throughput : " + DECIMAL_FORMAT.format(avgThroughput) + " eps");
            throughputStatistics.addValue(avgThroughput);
            currentEventCount.set(0);
        }
    }

    @Override
    public void receive(long timeStamp, Object[] data) {
//        log.info("Calling void receive(long timeStamp, Object[] data): " + timeStamp + ", " + data);
        long startTime = System.nanoTime();
        StreamEvent borrowedEvent = streamEventPool.borrowEvent();
        streamEventConverter.convertData(timeStamp, data, borrowedEvent);
        // Send to debugger
        if (siddhiDebugger != null) {
            siddhiDebugger.checkBreakPoint(queryName, SiddhiDebugger.QueryTerminal.IN, borrowedEvent);
        }
        process(new ComplexEventChunk<StreamEvent>(borrowedEvent, borrowedEvent, this.batchProcessingAllowed));
        long endTime = System.nanoTime();
        markStat(startTime, endTime, 1);
    }

    protected void processAndClear(ComplexEventChunk<StreamEvent> streamEventChunk) {
        next.process(streamEventChunk);
        streamEventChunk.clear();
    }

    public void setMetaStreamEvent(MetaStreamEvent metaStreamEvent) {
        this.metaStreamEvent = metaStreamEvent;
    }

    public boolean toTable() {
        return metaStreamEvent.isTableEvent();
    }

    public void setBatchProcessingAllowed(boolean batchProcessingAllowed) {
        this.batchProcessingAllowed = batchProcessingAllowed;
    }

    public void setNext(Processor next) {
        this.next = next;
    }

    public void setStreamEventPool(StreamEventPool streamEventPool) {
        this.streamEventPool = streamEventPool;
    }

    public void setLockWrapper(LockWrapper lockWrapper) {
        this.lockWrapper = lockWrapper;
    }

    public void init() {
        streamEventConverter = StreamEventConverterFactory.constructEventConverter(metaStreamEvent);
    }

    public void addStatefulProcessor(PreStateProcessor stateProcessor) {
        stateProcessors.add(stateProcessor);
        stateProcessorsSize = stateProcessors.size();
    }

    protected void markStat(long startTime, long endTime, long delta) {
        totalDuration.addAndGet(endTime - startTime);
        long tempCurrentEventCount = currentEventCount.addAndGet(delta);

        if(tempCurrentEventCount >= performanceCalculateBatchCount) {
            double avgThroughput = tempCurrentEventCount * 1000000000 / totalDuration.get();
            log.info("<" + queryName + "> " + tempCurrentEventCount + " Events Throughput : " + DECIMAL_FORMAT.format(avgThroughput) + " eps");
            throughputStatistics.addValue(avgThroughput);
            totalDuration.set(0);
            currentEventCount.set(0);
        }
    }

    public void printStatistics() {
        log.info(new StringBuilder()
                .append("EventProcessTroughput ExecutionPlan=").append(queryName).append("_").append(streamId)
                .append("|length=").append(throughputStatistics.getN())
                .append("|Avg=").append(DECIMAL_FORMAT.format(throughputStatistics.getMean()))
                .append("|Min=").append(DECIMAL_FORMAT.format(throughputStatistics.getMin()))
                .append("|Max=").append(DECIMAL_FORMAT.format(throughputStatistics.getMax()))
                .append("|Var=").append(DECIMAL_FORMAT.format(throughputStatistics.getVariance()))
                .append("|StdDev=").append(DECIMAL_FORMAT.format(throughputStatistics.getStandardDeviation())).toString());
    }

    public void getStatistics(List<SynchronizedSummaryStatistics> statList) {
        statList.add(throughputStatistics);
    }

    public int getPerformanceCalculateBatchCount() {
        return performanceCalculateBatchCount;
    }

    public void setPerformanceCalculateBatchCount(int performanceCalculateBatchCount) {
        this.performanceCalculateBatchCount = performanceCalculateBatchCount;
    }

    public AtomicLong getCurrentEventCount() {
        return currentEventCount;
    }

    public SynchronizedSummaryStatistics getThroughputStatistics() {
        return throughputStatistics;
    }

}
