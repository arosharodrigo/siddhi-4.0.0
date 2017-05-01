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

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.event.stream.MetaStreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventPool;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverter;
import org.wso2.siddhi.core.event.stream.converter.StreamEventConverterFactory;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.util.statistics.LatencyTracker;

import java.util.ArrayList;
import java.util.List;

public class MultiProcessStreamReceiver extends ProcessStreamReceiver {

    private static final Logger log = Logger.getLogger(MultiProcessStreamReceiver.class);

    protected Processor[] nextProcessors;
    private MetaStreamEvent[] metaStreamEvents;
    private StreamEventPool[] streamEventPools;
    private StreamEventConverter[] streamEventConverters;
    protected int processCount;
    private List<Event> eventBuffer = new ArrayList<Event>(0);
    protected int[] eventSequence;
    protected String queryName;

    public MultiProcessStreamReceiver(String streamId, int processCount, LatencyTracker latencyTracker, String queryName) {
        super(streamId, latencyTracker, queryName);
        this.processCount = processCount;
        this.queryName = queryName;
        nextProcessors = new Processor[processCount];
        metaStreamEvents = new MetaStreamEvent[processCount];
        streamEventPools = new StreamEventPool[processCount];
        streamEventConverters = new StreamEventConverter[processCount];
        eventSequence = new int[processCount];
        for (int i = 0; i < eventSequence.length; i++) {
            eventSequence[i] = i;
        }

    }

    public MultiProcessStreamReceiver clone(String key) {
        return new MultiProcessStreamReceiver(streamId + key, processCount, latencyTracker, queryName);
    }

    private void process(int eventSequence, StreamEvent borrowedEvent) {
        if (lockWrapper != null) {
            lockWrapper.lock();
        }
        try {
            if (latencyTracker != null) {
                try {
                    latencyTracker.markIn();
                    processAndClear(eventSequence, borrowedEvent);
                } finally {
                    latencyTracker.markOut();
                }
            } else {
                processAndClear(eventSequence, borrowedEvent);
            }
        } finally {
            if (lockWrapper != null) {
                lockWrapper.unlock();
            }
        }
    }

    @Override
    public void receive(ComplexEvent complexEvent) {
//        log.info("Calling void receive(ComplexEvent complexEvents): " + complexEvent);
        long startTime = System.nanoTime();
        ComplexEvent aComplexEvent = complexEvent;
        while (aComplexEvent != null) {
            synchronized (this) {
                stabilizeStates();
                for (int anEventSequence : eventSequence) {
                    StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                    StreamEventPool aStreamEventPool = streamEventPools[anEventSequence];
                    StreamEvent borrowedEvent = aStreamEventPool.borrowEvent();
                    aStreamEventConverter.convertComplexEvent(aComplexEvent, borrowedEvent);
                    process(anEventSequence, borrowedEvent);

                }
            }
            aComplexEvent = aComplexEvent.getNext();
        }
        long endTime = System.nanoTime();
        markStat(startTime, endTime, 1);
    }

    @Override
    public void receive(Event event) {
//        log.info("Calling void receive(Event event): " + event);
        long startTime = System.nanoTime();
        synchronized (this) {
            stabilizeStates();
            for (int anEventSequence : eventSequence) {
                StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                StreamEventPool aStreamEventPool = streamEventPools[anEventSequence];
                StreamEvent borrowedEvent = aStreamEventPool.borrowEvent();
                aStreamEventConverter.convertEvent(event, borrowedEvent);
                process(anEventSequence, borrowedEvent);
            }
        }
        long endTime = System.nanoTime();
        markStat(startTime, endTime, 1);
    }

    @Override
    public void receive(Event[] events) {
//        log.info("Calling void receive(Event[] events): " + events);
        long startTime = System.nanoTime();
        for (Event event : events) {
            synchronized (this) {
                stabilizeStates();
                for (int anEventSequence : eventSequence) {
                    StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                    StreamEventPool aStreamEventPool = streamEventPools[anEventSequence];
                    StreamEvent borrowedEvent = aStreamEventPool.borrowEvent();
                    aStreamEventConverter.convertEvent(event, borrowedEvent);
                    process(anEventSequence, borrowedEvent);
                }
            }
        }
        long endTime = System.nanoTime();
        markStat(startTime, endTime, events.length);
    }

    @Override
    public void receive(Event event, boolean endOfBatch) {
//        log.info("Calling void receive(Event event, boolean endOfBatch): " + event + ", " + endOfBatch);
        long tempCurrentEventCount = getCurrentEventCount().incrementAndGet();
        eventBuffer.add(event);
        if (endOfBatch) {
            long startTime = System.nanoTime();
            for (Event aEvent : eventBuffer) {
                synchronized (this) {
                    stabilizeStates();
                    for (int anEventSequence : eventSequence) {
                        StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                        StreamEventPool aStreamEventPool = streamEventPools[anEventSequence];
                        StreamEvent borrowedEvent = aStreamEventPool.borrowEvent();
                        aStreamEventConverter.convertEvent(aEvent, borrowedEvent);
                        process(anEventSequence, borrowedEvent);
                    }
                }
            }
            eventBuffer.clear();
            long endTime = System.nanoTime();
            double avgThroughput = tempCurrentEventCount * 1000000000 / (endTime - startTime);
            log.info("<" + queryName + "> " + tempCurrentEventCount + " Batch Throughput : " + DECIMAL_FORMAT.format(avgThroughput) + " eps");
            getThroughputStatistics().addValue(avgThroughput);
            getCurrentEventCount().set(0);
        }
    }

    @Override
    public void receive(long timeStamp, Object[] data) {
//        log.info("Calling void receive(long timeStamp, Object[] data): " + timeStamp + ", " + data);
        long startTime = System.nanoTime();
        synchronized (this) {
            stabilizeStates();
            for (int anEventSequence : eventSequence) {
                StreamEventConverter aStreamEventConverter = streamEventConverters[anEventSequence];
                StreamEventPool aStreamEventPool = streamEventPools[anEventSequence];
                StreamEvent borrowedEvent = aStreamEventPool.borrowEvent();
                aStreamEventConverter.convertData(timeStamp, data, borrowedEvent);
                process(anEventSequence, borrowedEvent);
            }
        }
        long endTime = System.nanoTime();
        markStat(startTime, endTime, 1);
    }

    protected void processAndClear(int processIndex, StreamEvent streamEvent) {
        ComplexEventChunk<StreamEvent> currentStreamEventChunk = new ComplexEventChunk<StreamEvent>(streamEvent, streamEvent, batchProcessingAllowed);
        nextProcessors[processIndex].process(currentStreamEventChunk);
    }

    protected void stabilizeStates() {

    }


    public void setNext(Processor nextProcessor) {
        for (int i = 0, nextLength = nextProcessors.length; i < nextLength; i++) {
            Processor processor = nextProcessors[i];
            if (processor == null) {
                nextProcessors[i] = nextProcessor;
                break;
            }
        }
    }

    public void setMetaStreamEvent(MetaStreamEvent metaStreamEvent) {
        for (int i = 0, nextLength = metaStreamEvents.length; i < nextLength; i++) {
            MetaStreamEvent streamEvent = metaStreamEvents[i];
            if (streamEvent == null) {
                metaStreamEvents[i] = metaStreamEvent;
                break;
            }
        }
    }

    @Override
    public boolean toTable() {
        return metaStreamEvents[0].isTableEvent();
    }

    public void setStreamEventPool(StreamEventPool streamEventPool) {
        for (int i = 0, nextLength = streamEventPools.length; i < nextLength; i++) {
            StreamEventPool eventPool = streamEventPools[i];
            if (eventPool == null) {
                streamEventPools[i] = streamEventPool;
                break;
            }
        }
    }

    public void init() {

        for (int i = 0, nextLength = streamEventConverters.length; i < nextLength; i++) {
            StreamEventConverter streamEventConverter = streamEventConverters[i];
            if (streamEventConverter == null) {
                streamEventConverters[i] = StreamEventConverterFactory.constructEventConverter(metaStreamEvents[i]);
                break;
            }
        }
    }
}
