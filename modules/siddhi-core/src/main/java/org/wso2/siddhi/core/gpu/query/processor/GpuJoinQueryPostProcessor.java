package org.wso2.siddhi.core.gpu.query.processor;

import org.wso2.siddhi.core.gpu.event.stream.converter.ConversionGpuEventChunk;

import java.nio.ByteBuffer;

public class GpuJoinQueryPostProcessor extends GpuQueryPostProcessor {
    
    private int eventSegmentSize;

    public GpuJoinQueryPostProcessor(ByteBuffer outputEventBuffer, ConversionGpuEventChunk complexEventChunk, int eventSegmentSize) {
        super(outputEventBuffer, complexEventChunk);
        this.eventSegmentSize = eventSegmentSize;
    }

    @Override
    public void process(ByteBuffer inputEventBuffer, int eventCount) {
        outputEventBuffer.position(0);
        inputEventBuffer.position(0);
        
        complexEventChunk.convertAndAdd(outputEventBuffer, eventCount, eventSegmentSize);

    }

}
