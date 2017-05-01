package org.wso2.siddhi.core.gpu.query.processor;

import org.wso2.siddhi.core.gpu.event.stream.converter.ConversionGpuEventChunk;

import java.nio.ByteBuffer;

public class GpuLengthWindowQueryPostProcessor extends GpuQueryPostProcessor {

    public GpuLengthWindowQueryPostProcessor(ByteBuffer outputEventBuffer, ConversionGpuEventChunk complexEventChunk) {
        super(outputEventBuffer, complexEventChunk);
    }

    @Override
    public void process(ByteBuffer inputEventBuffer, int eventCount) {
        outputEventBuffer.position(0);
        inputEventBuffer.position(0);
        
        complexEventChunk.convertAndAdd(outputEventBuffer, eventCount);

    }

}
