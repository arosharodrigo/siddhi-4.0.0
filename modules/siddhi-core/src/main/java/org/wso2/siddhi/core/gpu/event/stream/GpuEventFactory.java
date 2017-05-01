package org.wso2.siddhi.core.gpu.event.stream;

import com.lmax.disruptor.EventFactory;
import org.wso2.siddhi.core.gpu.util.ByteBufferWriter;


public class GpuEventFactory implements EventFactory<GpuEvent> {

    private GpuMetaStreamEvent metaEvent;
    private ByteBufferWriter bufferWriter;
    
    public GpuEventFactory(GpuMetaStreamEvent metaEvent, ByteBufferWriter bufferWriter) {
        this.metaEvent = metaEvent;
        this.bufferWriter = bufferWriter;
    }

    public GpuEvent newInstance() {
        return new GpuEvent(metaEvent, bufferWriter);
    }
}
