package org.wso2.siddhi.core.gpu.query.input.stream;

import org.wso2.siddhi.core.event.MetaComplexEvent;
import org.wso2.siddhi.core.gpu.query.input.GpuProcessStreamReceiver;
import org.wso2.siddhi.core.gpu.query.processor.GpuQueryProcessor;
import org.wso2.siddhi.core.query.input.ProcessStreamReceiver;
import org.wso2.siddhi.core.query.input.stream.StreamRuntime;
import org.wso2.siddhi.core.query.input.stream.single.SingleStreamRuntime;
//import org.wso2.siddhi.core.query.input.stream.single.SingleThreadEntryValveProcessor;
import org.wso2.siddhi.core.query.output.ratelimit.OutputRateLimiter;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.TimeWindowProcessor;
import org.wso2.siddhi.core.query.selector.QuerySelector;

import java.util.ArrayList;
import java.util.List;

public class GpuStreamRuntime extends SingleStreamRuntime {
    
    private GpuQueryProcessor gpuQueryProcessor;

    public GpuStreamRuntime(ProcessStreamReceiver processStreamReceiver, Processor processorChain, MetaComplexEvent metaComplexEvent) {
        super(processStreamReceiver, processorChain, metaComplexEvent);
        gpuQueryProcessor = ((GpuProcessStreamReceiver)processStreamReceiver).getGpuQueryProcessor();
    }
    
    @Override
    public List<SingleStreamRuntime> getSingleStreamRuntimes() {
        List<SingleStreamRuntime> list = new ArrayList<SingleStreamRuntime>(1);
        list.add(this);
        return list;
    }

    @Override
    public StreamRuntime clone(String key) {
        ProcessStreamReceiver clonedProcessStreamReceiver = this.getProcessStreamReceiver().clone(key);
//        SingleThreadEntryValveProcessor singleThreadEntryValveProcessor = null;
        TimeWindowProcessor windowProcessor;
        Processor clonedProcessorChain = null;
        Processor processorChain = getProcessorChain();
        if (processorChain != null) {
            if (!(processorChain instanceof QuerySelector || processorChain instanceof OutputRateLimiter)) {
//                clonedProcessorChain = processorChain.cloneProcessor();
//                if(clonedProcessorChain instanceof SingleThreadEntryValveProcessor){
//                    singleThreadEntryValveProcessor = (SingleThreadEntryValveProcessor) clonedProcessorChain;
//                }
            }
            Processor processor = processorChain.getNextProcessor();
            while (processor != null) {
                if (!(processor instanceof QuerySelector || processor instanceof OutputRateLimiter)) {
//                    Processor clonedProcessor = processor.cloneProcessor();
//                    clonedProcessorChain.setToLast(clonedProcessor);
//                    if(clonedProcessor instanceof SingleThreadEntryValveProcessor){
//                        singleThreadEntryValveProcessor = (SingleThreadEntryValveProcessor) clonedProcessor;
//                    } else if(clonedProcessor instanceof TimeWindowProcessor){
//                        windowProcessor = (TimeWindowProcessor) clonedProcessor;
//                        windowProcessor.cloneScheduler((TimeWindowProcessor) processor,singleThreadEntryValveProcessor);
//                    }
                }
                processor = processor.getNextProcessor();
            }
        }
        return new SingleStreamRuntime(clonedProcessStreamReceiver, clonedProcessorChain, getMetaComplexEvent());
    }

    @Override
    public void setCommonProcessor(Processor commonProcessor) {
        if (getProcessorChain() == null) {
            getProcessStreamReceiver().setNext(commonProcessor);
        } else {
            getProcessStreamReceiver().setNext(getProcessorChain());
            getProcessorChain().setToLast(commonProcessor);
        }
        
        ((GpuProcessStreamReceiver)getProcessorChain()).setSelectProcessor(commonProcessor);
    }
}
