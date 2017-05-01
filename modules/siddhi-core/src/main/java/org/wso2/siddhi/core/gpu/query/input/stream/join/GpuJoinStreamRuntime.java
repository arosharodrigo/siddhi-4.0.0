package org.wso2.siddhi.core.gpu.query.input.stream.join;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.state.MetaStateEvent;
import org.wso2.siddhi.core.gpu.query.selector.GpuJoinQuerySelector;
import org.wso2.siddhi.core.query.input.stream.join.JoinStreamRuntime;
import org.wso2.siddhi.core.query.input.stream.single.SingleStreamRuntime;
import org.wso2.siddhi.core.query.processor.Processor;

public class GpuJoinStreamRuntime extends JoinStreamRuntime {
    
    public GpuJoinStreamRuntime(ExecutionPlanContext executionPlanContext, MetaStateEvent metaStateEvent) {
        super(executionPlanContext, metaStateEvent);
    }
    
//    public void addRuntime(GpuStreamRuntime singleStreamRuntime) {
//        singleStreamRuntimeList.add(singleStreamRuntime);
//        
//        if(gpuQueryProcessor == null) {
////            gpuQueryProcessor = singleStreamRuntime.getProcessStreamReceiver();
//        }
//    }
    
    @Override
    public void setCommonProcessor(Processor commonProcessor) {
        if(commonProcessor instanceof GpuJoinQuerySelector) {
            GpuJoinQuerySelector joinQuerySelector = (GpuJoinQuerySelector)commonProcessor;
            
            int iIndex = 0;
            for (SingleStreamRuntime singleStreamRuntime : getSingleStreamRuntimes()) {
                singleStreamRuntime.setCommonProcessor(joinQuerySelector.clone(Integer.toString(iIndex)));
                iIndex++;
            }
        } else {
            for (SingleStreamRuntime singleStreamRuntime : getSingleStreamRuntimes()) {
                singleStreamRuntime.setCommonProcessor(commonProcessor);
            }
        }
    }
}
