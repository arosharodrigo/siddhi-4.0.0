package org.wso2.siddhi.core.gpu.event.stream;

import org.wso2.siddhi.core.exception.DefinitionNotExistException;
import org.wso2.siddhi.core.exception.OperationNotSupportedException;
import org.wso2.siddhi.core.gpu.config.GpuQueryContext;
import org.wso2.siddhi.core.gpu.event.GpuMetaEvent;
import org.wso2.siddhi.core.gpu.event.stream.GpuMetaStreamEvent.GpuEventAttribute.Type;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.execution.query.input.stream.BasicSingleInputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.InputStream;
import org.wso2.siddhi.query.api.execution.query.input.stream.SingleInputStream;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GpuMetaStreamEvent implements GpuMetaEvent {
    private int streamIndex;
    private String streamId;
    private AbstractDefinition inputDefinition;
    private StreamDefinition outputStreamDefinition;
    private List<GpuEventAttribute> attributes = new ArrayList<GpuEventAttribute>();
    private int eventSizeInBytes;
    private Map<String, Integer> stringAttributeSizes = null;
    
    
    public static class GpuEventAttribute {
        public enum Type {
            BOOL, INT, LONG, FLOAT, DOUBLE, STRING
        }
        
        public String name;
        public Type type;
        public int length;
        public int position;
        
        @Override
        public String toString() {
            return "GpuEventAttribute [name=" + name + ", type=" + type + ", length=" + length + ", position=" + position + "]";
        }
        
        
    }
    
    public GpuMetaStreamEvent(InputStream inputStream, Map<String, AbstractDefinition> definitionMap, GpuQueryContext gpuQueryContext) {
        
        if (inputStream instanceof BasicSingleInputStream || inputStream instanceof SingleInputStream) {
            SingleInputStream singleInputStream = (SingleInputStream) inputStream;
            streamId = singleInputStream.getStreamId();
            if(singleInputStream.isInnerStream()){
                streamId = "#".concat(streamId);
            }
            if (definitionMap != null && definitionMap.containsKey(streamId)) {
                inputDefinition = definitionMap.get(streamId);
            } else {
                throw new DefinitionNotExistException("Stream definition with stream ID '" + singleInputStream.getStreamId() + "' has not been defined");
            }
            
            streamId = streamId + (singleInputStream.getStreamReferenceId() != null ? "_" + singleInputStream.getStreamReferenceId() : "");
            
            processGpuQueryContext(gpuQueryContext);
            processAbastractDefinition();            
            
        } else {
            throw new OperationNotSupportedException("GpuMetaStreamEvent can only be created using SingleInputStream");
        }
    }
    
    public GpuMetaStreamEvent(String streamId, AbstractDefinition abstractDefinition, GpuQueryContext gpuQueryContext) {
        this.streamId = streamId;
        this.inputDefinition = abstractDefinition;
        
        processGpuQueryContext(gpuQueryContext);
        processAbastractDefinition();   
    }

    public String getStreamId() {
        return streamId;
    }

    public void setStreamId(String streamId) {
        this.streamId = streamId;
    }

    public AbstractDefinition getInputDefinition() {
        return inputDefinition;
    }

    public void setInputDefinition(AbstractDefinition inputDefinition) {
        this.inputDefinition = inputDefinition;
    }

    public StreamDefinition getOutputStreamDefinition() {
        return outputStreamDefinition;
    }

    public void setOutputStreamDefinition(StreamDefinition outputStreamDefinition) {
        this.outputStreamDefinition = outputStreamDefinition;
    }

    public List<GpuEventAttribute> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<GpuEventAttribute> attributes) {
        this.attributes = attributes;
    }

    public int getEventSizeInBytes() {
        return eventSizeInBytes;
    }

    public void setEventSizeInBytes(int eventSizeInBytes) {
        this.eventSizeInBytes = eventSizeInBytes;
    }

    public int getStringMaxSize(String attributeName) {
        return stringAttributeSizes.get(attributeName);
    }

    public void setStringMaxSize(String attributeName, int stringMaxSize) {
        this.stringAttributeSizes.put(attributeName, stringMaxSize);
    }
    
    public int getStreamIndex() {
        return streamIndex;
    }

    public void setStreamIndex(int streamIndex) {
        this.streamIndex = streamIndex;
    }
    
    private void processGpuQueryContext(GpuQueryContext gpuQueryContext) {
        String stringAttributeSizesAnnotation = gpuQueryContext.getStringAttributeSizes();
        if(stringAttributeSizesAnnotation != null) {
            String[] tokens = stringAttributeSizesAnnotation.split(",");
            if (tokens.length > 0) {
                this.stringAttributeSizes = new HashMap<String, Integer>();

                for (String token : tokens) {
                    String[] keyVal = token.split("=");
                    this.stringAttributeSizes.put(keyVal[0], Integer.parseInt(keyVal[1]));
                }
            }
        }
    }
    
    private void processAbastractDefinition() {
        
        int position = 8 + 8 + 2; // time + seq + type
        for(Attribute attrib : inputDefinition.getAttributeList()) {
            GpuEventAttribute gpuAttrib = new GpuEventAttribute();
            gpuAttrib.name = attrib.getName();
            
            switch(attrib.getType()) {
            case BOOL:
                gpuAttrib.type = Type.BOOL;
                gpuAttrib.length = 2;
                gpuAttrib.position = position;
                position += gpuAttrib.length; 
                break;
            case INT:
                gpuAttrib.type = Type.INT;
                gpuAttrib.length = 4;
                gpuAttrib.position = position;
                position += gpuAttrib.length; 
                break;
            case LONG:
                gpuAttrib.type = Type.LONG;
                gpuAttrib.length = 8;
                gpuAttrib.position = position;
                position += gpuAttrib.length; 
                break;
            case DOUBLE:
                gpuAttrib.type = Type.DOUBLE;
                gpuAttrib.length = 8;
                gpuAttrib.position = position;
                position += gpuAttrib.length; 
                break;
            case FLOAT:
                gpuAttrib.type = Type.FLOAT;
                gpuAttrib.length = 4;
                gpuAttrib.position = position;
                position += gpuAttrib.length; 
                break;
            case STRING: {
                gpuAttrib.type = Type.STRING;
                
                int strBodyLength = 8; 
                if(stringAttributeSizes != null && stringAttributeSizes.containsKey(attrib.getName())) {
                    strBodyLength = stringAttributeSizes.get(attrib.getName());
                }
                gpuAttrib.length = strBodyLength;
                gpuAttrib.position = position;
                position += (gpuAttrib.length + 2); // + length
            }
            break;
            default:
                break;
            }
            
            attributes.add(gpuAttrib);
        }
        
        eventSizeInBytes = position;
    }
}
