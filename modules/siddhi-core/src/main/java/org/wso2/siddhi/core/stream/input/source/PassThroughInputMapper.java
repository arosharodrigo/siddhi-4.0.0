/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package org.wso2.siddhi.core.stream.input.source;

import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.query.api.execution.io.map.AttributeMapping;
import org.wso2.siddhi.core.util.transport.OptionHolder;
import org.wso2.siddhi.query.api.definition.StreamDefinition;

import java.util.List;

@Extension(
        name = "passThrough",
        namespace = "inputmapper",
        description = "Pass-through mapper passed events (Event[]) through without any mapping or modifications."
)
public class PassThroughInputMapper extends InputMapper {
    private static final Logger log = Logger.getLogger(PassThroughInputMapper.class);

    @Override
    public void init(StreamDefinition streamDefinition, OptionHolder optionHolder, List<AttributeMapping> attributeMappingList) {

    }

    @Override
    protected void mapAndProcess(Object eventObject, InputHandler inputHandler) throws InterruptedException {
        if (eventObject != null) {
            if (eventObject instanceof Event[]) {
                inputHandler.send((Event[]) eventObject);
            } else if (eventObject instanceof Event) {
                inputHandler.send((Event) eventObject);
            } else if (eventObject instanceof Object[]) {
                inputHandler.send((Object[]) eventObject);
            } else {
                throw new ExecutionPlanRuntimeException("Event object must be either Event[], Event or Object[] " +
                        "but found " + eventObject.getClass().getCanonicalName());
            }
        }
    }
}