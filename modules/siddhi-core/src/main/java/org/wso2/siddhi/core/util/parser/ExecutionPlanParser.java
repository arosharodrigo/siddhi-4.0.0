/*
 *
 *  * Copyright (c) 2005 - 2014, WSO2 Inc. (http://www.wso2.org)
 *  * All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.wso2.siddhi.core.util.parser;

import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.config.SiddhiContext;
import org.wso2.siddhi.core.exception.QueryCreationException;
import org.wso2.siddhi.core.partition.PartitionRuntime;
import org.wso2.siddhi.core.query.QueryRuntime;
import org.wso2.siddhi.query.api.ExecutionPlan;
import org.wso2.siddhi.query.api.annotation.Element;
import org.wso2.siddhi.query.api.definition.StreamDefinition;
import org.wso2.siddhi.query.api.exception.DuplicateAnnotationException;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.execution.ExecutionElement;
import org.wso2.siddhi.query.api.execution.partition.Partition;
import org.wso2.siddhi.query.api.execution.query.Query;
import org.wso2.siddhi.query.api.util.AnnotationHelper;

import java.util.Map;
import java.util.UUID;

public class ExecutionPlanParser {

    /**
     * Parse an ExecutionPlan returning ExecutionPlanRuntime
     *
     * @param executionPlan plan to be parsed
     * @return ExecutionPlanRuntime
     */
    public static ExecutionPlanRuntime parse(ExecutionPlan executionPlan, SiddhiContext siddhiContext) {
        ExecutionPlanRuntime executionPlanRuntime = new ExecutionPlanRuntime(siddhiContext);

        try {
            Element element = AnnotationHelper.getAnnotationElement("info", "name", executionPlan.getAnnotations());
            if (element != null) {
                executionPlanRuntime.setName(element.getValue());
            } else {
                executionPlanRuntime.setName(UUID.randomUUID().toString());
            }
        } catch (DuplicateAnnotationException e) {
            throw new ExecutionPlanValidationException(e.getMessage() + " for the same Execution Plan " +
                    executionPlan.toString());
        }

        defineStreamDefinitions(executionPlanRuntime, executionPlan.getStreamDefinitionMap());
        try {
            for (ExecutionElement executionElement : executionPlan.getExecutionElementList()) {
                if (executionElement instanceof Query) {
                    QueryRuntime queryRuntime = QueryParser.parse((Query) executionElement, siddhiContext, executionPlanRuntime.getStreamDefinitionMap());
                    executionPlanRuntime.addQuery(queryRuntime);
                } else {
                    PartitionRuntime partitionRuntime = PartitionParser.parse(executionPlanRuntime, (Partition) executionElement, siddhiContext, executionPlanRuntime.getStreamDefinitionMap());
                    executionPlanRuntime.addPartition(partitionRuntime);
                }
            }
        } catch (QueryCreationException e) {
            throw new ExecutionPlanValidationException(e.getMessage() + " in execution plan " +
                    executionPlanRuntime.getName(), e);
        }
        return executionPlanRuntime;
    }

    private static void defineStreamDefinitions(ExecutionPlanRuntime executionPlanRuntime, Map<String, StreamDefinition> streamDefinitionMap) {
        for (StreamDefinition definition : streamDefinitionMap.values()) {
            executionPlanRuntime.defineStream(definition);
        }
    }

}
