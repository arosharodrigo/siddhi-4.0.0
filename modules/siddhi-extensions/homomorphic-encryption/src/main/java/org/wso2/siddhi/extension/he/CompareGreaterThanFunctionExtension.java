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
package org.wso2.siddhi.extension.he;

import org.wso2.siddhi.extension.he.api.HomomorphicEncryptionEvaluation;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

/**
 * compareGreaterThan(int, compareTo)
 * Compares two integers.
 * Accept Type(s): (INT,INT)
 * Return Type(s): BOOL
 */
public class CompareGreaterThanFunctionExtension extends FunctionExecutor {
    Attribute.Type returnType = Attribute.Type.BOOL;

    @Override
    public Attribute.Type getReturnType() {
        return returnType;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return null;
    }

    @Override
    public void restoreState(Object[] state) {

    }

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length != 2) {
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to he:compareGreaterThan() function, " + "required 2, but found "
                            + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.INT) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the first argument of he:compareGreaterThan() function, "
                            + "required " + Attribute.Type.INT + ", but found "
                            + attributeExpressionExecutors[0].getReturnType().toString());
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.INT) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the second argument of he:compareGreaterThan()) function, "
                            + "required " + Attribute.Type.INT + ", but found "
                            + attributeExpressionExecutors[1].getReturnType().toString());
        }

    }

    @Override
    protected Object execute(Object[] data) {
        if (data[0] == null) {
            throw new ExecutionPlanRuntimeException(
                    "Invalid input given to he:compareGreaterThan() function. First argument cannot be null");
        }
        int volume = (Integer) data[0];
        int compareTo = (Integer) data[1];

        HomomorphicEncryptionEvaluation heEval = new HomomorphicEncryptionEvaluation();
        return heEval.compareGreaterThanIntInt(volume, compareTo);
    }

    @Override
    protected Object execute(Object data) {
        return null;
    }

}
