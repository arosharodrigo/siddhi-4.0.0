package org.wso2.siddhi.extension.he;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.exception.ExecutionPlanRuntimeException;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.extension.he.api.HomomorphicEncryptionEvaluation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class CompareEqualFunctionExtension extends FunctionExecutor {

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
                    "Invalid no of arguments passed to he:compareEqualIntInt() function, " + "required 2, but found "
                            + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.INT) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the first argument of he:compareEqualIntInt() function, "
                            + "required " + Attribute.Type.INT + ", but found "
                            + attributeExpressionExecutors[0].getReturnType().toString());
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.INT) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the second argument of he:compareEqualIntInt()) function, "
                            + "required " + Attribute.Type.INT + ", but found "
                            + attributeExpressionExecutors[1].getReturnType().toString());
        }

    }

    @Override
    protected Object execute(Object[] data) {
        if (data[0] == null) {
            throw new ExecutionPlanRuntimeException(
                    "Invalid input given to he:compareEqualIntInt() function. First argument cannot be null");
        }
        int volume = (Integer) data[0];
        int compareTo = (Integer) data[1];

        HomomorphicEncryptionEvaluation heEval = new HomomorphicEncryptionEvaluation();
        return heEval.compareEqualIntInt(volume, compareTo);
    }

    @Override
    protected Object execute(Object data) {
        return null;
    }
    
}
