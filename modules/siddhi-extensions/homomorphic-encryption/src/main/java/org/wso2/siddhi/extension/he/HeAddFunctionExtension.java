package org.wso2.siddhi.extension.he;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.extension.he.api.HomomorphicEncDecService;
import org.wso2.siddhi.extension.he.api.HomomorphicEncryptionEvaluation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

public class HeAddFunctionExtension extends FunctionExecutor {

    Attribute.Type returnType = Attribute.Type.STRING;
    private HomomorphicEncryptionEvaluation heEval;
    private HomomorphicEncDecService homomorphicEncDecService;

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
                    "Invalid no of arguments passed to he:compareEqualIntInt() function, " + "required 21, but found "
                            + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.STRING) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the first argument of he:compareEqualIntInt() function, "
                            + "required " + Attribute.Type.STRING + ", but found "
                            + attributeExpressionExecutors[0].getReturnType().toString());
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.LONG) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the second argument of he:compareEqualIntInt()) function, "
                            + "required " + Attribute.Type.LONG + ", but found "
                            + attributeExpressionExecutors[1].getReturnType().toString());
        }

        heEval = new HomomorphicEncryptionEvaluation();
        homomorphicEncDecService = new HomomorphicEncDecService();
        heEval.init("/home/arosha/helib-keys");
        homomorphicEncDecService.init("/home/arosha/helib-keys");
//        heEval.init("/home/aroshar/helib-keys");
//        homomorphicEncDecService.init("/home/aroshar/helib-keys");
//        heEval.init("/home/ubuntu/helib-keys");
//        homomorphicEncDecService.init("/home/ubuntu/helib-keys");
    }

    @Override
    protected Object execute(Object[] data) {
        String param1 = (String)data[0];
        String param2 = homomorphicEncDecService.encryptLong((Long)data[1]);
        String result = heEval.evaluateAdd(param1, param2);
        return result;
    }

    @Override
    protected Object execute(Object data) {
        return null;
    }
    
}
