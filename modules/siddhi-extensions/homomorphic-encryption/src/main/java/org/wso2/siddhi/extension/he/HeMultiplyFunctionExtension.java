package org.wso2.siddhi.extension.he;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.extension.he.api.HomomorphicEncDecService;
import org.wso2.siddhi.extension.he.api.HomomorphicEncryptionEvaluation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import util.Properties;

import java.util.concurrent.atomic.AtomicReference;

public class HeMultiplyFunctionExtension extends FunctionExecutor {

    Attribute.Type returnType = Attribute.Type.STRING;
    private HomomorphicEncryptionEvaluation heEval;
    private HomomorphicEncDecService homomorphicEncDecService;

    private AtomicReference<String> encryptedOperand = new AtomicReference<String>("");

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
        String keyFilePath = Properties.PROP.getProperty("key.file.path", "/home/arosha/helib-keys");
        heEval.init(keyFilePath);
        homomorphicEncDecService.init(keyFilePath);
    }

    @Override
    protected Object execute(Object[] data) {
        String param1 = (String)data[0];
        if(encryptedOperand.get().equals("")) {
            String param2 = homomorphicEncDecService.encryptLong((Long)data[1]);
            encryptedOperand = new AtomicReference<String>(param2);
        }
        String result = heEval.evaluateMultiply(param1, encryptedOperand.get());
        return result;
    }

    @Override
    protected Object execute(Object data) {
        return null;
    }

}
