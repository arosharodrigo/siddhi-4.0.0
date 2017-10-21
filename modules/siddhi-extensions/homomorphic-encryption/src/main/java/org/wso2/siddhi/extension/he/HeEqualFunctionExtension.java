package org.wso2.siddhi.extension.he;

import org.apache.log4j.Logger;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.function.FunctionExecutor;
import org.wso2.siddhi.extension.he.api.HomomorphicEncDecService;
import org.wso2.siddhi.extension.he.api.HomomorphicEncryptionEvaluation;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import util.Properties;

public class HeEqualFunctionExtension extends FunctionExecutor {

    static final Logger log = Logger.getLogger(HeEqualFunctionExtension.class);

    Attribute.Type returnType = Attribute.Type.BOOL;
    private HomomorphicEncryptionEvaluation heEval;
    private HomomorphicEncDecService homomorphicEncDecService;
    private final int batchSize = 478;
//    private final int batchSize = 39;

    private String encryptedOperand = null;

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
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.STRING) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the second argument of he:compareEqualIntInt()) function, "
                            + "required " + Attribute.Type.STRING + ", but found "
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

        if(encryptedOperand == null) {

        }
//        String param2 = (String)data[1];

        StringBuilder valueBuilder = new StringBuilder();
//        byte[] param2Bytes = param2.getBytes();
        for(byte value : param2Bytes) {
            valueBuilder.append(value);
            valueBuilder.append(",");
        }
//        int dummyCount = batchSize - param2Bytes.length;
        for(int i = 0;i < dummyCount; i++) {
            valueBuilder.append(0);
            valueBuilder.append(",");
        }
        String valueList = valueBuilder.toString().replaceAll(",$", "");

        String encryptedParam2 = homomorphicEncDecService.encryptLongVector(valueList);
        
        String result = heEval.evaluateSubtract(param1, encryptedParam2);
        String decryptLongVector = homomorphicEncDecService.decryptLongVector(result);
        String modifiedDecryptLongVector = decryptLongVector.replace("0,", "");
        return modifiedDecryptLongVector.isEmpty();
//        return false;
    }

    @Override
    protected Object execute(Object data) {
        return null;
    }

}
