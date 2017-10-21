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

import java.util.concurrent.atomic.AtomicReference;

public class HeEqualFunctionExtension extends FunctionExecutor {

    static final Logger log = Logger.getLogger(HeEqualFunctionExtension.class);

    Attribute.Type returnType = Attribute.Type.STRING;
    private HomomorphicEncryptionEvaluation heEval;
    private HomomorphicEncDecService homomorphicEncDecService;

    private static final int batchSize = 478;
    private static final int maxEmailLength = 40;
    private static final int compositeEventSize = 10;

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

        if(encryptedOperand.get().equals("")) {
            String param2 = (String)data[1];
            String binaryForm = convertToBinaryForm(param2, maxEmailLength);
            StringBuilder valueBuilder = new StringBuilder();
            for(int i = 0; i < compositeEventSize; i++) {
                valueBuilder.append(binaryForm).append(",");
            }
            int remainingSlots = batchSize - (compositeEventSize * maxEmailLength);
            for(int i = 0; i < remainingSlots; i++) {
                valueBuilder.append(0);
                valueBuilder.append(",");
            }
            String valueList = valueBuilder.toString().replaceAll(",$", "");
            encryptedOperand = new AtomicReference<String>(homomorphicEncDecService.encryptLongVector(valueList));
        }
        String result = heEval.evaluateSubtract(param1, encryptedOperand.get());
//        String decryptLongVector = homomorphicEncDecService.decryptLongVector(result);
//        String modifiedDecryptLongVector = decryptLongVector.replace("0,", "");
//        return modifiedDecryptLongVector.isEmpty();
        return result;
    }

    @Override
    protected Object execute(Object data) {
        return null;
    }

    private static String convertToBinaryForm(String param, int batchSize) {
        StringBuilder valueBuilder = new StringBuilder();
        byte[] paramBytes = param.getBytes();
        int minimumSize = (paramBytes.length < batchSize) ? paramBytes.length : batchSize;
        for(int i = 0; i < minimumSize; i++) {
            valueBuilder.append(paramBytes[i]);
            valueBuilder.append(",");
        }
        int dummyCount = batchSize - minimumSize;
        for(int j = 0;j < dummyCount; j++) {
            valueBuilder.append(0);
            valueBuilder.append(",");
        }
        String valueList = valueBuilder.toString().replaceAll(",$", "");
        return valueList;
    }

}
