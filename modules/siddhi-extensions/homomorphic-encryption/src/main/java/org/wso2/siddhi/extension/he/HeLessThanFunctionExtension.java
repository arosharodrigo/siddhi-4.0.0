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

public class HeLessThanFunctionExtension extends FunctionExecutor {

    static final Logger log = Logger.getLogger(HeEqualFunctionExtension.class);

    Attribute.Type returnType = Attribute.Type.STRING;
    private HomomorphicEncryptionEvaluation heEval;
    private HomomorphicEncDecService homomorphicEncDecService;

    private static final int batchSize = 168;

    private AtomicReference<String> encryptedOperandBit1 = new AtomicReference<String>("");
    private AtomicReference<String> encryptedOperandBit2 = new AtomicReference<String>("");

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
        if (attributeExpressionExecutors.length != 3) {
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to he:compareGreaterThanIntInt() function, " + "required 21, but found "
                            + attributeExpressionExecutors.length);
        }
        if (attributeExpressionExecutors[0].getReturnType() != Attribute.Type.STRING) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the first argument of he:compareGreaterThanIntInt() function, "
                            + "required " + Attribute.Type.STRING + ", but found "
                            + attributeExpressionExecutors[0].getReturnType().toString());
        }
        if (attributeExpressionExecutors[1].getReturnType() != Attribute.Type.STRING) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the second argument of he:compareGreaterThanIntInt()) function, "
                            + "required " + Attribute.Type.STRING + ", but found "
                            + attributeExpressionExecutors[1].getReturnType().toString());
        }
        if (attributeExpressionExecutors[2].getReturnType() != Attribute.Type.STRING) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the second argument of he:compareGreaterThanIntInt()) function, "
                            + "required " + Attribute.Type.STRING + ", but found "
                            + attributeExpressionExecutors[2].getReturnType().toString());
        }

        heEval = new HomomorphicEncryptionEvaluation();
        homomorphicEncDecService = new HomomorphicEncDecService();
        String keyFilePath = Properties.PROP.getProperty("key.file.path", "/home/arosha/helib-keys/greater-than");
        heEval.init(keyFilePath);
        homomorphicEncDecService.init(keyFilePath);
    }

    @Override
    protected Object execute(Object[] data) {
        String param1bit1 = (String)data[0];
        String param1bit2 = (String)data[1];

        if(encryptedOperandBit1.get().equals("") || encryptedOperandBit2.get().equals("")) {
            String operandParam = (String)data[2];
            encryptedOperandBit1.set(encryptToStr(String.valueOf(operandParam.charAt(1)), batchSize));
            encryptedOperandBit2.set(encryptToStr(String.valueOf(operandParam.charAt(0)), batchSize));
        }
        String result = heEval.evaluateLessThanBitSize2(param1bit1, param1bit2, encryptedOperandBit1.get(), encryptedOperandBit2.get());
        return result;
    }

    @Override
    protected Object execute(Object data) {
        return null;
    }

    private String encryptToStr(String param, int batchSize) {
        StringBuilder valueBuilder = new StringBuilder();
        byte[] paramBytes = param.getBytes();
        int dummyCount = batchSize - paramBytes.length;
        for(byte value : paramBytes) {
            valueBuilder.append(value-48);
            valueBuilder.append(",");
        }
        for(int i = 0;i < dummyCount; i++) {
            valueBuilder.append(0);
            valueBuilder.append(",");
        }
        String valueList = valueBuilder.toString().replaceAll(",$", "");
        String encryptedParam = homomorphicEncDecService.encryptLongVector(valueList);
        return encryptedParam;
    }

}
