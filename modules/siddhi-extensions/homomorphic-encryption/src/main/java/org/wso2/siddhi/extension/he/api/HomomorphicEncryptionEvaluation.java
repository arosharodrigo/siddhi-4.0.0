package org.wso2.siddhi.extension.he.api;

public class HomomorphicEncryptionEvaluation {

    static {
        System.loadLibrary("he");
    }

    public native void init(String keyFileLocation);
    public native void destroy();

    public native String evaluateAdd(String val1, String val2);
    public native String evaluateSubtract(String val1, String val2);
    public native String evaluateMultiply(String val1, String val2);
    public native String evaluateDivide(String val1, String val2);
    public native String evaluateGreaterThanBitSize1(String val1bit1, String val2bit1);
    public native String evaluateGreaterThanBitSize2(String val1bit1, String val1bit2, String val2bit1, String val2bit2);
    public native String evaluateLessThanBitSize2(String val1bit1, String val1bit2, String val2bit1, String val2bit2);
    public native String evaluateEqualBitSize2(String val1bit1, String val1bit2, String val2bit1, String val2bit2);

}
