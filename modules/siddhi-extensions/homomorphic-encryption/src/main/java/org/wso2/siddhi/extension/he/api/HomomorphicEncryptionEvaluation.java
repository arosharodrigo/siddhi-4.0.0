package org.wso2.siddhi.extension.he.api;

public class HomomorphicEncryptionEvaluation {

    static {
        System.loadLibrary("he");
    }

    public native void init(String keyFileLocation);
    public native void destroy();

    public native String evaluateAdd(String val1, String val2);
    public native String evaluateSubtract(String val1, String val2);

}
