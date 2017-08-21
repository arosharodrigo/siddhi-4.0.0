package org.wso2.siddhi.extension.he.api;

public class HomomorphicEncryptionEvaluation {

    static {
        System.loadLibrary("heShared");
    }

    public native void init();
    public native void destroy();

    public native String compareEqualIntInt(String val1, String val2);
    public native String compareGreaterThanIntInt(String val1, String val2);

}
