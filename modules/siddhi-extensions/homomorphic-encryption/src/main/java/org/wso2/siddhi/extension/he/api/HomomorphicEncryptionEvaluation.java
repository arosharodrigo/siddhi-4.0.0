package org.wso2.siddhi.extension.he.api;

public class HomomorphicEncryptionEvaluation {

    static {
        System.loadLibrary("heShared");
    }

    public native boolean compareEqualIntInt(int val1, int val2);
    public native boolean compareGreaterThanIntInt(int val1, int val2);

}
