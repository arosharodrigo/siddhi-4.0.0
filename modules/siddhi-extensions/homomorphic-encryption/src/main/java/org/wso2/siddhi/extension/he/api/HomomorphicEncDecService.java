package org.wso2.siddhi.extension.he.api;

public class HomomorphicEncDecService {

    static {
        System.loadLibrary("heShared");
    }

    public native void init();
    public native void destroy();

    public native String encrypt(String binaryForm32);
    public native String decrypt(String encryptedVal);

}
