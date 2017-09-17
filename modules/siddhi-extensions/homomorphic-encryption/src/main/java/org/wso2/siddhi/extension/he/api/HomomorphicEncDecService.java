package org.wso2.siddhi.extension.he.api;

public class HomomorphicEncDecService {

    static {
        System.loadLibrary("he");
    }

    public native void init(String keyFileLocation);
    public native void destroy();

    public native void generateKeys(long p, long r, long L, long c, long w, long d, long k, long s);
    public native String encryptLong(long val);
    public native String encryptLongVector(String val);
    public native long decryptLong(String encryptedVal);
    public native String decryptLongVector(String encryptedVal);

}
