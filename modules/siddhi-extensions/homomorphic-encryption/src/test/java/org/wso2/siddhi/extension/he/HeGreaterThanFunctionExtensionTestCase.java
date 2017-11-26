package org.wso2.siddhi.extension.he;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.he.api.HomomorphicEncDecService;
import org.wso2.siddhi.extension.he.api.HomomorphicEncryptionEvaluation;
import org.wso2.siddhi.extension.he.test.util.SiddhiTestHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by arosha on 11/25/17.
 */
public class HeGreaterThanFunctionExtensionTestCase {

    private static final Logger log = Logger.getLogger(HeAddFunctionExtensionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;
    private HomomorphicEncDecService homomorphicEncDecService;
    private final int batchSize = 168;

    private HomomorphicEncryptionEvaluation heEval;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
        homomorphicEncDecService = new HomomorphicEncDecService();
//        homomorphicEncDecService.generateKeys("/home/arosha/helib-keys/greater-than", 2, 1, 4, 2, 64, 0, 80, 0);
        homomorphicEncDecService.init("/home/arosha/helib-keys/greater-than");

        heEval = new HomomorphicEncryptionEvaluation();
        heEval.init("/home/arosha/helib-keys/greater-than");
    }

    @Test
    public void testGreaterThanFunctionExtension() throws InterruptedException {
        log.info("HeGreaterThanFunctionExtensionTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputEmailsStream (iij_timestamp string, param1bit1 string, param1bit2 string);";

        String query = ("@info(name = 'query1') " + "from inputEmailsStream "
                + "select iij_timestamp, he:greaterThan(param1bit1, param1bit2, '11') as isLarge "
                + "insert into outputEmailsStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                for (Event inEvent : inEvents) {
                    count.incrementAndGet();
                    Object[] data = inEvent.getData();
                    System.out.println("data[0]: " + data[0]);
                    String decryptLongVector = homomorphicEncDecService.decryptLongVector((String) data[1]);
                    System.out.println("data[1]:" + decryptLongVector.charAt(0));
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputEmailsStream");
        executionPlanRuntime.start();

        String param1bit1 = homomorphicEncDecService.encryptLong(1);
        String param1bit2 = homomorphicEncDecService.encryptLong(1);

        Object[] event1 = {System.currentTimeMillis(), param1bit1, param1bit2};
        List<Object[]> eventList = new ArrayList<Object[]>();
        eventList.add(event1);

        for(Object[] event: eventList) {
            inputHandler.send(event);
        }

        SiddhiTestHelper.waitForEvents(100, 1, count, 10000);
        Assert.assertEquals(1, count.get());
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testEvaluateGreaterThanBitSize1() throws Exception {
        String param1bit1 = encryptToStr("1", batchSize);
        String param1bit2 = encryptToStr("0", batchSize);

        String result = heEval.evaluateGreaterThanBitSize1(param1bit1, param1bit2);
        String decryptedVal = homomorphicEncDecService.decryptLongVector(result);
        System.out.println("Decrypted result: " + decryptedVal.charAt(0));
    }

    @Test
    public void testEvaluateGreaterThanBitSize2() throws Exception {
        String param1bit1 = encryptToStr("1", batchSize);
        String param1bit2 = encryptToStr("1", batchSize);
        String param2bit1 = encryptToStr("0", batchSize);
        String param2bit2 = encryptToStr("1", batchSize);

        String result = heEval.evaluateGreaterThanBitSize2(param1bit1, param1bit2, param2bit1, param2bit2);
        String decryptedVal = homomorphicEncDecService.decryptLongVector(result);
        System.out.println("Decrypted result: " + decryptedVal.charAt(0));
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
