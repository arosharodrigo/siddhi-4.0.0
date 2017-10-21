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
import org.wso2.siddhi.extension.he.test.util.SiddhiTestHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class HeEqualFunctionExtensionTestCase {

    private static final Logger log = Logger.getLogger(HeAddFunctionExtensionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;
    private HomomorphicEncDecService homomorphicEncDecService;
    private final int batchSize = 500;
//    private final int batchSize = 39;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
        homomorphicEncDecService = new HomomorphicEncDecService();
        homomorphicEncDecService.generateKeys("/home/arosha/helib-keys", 131, 1, 50, 16, 64, 1, 80, 0);
//        homomorphicEncDecService.init("/home/arosha/helib-keys");
    }

    @Test
    public void testCompareEqualFunctionExtension() throws InterruptedException {
        log.info("HeEqualFunctionExtensionTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputEmailsStream (iij_timestamp long, fromAddress string, toAddresses string, " +
                "ccAddresses string, bccAddresses string, subject string, body string, regexstr string);";

        String query = ("@info(name = 'query1') " + "from inputEmailsStream [(he:equal(fromAddress, 'lynn.blair@enron.com'))] "
                + "select iij_timestamp, fromAddress, toAddresses as toAdds, ccAddresses as ccAdds, bccAddresses as bccAdds, subject as updatedSubject, body as bodyObfuscated "
                + "insert into outputEmailsStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                    for (Event inEvent : inEvents) {
                        count.incrementAndGet();
                        if (count.get() == 1) {
                            Assert.assertEquals("1", String.valueOf(inEvent.getData(2)));
                        }
                        if (count.get() == 2) {
                            Assert.assertEquals("4", String.valueOf(inEvent.getData(2)));
                        }
                    }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputEmailsStream");
        executionPlanRuntime.start();
        Object[] event1 = {System.currentTimeMillis(), "lynn.blair@enron.com", "1", "", "", "", "", ""};
        Object[] event2 = {System.currentTimeMillis(), "richard.hanagriff@enron.com", "2", "", "", "", "", ""};
        Object[] event3 = {System.currentTimeMillis(), "richard.hanagriff@enron.com", "3", "", "", "", "", ""};
        Object[] event4 = {System.currentTimeMillis(), "lynn.blair@enron.com", "4", "", "", "", "", ""};
        List<Object[]> eventList = new ArrayList<Object[]>();
        eventList.add(event1);
        eventList.add(event2);
        eventList.add(event3);
        eventList.add(event4);

        for(Object[] event: eventList) {
            event[1] = encryptToStr((String)event[1], batchSize);
            inputHandler.send(event);
        }

        SiddhiTestHelper.waitForEvents(100, 2, count, 30000);
        Assert.assertEquals(2, count.get());
        executionPlanRuntime.shutdown();
    }

    private String encryptToStr(String param, int batchSize) {
        StringBuilder valueBuilder = new StringBuilder();
        byte[] paramBytes = param.getBytes();
        for(byte value : paramBytes) {
            valueBuilder.append(value);
            valueBuilder.append(",");
        }
        int dummyCount = batchSize - paramBytes.length;
        for(int i = 0;i < dummyCount; i++) {
            valueBuilder.append(0);
            valueBuilder.append(",");
        }
        String valueList = valueBuilder.toString().replaceAll(",$", "");
        String encryptedParam = homomorphicEncDecService.encryptLongVector(valueList);
        return encryptedParam;
    }

    @Test
    public void test() throws Exception {


    }
}
