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

public class HeDivideFunctionExtensionTestCase {

    private static final Logger log = Logger.getLogger(HeMultiplyFunctionExtensionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;
    private HomomorphicEncDecService homomorphicEncDecService;
    //    private final int batchSize = 478;
    private final int batchSize = 140;
//    private final int batchSize = 156;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
        homomorphicEncDecService = new HomomorphicEncDecService();
//        homomorphicEncDecService.generateKeys("/home/arosha/helib-keys", 1201, 1, 2, 15, 64, 1, 80, 0);
        homomorphicEncDecService.generateKeys("/home/arosha/helib-keys", 16, 1, 8, 2, 64, 0, 80, 0);
//        homomorphicEncDecService.generateKeys("/home/arosha/helib-keys", 101, 1, 8, 2, 64, 0, 80, 0);
        homomorphicEncDecService.init("/home/arosha/helib-keys");
    }

    @Test
    public void testDivideFunctionExtension() throws InterruptedException {
        log.info("HeMultiplyFunctionExtensionTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (timestampList string, valueList string, size long);";

        String query = ("@info(name = 'query1') " + "from inputStream "
                + "select timestampList, he:divide(valueList, 3L) as withBonusValueList, size "
                + "insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEventsComposite, Event[] removeEventsComposite) {
                EventPrinter.print(timeStamp, inEventsComposite, removeEventsComposite);
                for (Event inEventComposite : inEventsComposite) {
                    int eventSize = Integer.parseInt(String.valueOf(inEventComposite.getData(2)));

                    String encryptedResult = String.valueOf(inEventComposite.getData(1));
                    String decryptedResult = homomorphicEncDecService.decryptLongVector(encryptedResult);

                    String[] timestampArray = String.valueOf(inEventComposite.getData(0)).split(",");
                    String[] decryptedResultArray = decryptedResult.split(",");

                    Event[] decryptedEvents = new Event[eventSize];
                    for(int i = 0;i < eventSize; i++) {
                        decryptedEvents[i] = new Event(inEventComposite.getTimestamp(), new Object[]{timestampArray[i], decryptedResultArray[i]});
                    }

                    for (Event inEvent : decryptedEvents) {
                        count.incrementAndGet();
                        if (count.get() == 1) {
                            Assert.assertEquals(2L, Long.parseLong(String.valueOf(inEvent.getData(1))));
                        }
                        if (count.get() == 2) {
                            Assert.assertEquals(8L, Long.parseLong(String.valueOf(inEvent.getData(1))));
                        }
                        if (count.get() == 3) {
                            Assert.assertEquals(160L, Long.parseLong(String.valueOf(inEvent.getData(1))));
                        }
                        if (count.get() == 4) {
                            Assert.assertEquals(250L, Long.parseLong(String.valueOf(inEvent.getData(1))));
                        }
                        if (count.get() == 5) {
                            Assert.assertEquals(140L, Long.parseLong(String.valueOf(inEvent.getData(1))));
                        }
                    }
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        Object[] event1 = {System.currentTimeMillis(), 4};
        Object[] event2 = {System.currentTimeMillis(), 16};
        Object[] event3 = {System.currentTimeMillis(), 320};
        Object[] event4 = {System.currentTimeMillis(), 500};
        Object[] event5 = {System.currentTimeMillis(), 280};
        List<Object[]> eventList = new ArrayList<Object[]>();
        eventList.add(event1);
        eventList.add(event2);
        eventList.add(event3);
        eventList.add(event4);
        eventList.add(event5);

        StringBuilder timestampBuilder = new StringBuilder();
        StringBuilder valueBuilder = new StringBuilder();
        for(Object[] event : eventList) {
            timestampBuilder.append(event[0]);
            timestampBuilder.append(",");
            valueBuilder.append(event[1]);
            valueBuilder.append(",");
        }
        int dummyCount = batchSize - eventList.size();
        for(int i = 0;i < dummyCount; i++) {
            valueBuilder.append(0);
            valueBuilder.append(",");
        }

        String valueList = valueBuilder.toString().replaceAll(",$", "");
        String encryptedValueList = homomorphicEncDecService.encryptLongVector(valueList);
        Object[] compositeEvent = {timestampBuilder.toString().replaceAll(",$", ""), encryptedValueList, eventList.size()};
        inputHandler.send(compositeEvent);

        SiddhiTestHelper.waitForEvents(100, 5, count, 30000);
        Assert.assertEquals(5, count.get());
        executionPlanRuntime.shutdown();
    }

}
