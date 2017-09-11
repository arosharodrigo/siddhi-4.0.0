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

import java.util.concurrent.atomic.AtomicInteger;

public class HeAddFunctionExtensionTestCase {

    static final Logger log = Logger.getLogger(HeAddFunctionExtensionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;
    private HomomorphicEncDecService homomorphicEncDecService;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
        homomorphicEncDecService = new HomomorphicEncDecService();
        homomorphicEncDecService.init("/home/arosha/helib-keys");
        homomorphicEncDecService.generateKeys(1543, 1, 1, 2, 64, 0, 128, 0);
    }

    @Test
    public void testCompareEqualFunctionExtension() throws InterruptedException {
        log.info("HeAddFunctionExtensionTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price float, volume string);";

        String query = ("@info(name = 'query1') " + "from inputStream "
                + "select symbol, he:add(volume, 22L) as withBonusVolume "
                + "insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(55L, homomorphicEncDecService.decryptLong((String)inEvent.getData(1)));
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(48L, homomorphicEncDecService.decryptLong((String)inEvent.getData(1)));
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(247L, homomorphicEncDecService.decryptLong((String)inEvent.getData(1)));
                    }
                    if (count.get() == 4) {
                        Assert.assertEquals(172L, homomorphicEncDecService.decryptLong((String)inEvent.getData(1)));
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, homomorphicEncDecService.encryptLong(33L)});
        inputHandler.send(new Object[] { "WSO2", 60.5f, homomorphicEncDecService.encryptLong(26)});
        inputHandler.send(new Object[] { "wso2", 60.5f, homomorphicEncDecService.encryptLong(225)});
        inputHandler.send(new Object[] { "Codegen", 60.5f, homomorphicEncDecService.encryptLong(150)});
        SiddhiTestHelper.waitForEvents(100, 4, count, 90000);
        Assert.assertEquals(4, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

}
