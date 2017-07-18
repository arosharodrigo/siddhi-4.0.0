package org.wso2.siddhi.extension.he;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;
import org.wso2.siddhi.extension.he.test.util.SiddhiTestHelper;

import java.util.concurrent.atomic.AtomicInteger;

public class CompareEqualFunctionExtensionTestCase {

    static final Logger log = Logger.getLogger(CompareEqualFunctionExtensionTestCase.class);
    private AtomicInteger count = new AtomicInteger(0);
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count.set(0);
        eventArrived = false;
    }

    @Test
    public void testCompareEqualFunctionExtension() throws InterruptedException {
        log.info("CompareEqualFunctionExtensionTestCase TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (symbol string, price long, "
                + "volume int);";
        String query = ("@info(name = 'query1') " + "from inputStream "
                + "select symbol, he:compareEqual(volume, 22) as compareEqual "
                + "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager
                .createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                for (Event inEvent : inEvents) {
                    count.incrementAndGet();
                    if (count.get() == 1) {
                        Assert.assertEquals(false, inEvent.getData(1));
                    }
                    if (count.get() == 2) {
                        Assert.assertEquals(true, inEvent.getData(1));
                    }
                    if (count.get() == 3) {
                        Assert.assertEquals(true, inEvent.getData(1));
                    }
                    if (count.get() == 4) {
                        Assert.assertEquals(false, inEvent.getData(1));
                    }
                    eventArrived = true;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();
        inputHandler.send(new Object[] { "IBM", 700f, 33 });
        inputHandler.send(new Object[] { "WSO2", 60.5f, 22 });
        inputHandler.send(new Object[] { "wso2", 60.5f, 22 });
        inputHandler.send(new Object[] { "", 60.5f, 150 });
        SiddhiTestHelper.waitForEvents(100, 4, count, 60000);
        Assert.assertEquals(4, count.get());
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

}
