package myutils.pubsub;

import static myutils.TestUtil.sleep;

import java.io.IOException;
import java.util.Collections;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;


/**
 * This class has only one test do we can test that shutdown hooks fire when JVM ends.
 */
public class DistributedSocketPubSubShutdownTest {
    @SuppressWarnings("unused")
    private long startOfTime;
    
    @BeforeAll
    static void onStartAllTests() {
        System.out.println("start all tests");
        System.out.println("--------------------------------------------------------------------------------");
    }
    
    @AfterAll
    static void printAllTestsFinished() {
        System.out.println("----------------level----------------------------------------------------------------");
        System.out.println("all tests finished");
    }
    
    @BeforeEach
    void setStartOfTime(TestInfo testInfo) {
        startOfTime = System.currentTimeMillis();
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("test started: " + testInfo.getDisplayName());
    }
    
    @AfterEach
    void printTestFinished(TestInfo testInfo) {
        System.out.println("test finished: " + testInfo.getDisplayName());
    }

    
    //////////////////////////////////////////////////////////////////////

    private static final String CENTRAL_SERVER_HOST = "localhost";
    private static final int CENTRAL_SERVER_PORT = 2001;
    
    @Test
    void testPublishAndSubscribeAndUnsubscribe() throws IOException {
        DistributedMessageServer centralServer = new DistributedMessageServer(CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT, Collections.emptyMap());
        centralServer.start();
        sleep(250); // time to let the central server start
        
        DistributedSocketPubSub client1 = new DistributedSocketPubSub(1,
                                                                      PubSub.defaultQueueCreator(),
                                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                                      "client1",
                                                                      "localhost",
                                                                      30001,
                                                                      CENTRAL_SERVER_HOST,
                                                                      CENTRAL_SERVER_PORT);
        client1.start();
        
        DistributedSocketPubSub client2 = new DistributedSocketPubSub(1,
                                                                      PubSub.defaultQueueCreator(),
                                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                                      "client2",
                                                                      "localhost",
                                                                      30002,
                                                                      CENTRAL_SERVER_HOST,
                                                                      CENTRAL_SERVER_PORT);
        client2.start();

        sleep(250); // time to let clients start, connect to the central server, and send identification

        // the shutdown hook will close centralServer, client1, and client2
    }
}
