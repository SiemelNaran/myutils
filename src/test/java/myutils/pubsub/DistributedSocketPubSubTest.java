package myutils.pubsub;

import static myutils.TestUtil.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import myutils.pubsub.InMemoryPubSubTest.CloneableString;
import myutils.pubsub.PubSub.Publisher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;


public class DistributedSocketPubSubTest {
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

    
    ///////////////////////////////////////////////////////////////////////////////////////

    private static final String CENTRAL_SERVER_HOST = "localhost";
    private static final int CENTRAL_SERVER_PORT = 2001;
    
    @Test
    void testPublishAndSubscribeAndUnsubscribe() throws IOException {
        Cleaner cleaner = Cleaner.create();
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        DistributedMessageServer centralServer = new DistributedMessageServer(cleaner, CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT);
        centralServer.start();
        sleep(250); // time to let the central server start
        
        DistributedSocketPubSub client1 = new DistributedSocketPubSub(cleaner,
                                                          1,
                                                          PubSub.defaultQueueCreator(),
                                                          PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                          "client1",
                                                          CENTRAL_SERVER_HOST,
                                                          CENTRAL_SERVER_PORT);
        client1.start();
        
        DistributedSocketPubSub client2 = new DistributedSocketPubSub(cleaner,
                                                          1,
                                                          PubSub.defaultQueueCreator(),
                                                          PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                          "client2",
                                                          CENTRAL_SERVER_HOST,
                                                          CENTRAL_SERVER_PORT);
        client2.start();
        
        DistributedSocketPubSub client3 = new DistributedSocketPubSub(cleaner,
                                                          1,
                                                          PubSub.defaultQueueCreator(),
                                                          PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                          "client3",
                                                          CENTRAL_SERVER_HOST,
                                                          CENTRAL_SERVER_PORT);
        client3.start();

        sleep(250); // time to let clients start, connect to the central server, and send identification

        // subscribe before publisher created, as this could happen in a real system
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        client2.subscribe("hello", "ClientTwoSubscriber", CloneableString.class, str -> words.add(str.append("-s2")));
        client3.subscribe("hello", "ClientThreeSubscriber", CloneableString.class, str -> words.add(str.append("-s3")));
        assertFalse(client1.getPublisher("hello").isPresent());
        assertFalse(client2.getPublisher("hello").isPresent());
        
        // create publisher on client1
        // this will get replicated to client2 and client3
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        sleep(250); // time to let publisher be propagated to client2
        assertTrue(client1.getPublisher("hello").isPresent());
        assertTrue(client2.getPublisher("hello").isPresent());
        assertTrue(client3.getPublisher("hello").isPresent());
        
        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, and in client2 the system will call publisher2.publish(), and similarly for client3
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages be published to client2
        System.out.println("actual= " + words);
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1", "one-s2", "two-s2", "one-s3", "two-s3"));
        
        client3.shutdown();
        words.clear();
        
        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, and in client2 the system will call publisher2.publish()
        publisher1.publish(new CloneableString("three"));
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to client2
        System.out.println("actual= " + words);
        assertThat(words, Matchers.containsInAnyOrder("three-s1", "four-s1", "three-s2", "four-s2"));
        
        centralServer.shutdown();
    }
}
