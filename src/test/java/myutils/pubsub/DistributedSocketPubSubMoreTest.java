package myutils.pubsub;

import static myutils.TestUtil.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import myutils.pubsub.InMemoryPubSubTest.CloneableString;
import myutils.pubsub.PubSub.Publisher;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;


public class DistributedSocketPubSubMoreTest {
    @SuppressWarnings("unused")
    private long startOfTime;
    private LinkedList<Shutdowneable> shutdowns = new LinkedList<>();
    
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

    
    @BeforeEach
    void clearShutdowns() {
        shutdowns.clear();
    }
    
    @AfterEach
    void invokeShutdowns() {
        for (var iter = shutdowns.descendingIterator(); iter.hasNext(); ) {
            var s = iter.next();
            s.shutdown();
        }
    }
    
    <T extends Shutdowneable> T addShutdown(T s) {
        shutdowns.add(s);
        return s;
    }
    
    //////////////////////////////////////////////////////////////////////

    private static final String CENTRAL_SERVER_HOST = "localhost";
    private static final int CENTRAL_SERVER_PORT = 2101;
    
    @Test
    void testSubscribeLate() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        DistributedMessageServer centralServer = new DistributedMessageServer(CENTRAL_SERVER_HOST,
                                                                             CENTRAL_SERVER_PORT,
                                                                             Map.of(MessagePriority.HIGH, 2, MessagePriority.MEDIUM, 2));
        addShutdown(centralServer);
        centralServer.start();
        sleep(250); // time to let the central server start
        
        DistributedSocketPubSub client1 = new DistributedSocketPubSub(1,
                                                                      PubSub.defaultQueueCreator(),
                                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                                      "client1",
                                                                      "localhost",
                                                                      31001,
                                                                      CENTRAL_SERVER_HOST,
                                                                      CENTRAL_SERVER_PORT);
        addShutdown(client1);
        client1.start();
        
        // create publisher on client1
        // this will get replicated to client2 and client3
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // time to let publisher be propagated to client2
        
        // publish messages with different retentions
        // the server will remember the last 2 messages with low retention, and the last 2 messages with high retention
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, but client2 is not yet running
        publisher1.publish(new CloneableString("ImportantOne"), MessagePriority.HIGH);
        publisher1.publish(new CloneableString("ImportantTwo"), MessagePriority.HIGH);
        publisher1.publish(new CloneableString("ImportantThree"), MessagePriority.HIGH);
        publisher1.publish(new CloneableString("apple"), MessagePriority.MEDIUM);
        publisher1.publish(new CloneableString("banana"), MessagePriority.MEDIUM);
        publisher1.publish(new CloneableString("carrot"));
        publisher1.publish(new CloneableString("dragonfruit"), MessagePriority.MEDIUM);
        sleep(250); // time to let messages be published to client2
        System.out.println("actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantOne-s1", "ImportantTwo-s1", "ImportantThree-s1",
                                     "apple-s1", "banana-s1", "carrot-s1", "dragonfruit-s1"));
        words.clear();
        
        DistributedSocketPubSub client2 = new DistributedSocketPubSub(1,
                                                                      PubSub.defaultQueueCreator(),
                                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                                      "client2",
                                                                      "localhost",
                                                                      31002,
                                                                      CENTRAL_SERVER_HOST,
                                                                      CENTRAL_SERVER_PORT);
        addShutdown(client2);
        client2.start();
        client2.subscribe("hello", "ClientTwoSubscriber", CloneableString.class, str -> words.add(str.append("-s2")));
        
        sleep(250); // time to let publisher be propagated to client2
        assertTrue(client2.getPublisher("hello").isPresent());
        assertThat(words, Matchers.empty());
        
        client2.download(1);
        sleep(250); // time to let messages be sent to client2
        System.out.println("actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantTwo-s2", "ImportantThree-s2",
                                     "carrot-s2", "dragonfruit-s2"));
    }
}
