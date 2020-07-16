package myutils.pubsub;

import static myutils.TestUtil.sleep;
import static myutils.TestUtil.toFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.NetworkChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletionStage;
import java.util.function.Supplier;
import myutils.LogFailureToConsoleTestWatcher;
import myutils.pubsub.InMemoryPubSubTest.CloneableString;
import myutils.pubsub.MessageClasses.MessageBase;
import myutils.pubsub.PubSub.Publisher;
import myutils.pubsub.PubSub.Subscriber;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;


@ExtendWith(LogFailureToConsoleTestWatcher.class)
public class DistributedSocketPubSubTest {
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
        System.out.println("--------------------------------------------------------------------------------");
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
    
    void addShutdown(Shutdowneable s) {
        shutdowns.add(s);
    }
    
    //////////////////////////////////////////////////////////////////////

    private static final String CENTRAL_SERVER_HOST = "localhost";
    private static final int CENTRAL_SERVER_PORT = 2101;
    
    /**
     * Basic test for publish + subscribe + unsubscribe.
     * There is a central server, 3 clients, and 4 subscribers (2 subscribers in client2).
     */
    @Test
    void testPublishAndSubscribeAndUnsubscribe() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT, Collections.emptyMap());
        addShutdown(centralServer);
        centralServer.start();
        sleep(250); // time to let the central server start
        
        var client1 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client1",
                                                      "localhost",
                                                      30001,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client1);
        client1.start();
        
        var client2 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client2",
                                                      "localhost",
                                                      30002,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client2);
        client2.start();
        
        var client3 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client3",
                                                      "localhost",
                                                      30003,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client3);
        client3.start();

        sleep(250); // time to let clients start, connect to the central server, and send identification
        assertEquals(1, client1.getCountSent()); // message sent = identification
        assertEquals(0, client1.getCountReceived());
        assertEquals(1, client2.getCountSent());
        assertEquals(0, client2.getCountReceived());
        assertEquals(1, client3.getCountSent());
        assertEquals(0, client3.getCountReceived());

        // subscribe before publisher created, as this could happen in a real system
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        Subscriber subscriber2a = client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        Subscriber subscriber2b = client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        client3.subscribe("hello", "ClientThreeSubscriber", CloneableString.class, str -> words.add(str.append("-s3")));
        assertFalse(client1.getPublisher("hello").isPresent());
        assertFalse(client2.getPublisher("hello").isPresent());
        assertFalse(client3.getPublisher("hello").isPresent());
        sleep(250); // time to let subscribers be sent to server
        assertEquals(2, client1.getCountSent()); // +1 because deferred subscriber sent to central server
        assertEquals(0, client1.getCountReceived());
        assertEquals(3, client2.getCountSent()); // +1 because two subscribers
        assertEquals(0, client2.getCountReceived());
        assertEquals(2, client3.getCountSent()); // +1
        assertEquals(0, client3.getCountReceived());
        
        // create publisher on client1
        // this will get replicated to client2 and client3
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        assertTrue(client1.getPublisher("hello").isPresent());
        sleep(250); // time to let publisher be propagated to all other clients
        sleep(250); // time to let each client sent a subscribe command to the server
        assertEquals(3, client1.getCountSent()); // +1 sent CreatePublisher
        assertEquals(0, client1.getCountReceived());
        assertEquals(3, client2.getCountSent());
        assertEquals(1, client2.getCountReceived()); // +1 received CreaetPublisher
        assertEquals(2, client3.getCountSent());
        assertEquals(1, client3.getCountReceived()); // +1 received CreaetPublisher
        assertTrue(client2.getPublisher("hello").isPresent());
        assertTrue(client3.getPublisher("hello").isPresent());
        
        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, and in client2 the system will call publisher2.publish(), and similarly for client3
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1", "one-s2a", "one-s2b", "two-s2a", "two-s2b", "one-s3", "two-s3"));
        assertEquals(5, client1.getCountSent()); // +2
        assertEquals(0, client1.getCountReceived());
        assertEquals(3, client2.getCountSent());
        assertEquals(3, client2.getCountReceived()); // +2
        assertEquals(2, client3.getCountSent());
        assertEquals(3, client3.getCountReceived()); // +2
        
        // explicitly shutdown client3 (normally this happens in the shutdown hook)
        // publish two messages
        // as client3 has been shutdown it will not receive the message
        client3.shutdown();
        sleep(250); // time to let client shutdown
        assertEquals(5, client1.getCountSent()); // no changes
        assertEquals(0, client1.getCountReceived());
        assertEquals(3, client2.getCountSent());
        assertEquals(3, client2.getCountReceived());
        assertEquals(2, client3.getCountSent());
        assertEquals(3, client3.getCountReceived());
        words.clear();       
        publisher1.publish(new CloneableString("three"));
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after shutdown client3: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("three-s1", "four-s1", "three-s2a", "three-s2b", "four-s2a", "four-s2b"));
        assertEquals(7, client1.getCountSent()); // +2
        assertEquals(0, client1.getCountReceived());
        assertEquals(3, client2.getCountSent());
        assertEquals(5, client2.getCountReceived()); // +2
        assertEquals(2, client3.getCountSent());
        assertEquals(3, client3.getCountReceived());
        
        // unsubscribe one subscriber in client2
        // publish two messages
        // as client2 still has one subscriber in the "hello" topic, client2 still receives the message
        client2.unsubscribe(subscriber2b);
        sleep(250); // time to let central server know that one subscribe in client2 unsubscribed
        assertEquals(7, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());
        assertEquals(4, client2.getCountSent()); // +1
        assertEquals(5, client2.getCountReceived());
        assertEquals(2, client3.getCountSent());
        assertEquals(3, client3.getCountReceived());
        words.clear();
        publisher1.publish(new CloneableString("five"));
        publisher1.publish(new CloneableString("six"));
        sleep(250); // time to let messages be published to client2
        System.out.println("after unsubsribe2b: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("five-s1", "six-s1", "five-s2a", "six-s2a"));
        assertEquals(9, client1.getCountSent()); // +2
        assertEquals(0, client1.getCountReceived());
        assertEquals(4, client2.getCountSent());
        assertEquals(7, client2.getCountReceived()); // +2
        assertEquals(2, client3.getCountSent());
        assertEquals(3, client3.getCountReceived());

        // unsubscribe the last subscriber in client2
        // publish two messages
        // as client3 has been shutdown and client2 has unsubscribed, only client1 will receive the message
        client2.unsubscribe(subscriber2a);
        sleep(250); // time to let central server know that client2 unsubscribed
        assertEquals(9, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());
        assertEquals(5, client2.getCountSent()); // +1
        assertEquals(7, client2.getCountReceived());
        assertEquals(2, client3.getCountSent());
        assertEquals(3, client3.getCountReceived());
        words.clear();
        publisher1.publish(new CloneableString("seven"));
        publisher1.publish(new CloneableString("eight"));
        sleep(250); // time to let messages be published to client2
        System.out.println("after unsubsribe2a: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("seven-s1", "eight-s1"));
        assertEquals(11, client1.getCountSent()); // +2
        assertEquals(0, client1.getCountReceived());
        assertEquals(5, client2.getCountSent());
        assertEquals(7, client2.getCountReceived()); // unchanged, client2 does not receive messages as it is no longer subscribed
        assertEquals(2, client3.getCountSent());
        assertEquals(3, client3.getCountReceived());
    }
    
    /**
     * Test a client coming online after messages have been published.
     * The new client does not receive new messages.
     * However, the new client can call download to retrieve and replay old messages.
     */
    @Test
    void testDownloadMessages() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                             CENTRAL_SERVER_PORT,
                                                             Map.of(MessagePriority.HIGH, 2, MessagePriority.MEDIUM, 3));
        addShutdown(centralServer);
        centralServer.start();
        sleep(250); // time to let the central server start
        
        var client1 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client1",
                                                      "localhost",
                                                      31001,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client1);
        client1.start();
        sleep(250); // time to let client start
        assertEquals(1, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());
        
        // create publisher on client1
        // this will get replicated to client2 and client3
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // time to let publisher be propagated to client2, but this client does not exist yet
        assertEquals(3, client1.getCountSent()); // sent identification, create publisher, add subscriber
        assertEquals(0, client1.getCountReceived());
        
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
        assertEquals(10, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());

        words.clear();
        var client2 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client2",
                                                      "localhost",
                                                      31002,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client2);
        assertFalse(client2.getPublisher("hello").isPresent());
        client2.start();
        sleep(250); // time to let client2 start
        assertEquals(10, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());
        assertEquals(1, client2.getCountSent());
        assertEquals(0, client2.getCountReceived()); // does not receive as client2 not subscribed to topic "hello"
        
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // time to let client2 subscribe and for server to send down the publisher
        assertTrue(client2.getPublisher("hello").isPresent());
        assertThat(words, Matchers.empty());
        assertEquals(10, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());
        assertEquals(3, client2.getCountSent()); // +2 = add subscriber
        assertEquals(1, client2.getCountReceived()); // +1 = create publisher
        
        client2.download(1, Long.MAX_VALUE);
        sleep(250); // time to let messages be sent to client2
        sleep(250); // time to let messages be sent to client2 as there are so many messages to send 
        System.out.println("actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantTwo-s2a", "ImportantTwo-s2b", "ImportantThree-s2a", "ImportantThree-s2b",
                                     "banana-s2a", "banana-s2b", "carrot-s2a", "carrot-s2b", "dragonfruit-s2a", "dragonfruit-s2b"));
        assertEquals(10, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());
        assertEquals(4, client2.getCountSent());
        assertEquals(6, client2.getCountReceived());
    }
    
    /**
     * Test the central server being created after the clients.
     * The clients keep checking if the central server exists.
     * Messages that failed to send before are sent now.
     */
    @Test
    void testCreateClientBeforeServer() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var client1 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client1",
                                                      "localhost",
                                                      31001,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client1);
        CompletionStage<Void> client1Started = client1.start();
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // time to let client start
        assertTrue(client1.getPublisher("hello").isPresent());
        assertEquals(0, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());
        
        var client2 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client2",
                                                      "localhost",
                                                      31002,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client2);
        client2.start();
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // time to let client2 start
        assertFalse(client2.getPublisher("hello").isPresent());
        assertEquals(0, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());
        assertEquals(0, client2.getCountSent());
        assertEquals(0, client2.getCountReceived());

        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, and in client2 the system will call publisher2.publish(), and similarly for client3
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish (and before central server started): actual=" + words);
        assertThat(words, Matchers.contains("one-s1", "two-s1"));
        assertEquals(0, client1.getCountSent());
        assertEquals(0, client1.getCountReceived());
        assertEquals(0, client2.getCountSent());
        assertEquals(0, client2.getCountReceived());
        
        assertFalse(toFuture(client1Started).isDone());

        words.clear();
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                             CENTRAL_SERVER_PORT,
                                                             Map.of(MessagePriority.HIGH, 1, MessagePriority.MEDIUM, 3));
        addShutdown(centralServer);
        centralServer.start();
        sleep(250); // time to let the central server start
        sleep(2000); // clients try to connect to the server every 1sec, 2sec, 4sec, 8sec, 8sec so more time to let the clients connect to the central server
        sleep(250); // time to let central server send messages down to the clients

        assertTrue(toFuture(client1Started).isDone());
        System.out.println("after central server started: actual=" + words);
        assertEquals(5, client1.getCountSent()); // identification, createPublisher, addSubscriber, 2 publish messages
        assertEquals(0, client1.getCountReceived());
        assertEquals(3, client2.getCountSent()); // identification, addSubscriber, addSubscriber
        assertEquals(3, client2.getCountReceived()); // createPublisher, message, message
        assertThat(words, Matchers.containsInAnyOrder("one-s2a", "one-s2b", "two-s2a", "two-s2b"));
    }
}



class TestDistributedMessageServer extends DistributedMessageServer {
    public TestDistributedMessageServer(String host,
                                        int port,
                                        Map<MessagePriority, Integer> mostRecentMessagesToKeep) throws IOException {
        super(host, port, mostRecentMessagesToKeep);
    }

    /**
     * Set SO_REUSEADDR so that the unit tests can close and open channels immediately, not waiting for TIME_WAIT seconds.
     */
    @Override
    protected void onBeforeSocketBound(NetworkChannel channel) throws IOException {
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    }
}


class TestDistributedSocketPubSub extends DistributedSocketPubSub {
    private List<String> countReceived = Collections.synchronizedList(new ArrayList<>());
    private List<String> countSent = Collections.synchronizedList(new ArrayList<>());

    public TestDistributedSocketPubSub(int numInMemoryHandlers,
                                       Supplier<Queue<Subscriber>> queueCreator,
                                       SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                       String machineId,
                                       String localServer,
                                       int localPort,
                                       String messageServerHost,
                                       int messageServerPort) throws IOException {
        super(numInMemoryHandlers, queueCreator, subscriptionMessageExceptionHandler, machineId, localServer, localPort, messageServerHost, messageServerPort);
    }

    /**
     * Set SO_REUSEADDR so that the unit tests can close and open channels immediately, not waiting for TIME_WAIT seconds.
     */
    @Override
    protected void onBeforeSocketBound(NetworkChannel channel) throws IOException {
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    }
   
    @Override
    protected void onBeforeSendMessage(MessageBase message) {
        countSent.add(message.getClass().getSimpleName());
    }
    
    @Override
    protected void onMessageReceived(MessageBase message) {
        countReceived.add(message.getClass().getSimpleName());
    }

    int getCountReceived() {
        return countReceived.size();
    }
    
    int getCountSent() {
        return countSent.size();
    }
}