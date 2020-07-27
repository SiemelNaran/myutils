package myutils.pubsub;

import static myutils.TestUtil.assertExceptionFromCallable;
import static myutils.TestUtil.assertIncreasing;
import static myutils.TestUtil.countElementsInListByType;
import static myutils.TestUtil.sleep;
import static myutils.TestUtil.toFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.NetworkChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import myutils.TestBase;
import myutils.pubsub.InMemoryPubSubTest.CloneableString;
import myutils.pubsub.MessageClasses.MessageBase;
import myutils.pubsub.MessageClasses.PublishMessage;
import myutils.pubsub.MessageClasses.RelayFields;
import myutils.pubsub.MessageClasses.RelayMessageBase;
import myutils.pubsub.PubSub.Publisher;
import myutils.pubsub.PubSub.Subscriber;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class DistributedSocketPubSubTest extends TestBase {
    private LinkedList<Shutdowneable> shutdowns = new LinkedList<>();
    
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
     * We create subscribers first as this could happen in a real system.
     * client1 then creates a publisher, which gets relayed to the other clients.
     * client1 then publishes messages, which get relayed to the other clients.
     * Clients then unsubscribe or shutdown.
     * When a client is shutdown, then the server no longer relays messages to this client.
     * When all subscribers to a topic in one machine unsubscribe, then the server no longer relays messages to this client.
     */
    @Test
    void testSubscribeAndPublishAndUnsubscribe() throws IOException {
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
        assertEquals(1, client1.getCountTypesSent()); // message sent = identification
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(1, client2.getCountTypesSent());
        assertEquals(0, client2.getCountTypesReceived());
        assertEquals(1, client3.getCountTypesSent());
        assertEquals(0, client3.getCountTypesReceived());

        // subscribe before publisher created, as this could happen in a real system
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        Subscriber subscriber2a = client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        Subscriber subscriber2b = client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        client3.subscribe("hello", "ClientThreeSubscriber", CloneableString.class, str -> words.add(str.append("-s3")));
        assertFalse(client1.getPublisher("hello").isPresent());
        assertFalse(client2.getPublisher("hello").isPresent());
        assertFalse(client3.getPublisher("hello").isPresent());
        sleep(250); // time to let subscribers be sent to server
        assertEquals(2, client1.getCountTypesSent()); // +1 because deferred subscriber sent to central server
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent()); // +1 because two subscribers
        assertEquals(0, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent()); // +1
        assertEquals(0, client3.getCountTypesReceived());
        
        // create publisher on client1
        // this will get replicated to client2 and client3
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        assertTrue(client1.getPublisher("hello").isPresent());
        sleep(250); // time to let publisher be propagated to all other clients
        sleep(250); // time to let each client sent a subscribe command to the server
        assertEquals(3, client1.getCountTypesSent()); // +1 sent CreatePublisher
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(1, client2.getCountTypesReceived()); // +1 received CreaetPublisher
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(1, client3.getCountTypesReceived()); // +1 received CreaetPublisher
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
        assertEquals(5, client1.getCountTypesSent()); // +2
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(3, client2.getCountTypesReceived()); // +2
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(3, client3.getCountTypesReceived()); // +2
        
        // explicitly shutdown client3 (normally this happens in the shutdown hook)
        // publish two messages
        // as client3 has been shutdown it will not receive the message
        client3.shutdown();
        sleep(250); // time to let client shutdown
        assertEquals(5, client1.getCountTypesSent()); // no changes
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(3, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(3, client3.getCountTypesReceived());
        words.clear();       
        publisher1.publish(new CloneableString("three"));
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after shutdown client3: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("three-s1", "four-s1", "three-s2a", "three-s2b", "four-s2a", "four-s2b"));
        assertEquals(7, client1.getCountTypesSent()); // +2
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(5, client2.getCountTypesReceived()); // +2
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(3, client3.getCountTypesReceived());
        
        // unsubscribe one subscriber in client2
        // publish two messages
        // as client2 still has one subscriber in the "hello" topic, client2 still receives the message
        client2.unsubscribe(subscriber2b);
        sleep(250); // time to let central server know that one subscribe in client2 unsubscribed
        assertEquals(7, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent()); // +1
        assertEquals(5, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(3, client3.getCountTypesReceived());
        words.clear();
        publisher1.publish(new CloneableString("five"));
        publisher1.publish(new CloneableString("six"));
        sleep(250); // time to let messages be published to client2
        System.out.println("after unsubsribe2b: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("five-s1", "six-s1", "five-s2a", "six-s2a"));
        assertEquals(9, client1.getCountTypesSent()); // +2
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent());
        assertEquals(7, client2.getCountTypesReceived()); // +2
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(3, client3.getCountTypesReceived());

        // unsubscribe the last subscriber in client2
        // publish two messages
        // as client3 has been shutdown and client2 has unsubscribed, only client1 will receive the message
        client2.unsubscribe(subscriber2a);
        sleep(250); // time to let central server know that client2 unsubscribed
        assertEquals(9, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(5, client2.getCountTypesSent()); // +1
        assertEquals(7, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(3, client3.getCountTypesReceived());
        words.clear();
        publisher1.publish(new CloneableString("seven"));
        publisher1.publish(new CloneableString("eight"));
        sleep(250); // time to let messages be published to client2
        System.out.println("after unsubsribe2a: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("seven-s1", "eight-s1"));
        assertEquals(11, client1.getCountTypesSent()); // +2
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(5, client2.getCountTypesSent());
        assertEquals(7, client2.getCountTypesReceived()); // unchanged, client2 does not receive messages as it is no longer subscribed
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(3, client3.getCountTypesReceived());
    }

    /**
     * Test a client coming online after messages have been published.
     * The new client does not receive the messages which were published earlier.
     * However, the new client can call download to retrieve the old messages.
     */
    @Test
    void testDownloadMessages() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                             CENTRAL_SERVER_PORT,
                                                             Map.of(RetentionPriority.HIGH, 2, RetentionPriority.MEDIUM, 3));
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
        assertEquals(1, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        
        // create publisher on client1
        // as client2 does not exist yet, nor is subscribed, it does not receive the publisher
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // time to let publisher be propagated to client2, but this client does not exist yet
        assertEquals(3, client1.getCountTypesSent()); // sent identification, create publisher, add subscriber
        assertEquals(0, client1.getCountTypesReceived());
        
        // publish messages with different retentions
        // the server will remember the last 2 messages with low retention, and the last 2 messages with high retention
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, but client2 is not yet running
        publisher1.publish(new CloneableString("ImportantOne"), RetentionPriority.HIGH);
        publisher1.publish(new CloneableString("ImportantTwo"), RetentionPriority.HIGH);
        publisher1.publish(new CloneableString("ImportantThree"), RetentionPriority.HIGH);
        publisher1.publish(new CloneableString("apple"), RetentionPriority.MEDIUM);
        publisher1.publish(new CloneableString("banana"), RetentionPriority.MEDIUM);
        publisher1.publish(new CloneableString("carrot"));
        publisher1.publish(new CloneableString("dragonfruit"), RetentionPriority.MEDIUM);
        sleep(250); // time to let messages be published to client2
        System.out.println("actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantOne-s1", "ImportantTwo-s1", "ImportantThree-s1",
                                     "apple-s1", "banana-s1", "carrot-s1", "dragonfruit-s1"));
        assertEquals(10, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());

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
        assertEquals(10, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(1, client2.getCountTypesSent());
        assertEquals(0, client2.getCountTypesReceived()); // does not receive as client2 not subscribed to topic "hello"
        
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // time to let client2 subscribe and for server to send down the publisher
        assertTrue(client2.getPublisher("hello").isPresent());
        assertThat(words, Matchers.empty());
        assertEquals(10, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent()); // +2 = add subscriber
        assertEquals(1, client2.getCountTypesReceived()); // +1 = create publisher
        
        client2.download(ServerIndex.MIN_VALUE, ServerIndex.MAX_VALUE);
        sleep(250); // time to let messages be sent to client2
        sleep(250); // time to let messages be sent to client2 as there are so many messages to send 
        System.out.println("actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantTwo-s2a", "ImportantTwo-s2b", "ImportantThree-s2a", "ImportantThree-s2b",
                                     "banana-s2a", "banana-s2b", "carrot-s2a", "carrot-s2b", "dragonfruit-s2a", "dragonfruit-s2b"));
        assertEquals(10, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent());
        assertEquals(6, client2.getCountTypesReceived());
    }
    
    /**
     * In this test we shutdown the client and server.
     * Verify that restart fails.
     */
    @Test
    void testRestartFails() throws IOException, InterruptedException, ExecutionException {
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                             CENTRAL_SERVER_PORT,
                                                             Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        addShutdown(centralServer);
        Future<Void> startCentralServerFuture = toFuture(centralServer.start());
        sleep(250); // time to let server start
        assertTrue(startCentralServerFuture.isDone());
        startCentralServerFuture.get(); // assert no exception

        var client1 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client1",
                                                      "localhost",
                                                      31001,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client1);
        Future<Void> startClient1Future = toFuture(client1.start());
        sleep(250); // time to let client start        
        assertTrue(startClient1Future.isDone());
        startClient1Future.get(); // assert no exception
        assertEquals(1, client1.getCountTypesSent()); // Identification
        assertEquals(0, client1.getCountTypesReceived());
        
        // start server again
        Future<Void> startCentralServerFutureAgain = toFuture(client1.start());
        sleep(250); // time to let server start        
        assertTrue(startCentralServerFutureAgain.isDone());
        assertExceptionFromCallable(() -> startCentralServerFutureAgain.get(), ExecutionException.class, "java.nio.channels.AlreadyConnectedException");

        // start another server on same host:port
       var centralServerDuplicateAddress = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                                             CENTRAL_SERVER_PORT,
                                                                             Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        Future<Void> startServerDuplicateAddressFuture = toFuture(centralServerDuplicateAddress.start());
        sleep(250); // time to let server start        
        assertTrue(startServerDuplicateAddressFuture.isDone());
        assertExceptionFromCallable(() -> startServerDuplicateAddressFuture.get(), ExecutionException.class, "java.net.BindException: Address already in use");
        
        // start client again
        Future<Void> startClient1FutureAgain = toFuture(client1.start());
        sleep(250); // time to let client start        
        assertTrue(startClient1FutureAgain.isDone());
        assertExceptionFromCallable(() -> startClient1FutureAgain.get(), ExecutionException.class, "java.nio.channels.AlreadyConnectedException");

        // connect another client on same host:port
        var clientDuplicateAddress = new TestDistributedSocketPubSub(1,
                                                                     PubSub.defaultQueueCreator(),
                                                                     PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                                     "client2",
                                                                     "localhost",
                                                                     31001,
                                                                     CENTRAL_SERVER_HOST,
                                                                     CENTRAL_SERVER_PORT);
        Future<Void> startClientDuplicateAddressFuture = toFuture(clientDuplicateAddress.start());
        sleep(250); // time to let client start        
        assertTrue(startClientDuplicateAddressFuture.isDone());
        assertExceptionFromCallable(() -> startClientDuplicateAddressFuture.get(), ExecutionException.class, "java.net.BindException: Cannot assign requested address");
        
        client1.shutdown();
        sleep(250); // time to let client shutdown
        assertExceptionFromCallable(() -> client1.start(), RejectedExecutionException.class);

        centralServer.shutdown();
        sleep(250); // time to let server shutdown
        assertExceptionFromCallable(() -> centralServer.start(), RejectedExecutionException.class);
    }

    /**
     * Test the central server being created after the clients.
     * The clients keep checking if the central server exists via capped exponential backoff.
     * Messages that failed to send before are sent now and relayed to the other client.
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
        assertEquals(0, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        
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
        assertEquals(0, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(0, client2.getCountTypesSent());
        assertEquals(0, client2.getCountTypesReceived());

        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, and in client2 the system will call publisher2.publish(), and similarly for client3
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish (and before central server started): actual=" + words);
        assertThat(words, Matchers.contains("one-s1", "two-s1"));
        assertEquals(0, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(0, client2.getCountTypesSent());
        assertEquals(0, client2.getCountTypesReceived());
        
        assertFalse(toFuture(client1Started).isDone());

        words.clear();
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                             CENTRAL_SERVER_PORT,
                                                             Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        addShutdown(centralServer);
        centralServer.start();
        sleep(250); // time to let the central server start
        sleep(2000); // clients try to connect to the server every 1sec, 2sec, 4sec, 8sec, 8sec so more time to let the clients connect to the central server
        sleep(250); // time to let central server send messages down to the clients

        assertTrue(toFuture(client1Started).isDone());
        System.out.println("after central server started: actual=" + words);
        assertEquals(5, client1.getCountTypesSent()); // identification, createPublisher, addSubscriber, 2 publish messages
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent()); // identification, addSubscriber, addSubscriber
        assertEquals(3, client2.getCountTypesReceived()); // createPublisher, message, message
        assertThat(words, Matchers.containsInAnyOrder("one-s2a", "one-s2b", "two-s2a", "two-s2b"));
    }

    /**
     * In this test the server and two clients start.
     * The server dies.
     * One client creates a publisher and publishes messages.
     * Ensure that the other client receives the messages when the server comes back online.
     */
    @Test
    void testServerRestartsWhileClientRunning() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                             CENTRAL_SERVER_PORT,
                                                             Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        addShutdown(centralServer);
        centralServer.start();
        sleep(250); // time to let server start
        
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
        assertEquals(1, client1.getCountTypesSent()); // Identification
        assertEquals(0, client1.getCountTypesReceived());
        
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
        sleep(250); // time to let client2 start
        assertEquals(1, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(1, client2.getCountTypesSent()); // Identification
        assertEquals(0, client2.getCountTypesReceived());
        
        centralServer.shutdown();
        sleep(250); // time to let central server shutdown

        var centralServer2 = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                              CENTRAL_SERVER_PORT,
                                                              Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        addShutdown(centralServer2);
        centralServer2.start();
        sleep(250); // time to let server start
        sleep(1000); // time to let client1 connect to server as part of exponential backoff
                
        // client1 creates publisher and subscriber
        // client2 creates two publishers and subscribers
        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // because the central server is down, the message is not relayed to the other client
        // in 1sec, 2sec, 4sec, 8sec, 8sec the client will attempt to reconnect to server
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(500); // time to let messages from client1 reach client2
        
        System.out.println("after second central server started: actual=" + words);
        assertEquals(6, client1.getCountTypesSent()); // +5 = Identification, CreatePublisher, AddSubscriber, PublishMessage, PublishMessage
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent()); // +3 = Identification, AddSubscriber, AddSubscriber
        assertEquals(3, client2.getCountTypesReceived()); // +2 = CreatePublisher, PublishMessage, PublishMessage
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1", "one-s2a", "one-s2b", "two-s2a", "two-s2b"));
    }

    /**
     * In this test the server and two clients start.
     * One client creates a publisher and subscribes to the topic, the other client subscribes to the same topic twice, and then the server dies.
     * The client publishes messages.
     * Ensure that the other client receives the messages when the server comes back online.
     */
    @Test
    void testServerRestartsWhileClientRunning2() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                             CENTRAL_SERVER_PORT,
                                                             Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        addShutdown(centralServer);
        centralServer.start();
        sleep(250); // time to let server start
        
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
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // time to let client start
        assertEquals(3, client1.getCountTypesSent()); // Identification, CreatePublisher, AddSubsriber
        assertEquals(0, client1.getCountTypesReceived());
        
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
        assertEquals(3, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent()); // Identification, AddSubsriber, AddSubsriber
        assertEquals(1, client2.getCountTypesReceived()); // CreatePublisher
        assertTrue(client2.getPublisher("hello").isPresent());
        
        centralServer.shutdown();
        sleep(250); // time to let central server shutdown

        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // because the central server is down, the message is not sent to the server not is it relayed to the other client
        // in 1sec, 2sec, 4sec, 8sec, 8sec the client will attempt to reconnect to server
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("right after central server shutdown: actual=" + words);
        assertEquals(3, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(1, client2.getCountTypesReceived());
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1"));
        
        words.clear();
        
        var centralServer2 = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                              CENTRAL_SERVER_PORT,
                                                              Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        addShutdown(centralServer2);
        centralServer2.start();
        sleep(250); // time to let server start
        sleep(1000); // time to let client1 connect to server as part of exponential backoff
        
        // client1 will send the message to centralServer2 which relays it to the other clients
        System.out.println("after central server restarted: actual=" + words);
        assertEquals(8, client1.getCountTypesSent()); // +5 = Identification, CreatePublisher, AddSubscriber, PublishMessage, PublishMessage
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(6, client2.getCountTypesSent()); // +3 = Identification, AddSubscriber, AddSubscriber
        assertEquals(3, client2.getCountTypesReceived()); // +2 = PublishMessage, PublishMessage
        assertThat(words, Matchers.containsInAnyOrder("one-s2a", "one-s2b", "two-s2a", "two-s2b"));
    }
    
    /**
     * Test uniqueness of server id is when server is restarted.
     * There is a central server and 2 clients, and client1 publishes two messages.
     * Central server shuts down and a new one is started.
     * client1 publishes two message.
     * Verify that all 4 server generated ids are unique and increasing.
     */
    @Test
    void testServerGeneratedId() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());

        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                             CENTRAL_SERVER_PORT,
                                                             Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        addShutdown(centralServer);
        centralServer.start();
        
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
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        
        var serverIndexesOfPublishMessageReceivedInClient2 = Collections.synchronizedList(new ArrayList<ServerIndex>());
        var client2 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client2",
                                                      "localhost",
                                                      31002,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client2);
        client2.setMessageReceivedListener(message -> {
            if (message instanceof PublishMessage) {
                ServerIndex serverIndex = PubSubUtils.extractServerIndex(message);
                serverIndexesOfPublishMessageReceivedInClient2.add(serverIndex);
            }
        });
        client2.start();
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // time to let server and clients start, and messages to be relayed
        System.out.println("after first two messages published: actual=" + words);
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("CreatePublisher=1", client2.getTypesReceived());
        assertEquals(0, serverIndexesOfPublishMessageReceivedInClient2.size()); // because no messages published yet
        
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let server and clients start, and messages to be relayed
        System.out.println("after first two messages published: actual=" + words);
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=2", client1.getTypesSent());
        assertEquals("", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("CreatePublisher=1, PublishMessage=2", client2.getTypesReceived());
        assertEquals(2, serverIndexesOfPublishMessageReceivedInClient2.size());
        
        centralServer.shutdown();
        sleep(250); // time to let central server shutdown

        var centralServer2 = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                              CENTRAL_SERVER_PORT,
                                                              Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        addShutdown(centralServer2);
        centralServer2.start();
        sleep(250); // time to let server start
        sleep(1000); // time to let client1 connect to server as part of exponential backoff
        
        System.out.println("after new server started: actual=" + words);
        assertEquals("AddSubscriber=2, CreatePublisher=2, Identification=2, PublishMessage=2", client1.getTypesSent());
        assertEquals("", client1.getTypesReceived());
        assertEquals("AddSubscriber=4, Identification=2", client2.getTypesSent());
        assertEquals("CreatePublisher=1, PublishMessage=2", client2.getTypesReceived());
        assertEquals(2, serverIndexesOfPublishMessageReceivedInClient2.size());
        
        // publish two more messages
        publisher1.publish(new CloneableString("three"));
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after first two messages published: actual=" + words);
        assertEquals("AddSubscriber=2, CreatePublisher=2, Identification=2, PublishMessage=4", client1.getTypesSent());
        assertEquals("", client1.getTypesReceived());
        assertEquals("AddSubscriber=4, Identification=2", client2.getTypesSent());
        assertEquals("CreatePublisher=1, PublishMessage=4", client2.getTypesReceived());
        assertEquals(4, serverIndexesOfPublishMessageReceivedInClient2.size());
        assertIncreasing(serverIndexesOfPublishMessageReceivedInClient2);
    }

    /**
     * In this test the client sends a message which has serverIndex set.
     * This indicates that the server is receiving a message that it already processed.
     * Ensure that the server ignores it.
     */
    @Test
    void testServerIgnoresMessagesAlreadyProcessed() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST,
                                                             CENTRAL_SERVER_PORT,
                                                             Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        addShutdown(centralServer);
        centralServer.start();
        sleep(250); // time to let server start
        
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
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // time to let client start
        assertTrue(client1.getPublisher("hello").isPresent());
        assertEquals(3, client1.getCountTypesSent()); // Identification, CreatePublisher, AddSubsriber
        assertEquals(0, client1.getCountTypesReceived());

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
        assertEquals(3, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent()); // Identification, AddSubsriber, AddSubsriber
        assertEquals(1, client2.getCountTypesReceived()); // CreatePublisher
        assertTrue(client2.getPublisher("hello").isPresent());
        
        assertEquals(6, centralServer.getCountValidTypesReceived());
        assertEquals(1, centralServer.getCountTypesSent());

        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // the client has been modified to tamper the message by adding a serverIndex
        // the server will ignore the message
        client1.enableTamperServerIndex();
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("actual=" + words);
        assertEquals(5, client1.getCountTypesSent());
        assertEquals(2, client1.getCountTypesReceived()); // InvalidRelayMessage, InvalidRelayMessage
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(1, client2.getCountTypesReceived()); // CreatePublisher
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1"));
        
        assertEquals(6, centralServer.getCountValidTypesReceived()); // +0 = invalid message not valid
        assertEquals(3, centralServer.getCountTypesSent()); // +2 = InvalidRelayMessage, InvalidRelayMessage
    }
    
    /**
     * Test performance.
     * There is a central server and 4 clients.
     * 3 clients publish N messages each, and one of them also publishes another N messages.
     * The 4th client receives all of the messages and has 2 subscribers.
     * 
     * <p>On my computer,<br/>
     * With N as 1000 the test takes about 3.8sec.<br/>
     * With N as 100 the test takes around 0.8sec.<br/>
     * 
     * <p>This test also tests that the server does not encounter WritePendingException
     * (where we one thread sends a message to a client while another is also sending a message to it).
     */
    @Test
    void testPerformance() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT, Collections.emptyMap());
        addShutdown(centralServer);
        centralServer.start();
        
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

        var client4 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client4",
                                                      "localhost",
                                                      30004,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client4);
        client4.start();
        
        final int N = 1000; // client1, client2, client3 each publish N messages
        final int totalMessagesHandledByClient4 = N * 4 * 2; // times 2 because there are 2 subscribers in client4
        final CountDownLatch latch = new CountDownLatch(totalMessagesHandledByClient4);
        
        client1.createPublisher("hello", CloneableString.class);
        client2.fetchPublisher("hello");
        client3.fetchPublisher("hello");
        client4.subscribe("hello", "ClientFourSubscriber_First", CloneableString.class, str -> { words.add(str.append("FirstHandler")); latch.countDown(); });
        client4.subscribe("hello", "ClientFourSubscriber_Second", CloneableString.class, str -> { words.add(str.append("SecondHandler")); latch.countDown(); });

        sleep(250);
        var publisher1 = client1.getPublisher("hello").get();
        var publisher2 = client2.getPublisher("hello").get();
        var publisher3 = client3.getPublisher("hello").get();

        Instant startTime = Instant.now();
        
        ExecutorService executor = Executors.newFixedThreadPool(4);
        for (int i = 1; i <= N; i++) {
            String val = Integer.toString(i);
            executor.submit(() -> publisher1.publish(new CloneableString("first message from client1: " + val)));
            executor.submit(() -> publisher1.publish(new CloneableString("second message from client1: " + val)));
            executor.submit(() -> publisher2.publish(new CloneableString("message from client2: " + val)));
            executor.submit(() -> publisher3.publish(new CloneableString("message from client3: " + val)));
        }
        
        double timeTakenMillis;
        try {
            latch.await(30, TimeUnit.SECONDS);
            executor.shutdown();
            executor.awaitTermination(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            timeTakenMillis = Duration.between(startTime, Instant.now()).toNanos() / 1_000_000.0;
            System.out.println("timeTaken=" + timeTakenMillis + "ms");
            System.out.println("actual.length=" + words.size());
        }
     
        assertEquals(totalMessagesHandledByClient4, words.size());
        assertThat(timeTakenMillis, Matchers.lessThan(6000.0));
    }
    
    /**
     * Test the fetch subscriber API.
     * There are 5 clients.
     * 2nd client subscribes and fetches publisher.
     * 3rd client fetches.
     * 1st client creates publisher.
     * 4th client fetches publisher after it is already in the server.
     * 5th client does nothing.
     * Verify that server sends 3 CreatePublisher messages (to client2, client3, client4).
     */
    @Test
    void testFetchPublisher() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        var centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT, Collections.emptyMap());
        addShutdown(centralServer);
        centralServer.start();
        
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

        var client4 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client4",
                                                      "localhost",
                                                      30004,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client4);
        client4.start();
        
        var client5 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client5",
                                                      "localhost",
                                                      30005,
                                                      CENTRAL_SERVER_HOST,
                                                      CENTRAL_SERVER_PORT);
        addShutdown(client5);
        client5.start();
        
        sleep(250); // time to let subscribe and fetchPublisher commands be registered

        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> { });
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> { });
        CompletableFuture<Publisher> futurePublisher2 = client2.fetchPublisher("hello");
        CompletableFuture<Publisher> futurePublisher3 = client3.fetchPublisher("hello");
        sleep(250); // time to let subscribe and fetchPublisher commands be registered
        assertFalse(futurePublisher2.isDone());
        assertFalse(futurePublisher3.isDone());
        assertEquals("AddSubscriber=2, FetchPublisher=2, Identification=5", centralServer.getValidTypesReceived());
        assertEquals("", centralServer.getTypesSent());
        
        client1.createPublisher("hello", CloneableString.class);
        sleep(250); // time to let createSubscriber be handled and publishers sent down to clients
        assertTrue(futurePublisher2.isDone());
        assertTrue(futurePublisher3.isDone());
        assertEquals("AddSubscriber=2, CreatePublisher=1, FetchPublisher=2, Identification=5", centralServer.getValidTypesReceived());
        assertEquals("CreatePublisher=2", centralServer.getTypesSent());

        CompletableFuture<Publisher> futurePublisher4 = client4.fetchPublisher("hello");
        assertFalse(futurePublisher4.isDone());
        Publisher publisher4 = futurePublisher4.get(1000, TimeUnit.MILLISECONDS);
        assertEquals("hello", publisher4.getTopic());
        assertEquals("CloneableString", publisher4.getPublisherClass().getSimpleName());
        assertEquals("AddSubscriber=2, CreatePublisher=1, FetchPublisher=3, Identification=5", centralServer.getValidTypesReceived());
        assertEquals("CreatePublisher=3", centralServer.getTypesSent());

        CompletableFuture<Publisher> repeatFuturePublisher4 = client4.fetchPublisher("hello");
        assertTrue(repeatFuturePublisher4.isDone());
        assertSame(publisher4, repeatFuturePublisher4.get());
        assertEquals("AddSubscriber=2, CreatePublisher=1, FetchPublisher=3, Identification=5", centralServer.getValidTypesReceived()); // unchanged
        assertEquals("CreatePublisher=3", centralServer.getTypesSent());
    }
}



class TestDistributedMessageServer extends DistributedMessageServer {
    private List<String> validTypesReceived = Collections.synchronizedList(new ArrayList<>());
    private List<String> typesSent = Collections.synchronizedList(new ArrayList<>());

    public TestDistributedMessageServer(String host,
                                        int port,
                                        Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        super(host, port, mostRecentMessagesToKeep);
    }

    /**
     * Set SO_REUSEADDR so that the unit tests can close and open channels immediately, not waiting for TIME_WAIT seconds.
     */
    @Override
    protected void onBeforeSocketBound(NetworkChannel channel) throws IOException {
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    }
    
    @Override
    protected void onMessageSent(MessageBase message) {
        typesSent.add(message.getClass().getSimpleName());
    }
    
    @Override
    protected void onValidMessageReceived(MessageBase message) {
        validTypesReceived.add(message.getClass().getSimpleName());
    }
    
    int getCountValidTypesReceived() {
        return validTypesReceived.size();
    }

    String getValidTypesReceived() {
        return countElementsInListByType(validTypesReceived);
    }
    
    int getCountTypesSent() {
        return typesSent.size();
    }

    String getTypesSent() {
        return countElementsInListByType(typesSent);
    }
}


class TestDistributedSocketPubSub extends DistributedSocketPubSub {
    private boolean enableTamperServerIndex;
    private List<String> typesReceived = Collections.synchronizedList(new ArrayList<>());
    private List<String> typesSent = Collections.synchronizedList(new ArrayList<>());
    private Consumer<MessageBase> messageReceivedListener;

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

    void enableTamperServerIndex() {
        enableTamperServerIndex = true;        
    }
    
    void setMessageReceivedListener(Consumer<MessageBase> listener) {
        this.messageReceivedListener = listener;
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
        if (enableTamperServerIndex && message instanceof RelayMessageBase) {
            RelayMessageBase relayMessage = (RelayMessageBase) message;
            relayMessage.setRelayFields(new RelayFields(System.currentTimeMillis(), ServerIndex.MIN_VALUE.increment(), "bogus"));
        }
    }
    
    @Override
    protected void onMessageSent(MessageBase message) {
        typesSent.add(message.getClass().getSimpleName());
    }
    
    @Override
    protected void onMessageReceived(MessageBase message) {
        typesReceived.add(message.getClass().getSimpleName());
        if (messageReceivedListener != null) {
            messageReceivedListener.accept(message);
        }
    }

    int getCountTypesReceived() {
        return typesReceived.size();
    }
    
    String getTypesReceived() {
        return countElementsInListByType(typesReceived);
    }

    int getCountTypesSent() {
        return typesSent.size();
    }
    
    String getTypesSent() {
        return countElementsInListByType(typesSent);
    }
}
