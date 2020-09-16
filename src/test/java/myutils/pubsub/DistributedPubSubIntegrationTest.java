package myutils.pubsub;

import static myutils.TestUtil.assertExceptionFromCallable;
import static myutils.TestUtil.assertIncreasing;
import static myutils.TestUtil.countElementsInListByType;
import static myutils.TestUtil.sleep;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
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
import java.util.stream.Collectors;
import myutils.LogFailureToConsoleTestWatcher;
import myutils.TestBase;
import myutils.TestUtil;
import myutils.pubsub.DistributedMessageServer.ClientMachine;
import myutils.pubsub.InMemoryPubSubIntegrationTest.CloneableString;
import myutils.pubsub.MessageClasses.MessageBase;
import myutils.pubsub.MessageClasses.PublishMessage;
import myutils.pubsub.MessageClasses.RelayFields;
import myutils.pubsub.MessageClasses.RelayMessageBase;
import myutils.pubsub.PubSub.Publisher;
import myutils.pubsub.PubSub.Subscriber;
import myutils.pubsub.PubSub.SubscriptionMessageExceptionHandler;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;


/**
 * Integration test that covers.
 * - PubSub.java
 * - CloneableObject.java
 * - DistributedSocketPubSub.java
 * - DistributedMessageServer.java
 * - MessageClasses.java
 * - SocketTransformer.java
 * - PubSubUtils.java
 * - ServerIndex.java
 * - RetentionPriority.java
 */
@ExtendWith(LogFailureToConsoleTestWatcher.class)
public class DistributedPubSubIntegrationTest extends TestBase {
    private static final System.Logger LOGGER = System.getLogger(DistributedPubSubIntegrationTest.class.getName());

    private final LinkedList<Shutdowneable> shutdowns = new LinkedList<>();
    
    @BeforeEach
    void beforeEachTest() {
    }
    
    @AfterEach
    void invokeShutdowns() {
        System.out.println("invokeShutdowns");
        for (var iter = shutdowns.descendingIterator(); iter.hasNext(); ) {
            var s = iter.next();
            s.shutdown();
        }
        shutdowns.clear();
    }
    //////////////////////////////////////////////////////////////////////

    private static final String CENTRAL_SERVER_HOST = "localhost";
    private static final int CENTRAL_SERVER_PORT = 2101;

    /**
     * Test duplicate clients.
     * 2 clients subscribe with the same name.
     * Verify that the second one is ignored.
     */
    @Test
    void testDuplicateClient() throws IOException {
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer = createServer(Collections.emptyMap());
        startFutures.add(centralServer.start());

        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001);

        startFutures.add(client1.start());
        waitFor(startFutures);
        sleep(250);

        assertEquals("Identification=1", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=1", centralServer.getTypesSent());
        assertThat(centralServer.getRemoteClients().stream().map(clientMachine -> clientMachine.getMachineId().toString()).collect(Collectors.toList()),
                   Matchers.contains("client1"));

        // start a new client on a different port but with the same machine name
        var client1b = createClient(PubSub.defaultQueueCreator(),
                                    PubSub.defaultSubscriptionMessageExceptionHandler(),
                                    "client1", // same name as above
                                    30002);
        var startClient1bFuture = client1b.start();
        sleep(250); // wait for server to receive message

        assertExceptionFromCallable(startClient1bFuture::get, ExecutionException.class,
                                    "myutils.pubsub.PubSubException: Duplicate channel: clientMachine=client1, otherClientChannel=/127.0.0.1:30001");

        assertEquals("Identification=2", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=1, ClientRejected=1", centralServer.getTypesSent());
        assertThat(centralServer.getRemoteClients().stream().map(clientMachine -> clientMachine.getMachineId().toString()).collect(Collectors.toList()),
                   Matchers.contains("client1"));
    }

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
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer = createServer(Collections.emptyMap());
        startFutures.add(centralServer.start());
        sleep(250); // time to let the central server start

        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001
        );
        startFutures.add(client1.start());

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002
        );
        startFutures.add(client2.start());

        var client3 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client3",
                                   30003
        );
        startFutures.add(client3.start());

        waitFor(startFutures);
        assertEquals(1, client1.getCountTypesSent()); // message sent = identification
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(1, client2.getCountTypesSent());
        assertEquals(1, client2.getCountTypesReceived());
        assertEquals(1, client3.getCountTypesSent());
        assertEquals(1, client3.getCountTypesReceived());

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
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent()); // +1 because two subscribers
        assertEquals(1, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent()); // +1
        assertEquals(1, client3.getCountTypesReceived());

        // create publisher on client1
        // this will get replicated to client2 and client3
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        assertTrue(client1.getPublisher("hello").isPresent());
        sleep(250); // time to let publisher be propagated to all other clients
        sleep(250); // time to let each client sent a subscribe command to the server
        assertEquals(3, client1.getCountTypesSent()); // +1 sent CreatePublisher
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(2, client2.getCountTypesReceived()); // +1 received CreatePublisher
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(2, client3.getCountTypesReceived()); // +1 received CreatePublisher
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
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(4, client2.getCountTypesReceived()); // +2
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(4, client3.getCountTypesReceived()); // +2

        // explicitly shutdown client3 (normally this happens in the shutdown hook)
        // publish two messages
        // as client3 has been shutdown it will not receive the message
        client3.shutdown();
        sleep(250); // time to let client shutdown
        assertEquals(5, client1.getCountTypesSent()); // no changes
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(4, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(4, client3.getCountTypesReceived());
        words.clear();
        publisher1.publish(new CloneableString("three"));
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after shutdown client3: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("three-s1", "four-s1", "three-s2a", "three-s2b", "four-s2a", "four-s2b"));
        assertEquals(7, client1.getCountTypesSent()); // +2
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(6, client2.getCountTypesReceived()); // +2
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(4, client3.getCountTypesReceived());

        // unsubscribe one subscriber in client2
        // publish two messages
        // as client2 still has one subscriber in the "hello" topic, client2 still receives the message
        client2.unsubscribe(subscriber2b);
        sleep(250); // time to let central server know that one subscribe in client2 unsubscribed
        assertEquals(7, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent()); // +1
        assertEquals(6, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(4, client3.getCountTypesReceived());
        words.clear();
        publisher1.publish(new CloneableString("five"));
        publisher1.publish(new CloneableString("six"));
        sleep(250); // time to let messages be published to client2
        System.out.println("after unsubscribe2b: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("five-s1", "six-s1", "five-s2a", "six-s2a"));
        assertEquals(9, client1.getCountTypesSent()); // +2
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent());
        assertEquals(8, client2.getCountTypesReceived()); // +2
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(4, client3.getCountTypesReceived());

        // unsubscribe the last subscriber in client2
        // publish two messages
        // as client3 has been shutdown and client2 has unsubscribed, only client1 will receive the message
        client2.unsubscribe(subscriber2a);
        sleep(250); // time to let central server know that client2 unsubscribed
        assertEquals(9, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(5, client2.getCountTypesSent()); // +1
        assertEquals(8, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(4, client3.getCountTypesReceived());
        words.clear();
        publisher1.publish(new CloneableString("seven"));
        publisher1.publish(new CloneableString("eight"));
        sleep(250); // time to let messages be published to client2
        System.out.println("after unsubscribe2a: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("seven-s1", "eight-s1"));
        assertEquals(11, client1.getCountTypesSent()); // +2
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(5, client2.getCountTypesSent());
        assertEquals(8, client2.getCountTypesReceived()); // unchanged, client2 does not receive messages as it is no longer subscribed
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(4, client3.getCountTypesReceived());
    }

    /**
     * Test a client coming online after messages have been published.
     * The new client does not receive the messages which were published earlier.
     * However, the new client can call download to retrieve the old messages.
     */
    @Test
    void testDownloadMessages() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 2, RetentionPriority.MEDIUM, 3));
        waitFor(Collections.singletonList(centralServer.start()));
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        waitFor(Collections.singletonList(client1.start()));
        assertEquals(1, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        
        // create publishers on client1
        // as client2 does not exist yet, nor is subscribed, it does not receive the publisher
        Publisher helloPublisher1 = client1.createPublisher("hello", CloneableString.class);
        Publisher worldPublisher1 = client1.createPublisher("world", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1-hello")));
        client1.subscribe("world", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1-world")));
        sleep(250); // time to let publisher be propagated to client2, but this client does not exist yet
        assertEquals(5, client1.getCountTypesSent()); // sent identification, create publisher * 2, add subscriber * 2
        assertEquals(1, client1.getCountTypesReceived()); // ClientAccepted
        
        // publish messages with different retentions
        // the server will remember the last 3 messages with low retention, and the last 2 messages with high retention
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, but client2 is not yet running
        helloPublisher1.publish(new CloneableString("ImportantOne"), RetentionPriority.HIGH);
        helloPublisher1.publish(new CloneableString("ImportantTwo"), RetentionPriority.HIGH);
        helloPublisher1.publish(new CloneableString("ImportantThree"), RetentionPriority.HIGH);
        helloPublisher1.publish(new CloneableString("apple"), RetentionPriority.MEDIUM);
        helloPublisher1.publish(new CloneableString("banana"), RetentionPriority.MEDIUM);
        helloPublisher1.publish(new CloneableString("carrot"));
        helloPublisher1.publish(new CloneableString("dragonfruit"), RetentionPriority.MEDIUM);
        worldPublisher1.publish(new CloneableString("ImportantOne"), RetentionPriority.HIGH);
        worldPublisher1.publish(new CloneableString("ImportantTwo"), RetentionPriority.HIGH);
        worldPublisher1.publish(new CloneableString("ImportantThree"), RetentionPriority.HIGH);
        worldPublisher1.publish(new CloneableString("apple"), RetentionPriority.MEDIUM);
        worldPublisher1.publish(new CloneableString("banana"), RetentionPriority.MEDIUM);
        worldPublisher1.publish(new CloneableString("carrot"));
        worldPublisher1.publish(new CloneableString("dragonfruit"), RetentionPriority.MEDIUM);
        sleep(250); // time to let messages be published to client2
        System.out.println("before client2 exists: actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantOne-s1-hello", "ImportantTwo-s1-hello", "ImportantThree-s1-hello",
                                     "apple-s1-hello", "banana-s1-hello", "carrot-s1-hello", "dragonfruit-s1-hello",
                                     "ImportantOne-s1-world", "ImportantTwo-s1-world", "ImportantThree-s1-world",
                                     "apple-s1-world", "banana-s1-world", "carrot-s1-world", "dragonfruit-s1-world"));
        assertEquals(19, client1.getCountTypesSent()); // +14 = PublishMessage
        assertEquals(1, client1.getCountTypesReceived());

        words.clear();
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        assertFalse(client2.getPublisher("hello").isPresent());
        waitFor(Collections.singletonList(client2.start()));
        sleep(250); // time to let client2 start
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(1, client2.getCountTypesSent());
        assertEquals("ClientAccepted=1", client2.getTypesReceived()); // does not receive as client2 not subscribed to topic "hello"
        
        client2.subscribe("hello", "ClientTwo_HelloSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a-hello")));
        client2.subscribe("hello", "ClientTwo_HelloSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b-hello")));
        client2.subscribe("world", "ClientTwo_WorldSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a-world")));
        sleep(250); // time to let client2 subscribe and for server to send down the publisher
        assertTrue(client2.getPublisher("hello").isPresent());
        assertTrue(client2.getPublisher("world").isPresent());
        assertThat(words, Matchers.empty());
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent()); // +3 = add subscriber
        assertEquals("ClientAccepted=1, CreatePublisher=2", client2.getTypesReceived()); // +2 = create publisher
        
        client2.download(List.of("hello", "world"), ServerIndex.MIN_VALUE, ServerIndex.MAX_VALUE);
        sleep(250); // time to let messages be sent to client2
        System.out.println("after client2 downloads: actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantTwo-s2a-hello", "ImportantTwo-s2b-hello", "ImportantThree-s2a-hello", "ImportantThree-s2b-hello",
                                     "banana-s2a-hello", "banana-s2b-hello", "carrot-s2a-hello", "carrot-s2b-hello", "dragonfruit-s2a-hello", "dragonfruit-s2b-hello",
                                     "ImportantTwo-s2a-world", "ImportantThree-s2a-world",
                                     "banana-s2a-world", "carrot-s2a-world", "dragonfruit-s2a-world"));
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(5, client2.getCountTypesSent()); // +1 = DownloadPublishedMessages
        assertEquals("ClientAccepted=1, CreatePublisher=2, PublishMessage=10", client2.getTypesReceived()); // +10 = PublishMessage

        // verify that messages can be downloaded a second time
        words.clear();
        client2.download(List.of("hello"), ServerIndex.MIN_VALUE, ServerIndex.MAX_VALUE);
        sleep(250); // time to let messages be sent to client2
        System.out.println("after client2 downloads a second time: actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantTwo-s2a-hello", "ImportantTwo-s2b-hello", "ImportantThree-s2a-hello", "ImportantThree-s2b-hello",
                                     "banana-s2a-hello", "banana-s2b-hello", "carrot-s2a-hello", "carrot-s2b-hello", "dragonfruit-s2a-hello", "dragonfruit-s2b-hello"));
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(6, client2.getCountTypesSent()); // +1 = DownloadPublishedMessages
        assertEquals("ClientAccepted=1, CreatePublisher=2, PublishMessage=15", client2.getTypesReceived()); // +5 = PublishMessage
        
        // verify error if download a topic that does not exist
        words.clear();
        client2.download(List.of("NewTopic"), ServerIndex.MIN_VALUE, ServerIndex.MAX_VALUE);
        sleep(250); // time to let messages be sent to client2
        System.out.println("download topic that does not exist: actual=" + words);
        assertThat(words, Matchers.empty());
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(7, client2.getCountTypesSent()); // +1 = DownloadPublishedMessages
        assertEquals("ClientAccepted=1, CreatePublisher=2, InvalidMessage=1, PublishMessage=15", client2.getTypesReceived()); // +1 = InvalidMessage

        // verify error if download a topic to which we are not subscribed
        client1.createPublisher("NewTopic", CloneableString.class);
        sleep(250); // time to let messages be sent to client2
        words.clear();
        client2.download(List.of("NewTopic"), ServerIndex.MIN_VALUE, ServerIndex.MAX_VALUE);
        sleep(250); // time to let messages be sent to client2
        System.out.println("download messages for a topic to which we are not subscribed: actual=" + words);
        assertThat(words, Matchers.empty());
        assertEquals(20, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(8, client2.getCountTypesSent()); // +1 = DownloadPublishedMessages
        assertEquals("ClientAccepted=1, CreatePublisher=2, InvalidMessage=2, PublishMessage=15", client2.getTypesReceived()); // +1 = InvalidMessage
    }
    
    /**
     * In this test we shutdown the client and server.
     * Verify that restart fails.
     */
    @Test
    void testRestartFails() throws IOException, InterruptedException, ExecutionException {
        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        Future<Void> startCentralServerFuture = centralServer.start();
        assertFalse(startCentralServerFuture.isDone());
        startCentralServerFuture.get(); // assert no exception
        assertTrue(startCentralServerFuture.isDone());

        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        Future<Void> startClient1Future = client1.start();
        assertFalse(startClient1Future.isDone());
        startClient1Future.get(); // assert no exception
        assertTrue(startClient1Future.isDone());
        assertEquals(1, client1.getCountTypesSent()); // Identification
        assertEquals(1, client1.getCountTypesReceived()); // ClientAccepted
        
        // start server again
        Future<Void> startCentralServerFutureAgain = client1.start();
        assertFalse(startCentralServerFutureAgain.isDone());
        sleep(250); // time to let server start        
        assertTrue(startCentralServerFutureAgain.isDone());
        assertExceptionFromCallable(startCentralServerFutureAgain::get, ExecutionException.class, "java.nio.channels.AlreadyConnectedException");

        // start another server on same host:port
        var centralServerDuplicateAddress = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        Future<Void> startServerDuplicateAddressFuture = centralServerDuplicateAddress.start();
        sleep(250); // time to let server start        
        assertTrue(startServerDuplicateAddressFuture.isDone());
        assertExceptionFromCallable(startServerDuplicateAddressFuture::get, ExecutionException.class, "java.net.BindException: Address already in use");
        
        // start client again
        Future<Void> startClient1FutureAgain = client1.start();
        sleep(250); // time to let client start        
        assertTrue(startClient1FutureAgain.isDone());
        assertExceptionFromCallable(startClient1FutureAgain::get, ExecutionException.class, "java.nio.channels.AlreadyConnectedException");

        // connect another client on same host:port
        var clientDuplicateAddress = createClient(PubSub.defaultQueueCreator(),
                                                  PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                  "client2",
                                                  31001
        );
        Future<Void> startClientDuplicateAddressFuture = clientDuplicateAddress.start();
        sleep(250); // time to let client start        
        assertTrue(startClientDuplicateAddressFuture.isDone());
        assertExceptionFromCallable(startClientDuplicateAddressFuture::get, ExecutionException.class, "java.net.BindException: Cannot assign requested address");
        
        client1.shutdown();
        sleep(250); // time to let client shutdown
        assertExceptionFromCallable(client1::start, RejectedExecutionException.class);

        centralServer.shutdown();
        sleep(250); // time to let server shutdown
        assertExceptionFromCallable(centralServer::start, RejectedExecutionException.class);
    }

    /**
     * Test the central server being created after the clients.
     * The clients keep checking if the central server exists via capped exponential backoff.
     * Messages that failed to send before are sent now and relayed to the other client.
     */
    @Test
    void testCreateClientBeforeServer() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        CompletableFuture<Void> client1Started = client1.start();
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // time to let client start
        assertTrue(client1.getPublisher("hello").isPresent());
        assertEquals(0, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
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
        
        assertFalse(client1Started.isDone());

        words.clear();
        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        centralServer.start();
        sleep(250); // time to let the central server start
        sleep(2000); // clients try to connect to the server every 1sec, 2sec, 4sec, 8sec, 8sec so more time to let the clients connect to the central server
        sleep(250); // time to let central server send messages down to the clients

        assertTrue(client1Started.isDone());
        System.out.println("after central server started: actual=" + words);
        assertEquals(5, client1.getCountTypesSent()); // identification, createPublisher, addSubscriber, 2 publish messages
        assertEquals(1, client1.getCountTypesReceived()); // ClientAccepted
        assertEquals(3, client2.getCountTypesSent()); // identification, addSubscriber, addSubscriber
        assertEquals(4, client2.getCountTypesReceived()); // ClientAccepted, CreatePublisher, PublishMessage, PublishMessage
        assertThat(words, Matchers.containsInAnyOrder("one-s2a", "one-s2b", "two-s2a", "two-s2b"));
    }

    /**
     * In this test the server and two clients start.
     * The server dies and a new one is started.
     * One client creates a publisher, another subscribes, and the first client publishes messages.
     * Ensure that the other client receives the messages.
     */
    @Test
    void testShutdownAndStartNewServer1() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        waitFor(Collections.singletonList(centralServer.start()));
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        waitFor(Collections.singletonList(client1.start()));
        assertEquals(1, client1.getCountTypesSent()); // Identification
        assertEquals(1, client1.getCountTypesReceived()); // ClientAccepted
        
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        waitFor(Collections.singletonList(client2.start()));
        assertEquals(1, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(1, client2.getCountTypesSent()); // Identification
        assertEquals(1, client2.getCountTypesReceived()); // ClientAccepted
        
        centralServer.shutdown();
        sleep(250); // time to let central server shutdown

        var centralServer2 = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        waitFor(Collections.singletonList(centralServer2.start()));
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
        assertEquals(2, client1.getCountTypesReceived()); // both ClientAccepted
        assertEquals(4, client2.getCountTypesSent()); // +3 = Identification, AddSubscriber, AddSubscriber
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=2", client2.getTypesReceived());
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1", "one-s2a", "one-s2b", "two-s2a", "two-s2b"));
    }

    /*
     * In this test the server and two clients start.
     * One client creates a publisher and subscribes to the topic, the other client subscribes to the same topic twice.
     * The server dies.
     * The first client publishes messages. The subscribers in the first client receive the messages right away.
     * Ensure that the other client receives the messages when a new server is started.
     */
    @Test
    void testShutdownAndStartNewServer2() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        waitFor(Collections.singletonList(centralServer.start()));
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        waitFor(Collections.singletonList(client1.start()));
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // wait for CreatePublisher and AddSubscriber commands to be sent
        assertEquals(3, client1.getCountTypesSent()); // Identification, CreatePublisher, AddSubscriber
        assertEquals(1, client1.getCountTypesReceived()); // ClientAccepted

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        waitFor(Collections.singletonList(client2.start()));
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // wait for CreatePublisher and AddSubscriber commands to be sent
        assertEquals(3, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent()); // Identification, AddSubscriber, AddSubscriber
        assertEquals(2, client2.getCountTypesReceived()); // ClientAccepted, CreatePublisher
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
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(2, client2.getCountTypesReceived());
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1"));
        
        words.clear();
        
        var centralServer2 = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        waitFor(Collections.singletonList(centralServer2.start()));
        sleep(1000); // time to let client1 connect to server as part of exponential backoff
        sleep(250); // more time to let messages be downloaded to client2
        
        // client1 will send the message to centralServer2 which relays it to the other clients
        System.out.println("after central server restarted: actual=" + words);
        assertEquals(8, client1.getCountTypesSent()); // +5 = Identification, CreatePublisher, AddSubscriber, PublishMessage, PublishMessage
        assertEquals(2, client1.getCountTypesReceived());
        assertEquals(6, client2.getCountTypesSent()); // +3 = Identification, AddSubscriber, AddSubscriber
        assertEquals(5, client2.getCountTypesReceived()); // +3 = ClientAccepted, PublishMessage, PublishMessage
        assertThat(words, Matchers.containsInAnyOrder("one-s2a", "one-s2b", "two-s2a", "two-s2b"));
    }
    
    /**
     * In this test there is a server and 2 clients.
     * Both clients die and new ones are started.
     * Verify that the server removes the client machines when it detects they are gone.
     * Verify that the new client receives publisher and subscriber already exist errors when they call createPublisher and addSubscriber.
     */
    @Test
    void testShutdownAndStartNewClient() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());

        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 8));
        waitFor(Collections.singletonList(centralServer.start()));
        
        {
            // START: this block of code almost the same as in both blocks

            List<CompletableFuture<Void>> startFutures = new ArrayList<>();
            
            var client1 = createClient(PubSub.defaultQueueCreator(),
                                       PubSub.defaultSubscriptionMessageExceptionHandler(),
                                       "client1",
                                       31001
            );
            startFutures.add(client1.start());
            
            var client2 = createClient(PubSub.defaultQueueCreator(),
                                       PubSub.defaultSubscriptionMessageExceptionHandler(),
                                       "client2",
                                       31002
            );
            startFutures.add(client2.start());

            waitFor(startFutures);

            Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
            client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
            client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
            client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
            sleep(250); // time to let server and clients start, and messages to be relayed
            System.out.println("after server and clients started: actual=" + words);
            assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent());
            assertEquals("ClientAccepted=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
            assertEquals("ClientAccepted=1, CreatePublisher=1", client2.getTypesReceived());
            assertThat(words, Matchers.empty());
            
            words.clear();
            publisher1.publish(new CloneableString("one"));
            publisher1.publish(new CloneableString("two"));
            sleep(250); // time to let server and clients start, and messages to be relayed
            System.out.println("after two messages (one, two) published: actual=" + words);
            assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=2", client1.getTypesSent());
            assertEquals("ClientAccepted=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
            assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=2", client2.getTypesReceived());
            assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1", "one-s2a", "one-s2b", "two-s2a", "two-s2b"));

            assertThat(centralServer.getRemoteClients().stream().map(clientMachine -> clientMachine.getMachineId().toString()).collect(Collectors.toSet()),
                       Matchers.containsInAnyOrder("client1", "client2"));
            
            // END: this block of code almost the same as in both blocks
            
            // shutdown clients:

            client2.shutdown();
            sleep(250); // time to let client shutdown
            
            words.clear();
            
            publisher1.publish(new CloneableString("three"));
            publisher1.publish(new CloneableString("four"));
            sleep(250); // time to let server and clients start, and messages to be relayed
            System.out.println("after two messages (three, four) published and client2 shutdown: actual=" + words);
            assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=4", client1.getTypesSent()); // PublishMessage +2
            assertEquals("ClientAccepted=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent()); // unchanged
            assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=2", client2.getTypesReceived()); // unchanged as client2 is shutdown
            assertThat(words, Matchers.contains("three-s1", "four-s1"));

            client1.shutdown();
            sleep(250); // time to let client shutdown
        }
        
        assertEquals("AddSubscriber=3, CreatePublisher=1, Identification=2, PublishMessage=4", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=2", centralServer.getTypesSent());
        assertEquals(Set.of(), centralServer.getRemoteClients());
        
        words.clear();
        System.out.println("about to start new clients");
        
        {
            // START: this block of code almost the same as in both blocks
            
            List<CompletableFuture<Void>> startFutures = new ArrayList<>();

            var client1 = createClient(PubSub.defaultQueueCreator(),
                                       PubSub.defaultSubscriptionMessageExceptionHandler(),
                                       "client1",
                                       31001
            );
            startFutures.add(client1.start());
            
            var client2 = createClient(PubSub.defaultQueueCreator(),
                                       PubSub.defaultSubscriptionMessageExceptionHandler(),
                                       "client2",
                                       31002
            );
            startFutures.add(client2.start());

            waitFor(startFutures);

            Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
            client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
            client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
            client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
            sleep(250); // time to let server and clients start, and messages to be relayed
            System.out.println("after clients restarted: actual=" + words);
            assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent());
            assertEquals("ClientAccepted=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
            assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=2", client2.getTypesReceived()); // differences: PublishMessage +2 because "three" and "four" sent to client
            assertThat(words, Matchers.contains("three-s2a", "three-s2b", "four-s2a", "four-s2b")); // differences: handle 2 new messages
            
            words.clear();
            publisher1.publish(new CloneableString("five")); // difference: above we publish "three"
            publisher1.publish(new CloneableString("six")); // difference: above we publish "four"
            sleep(250); // time to let server and clients start, and messages to be relayed
            System.out.println("after clients restarted two messages (five, six) published: actual=" + words);
            assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=2", client1.getTypesSent());
            assertEquals("ClientAccepted=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
            assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=4", client2.getTypesReceived()); // PublishMessage +2
            assertThat(words, Matchers.contains("five-s1", "six-s1", "five-s2a", "five-s2b", "six-s2a", "six-s2b"));
            
            assertThat(centralServer.getRemoteClients().stream().map(clientMachine -> clientMachine.getMachineId().toString()).collect(Collectors.toSet()),
                       Matchers.containsInAnyOrder("client1", "client2"));

            // END: this block of code almost the same as in both blocks
        }
        
        assertEquals("AddSubscriber=6, CreatePublisher=2, Identification=4, PublishMessage=6", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=4, CreatePublisher=2, PublishMessage=6", centralServer.getTypesSent());
        
        codeCoverageForClientMachine(centralServer.getRemoteClients().toArray(new ClientMachine[0]));
    }
    
    private void codeCoverageForClientMachine(ClientMachine[] remoteClients) {
        assertEquals(2, remoteClients.length);
        Arrays.sort(remoteClients, Comparator.comparing(ClientMachine::getMachineId));
        ClientMachine client1 = remoteClients[0];
        ClientMachine client2 = remoteClients[1];
        assertEquals("client1", client1.getMachineId().toString());
        assertEquals("client2", client2.getMachineId().toString());
        assertNotEquals(client1.hashCode(), client2.hashCode());
        assertNotEquals(client1, null);
        // ClientMachine::toString is mostly used for how the debugger renders the object
        assertEquals("client1@/127.0.0.1:31001", client1.toString());
        assertEquals("client2@/127.0.0.1:31002", client2.toString());
    }
    
    /**
     * Test uniqueness of server id is when server is restarted.
     * There is a central server and 2 clients, and client1 publishes two messages.
     * Central server shuts down and a new one is started.
     * client1 publishes two message.
     * Verify that all 4 server generated ids are unique and increasing.
     */
    @Test
    void testServerGeneratedId() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        startFutures.add(centralServer.start());
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        startFutures.add(client1.start());
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        
        var serverIndexesOfPublishMessageReceivedInClient2 = Collections.synchronizedList(new ArrayList<ServerIndex>());
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        client2.setMessageReceivedListener(message -> {
            if (message instanceof PublishMessage) {
                ServerIndex serverIndex = PubSubUtils.extractServerIndex(message);
                serverIndexesOfPublishMessageReceivedInClient2.add(serverIndex);
            }
        });
        startFutures.add(client2.start());
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));

        waitFor(startFutures);
        sleep(250); // wait for CreatePublisher and AddSubscriber messages to get sent
        System.out.println("after server and clients started: actual=" + words);
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisher=1", client2.getTypesReceived());
        assertEquals(0, serverIndexesOfPublishMessageReceivedInClient2.size()); // because no messages published yet
        
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let server and clients start, and messages to be relayed
        System.out.println("after first two messages published: actual=" + words);
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=2", client1.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=2", client2.getTypesReceived());
        assertEquals(2, serverIndexesOfPublishMessageReceivedInClient2.size());
        
        centralServer.shutdown();
        sleep(250); // time to let central server shutdown

        var centralServer2 = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        waitFor(Collections.singletonList(centralServer2.start()));
        sleep(1000); // time to let client1 connect to server as part of exponential backoff
        
        System.out.println("after new server started: actual=" + words);
        assertEquals("AddSubscriber=2, CreatePublisher=2, Identification=2, PublishMessage=2", client1.getTypesSent());
        assertEquals("ClientAccepted=2", client1.getTypesReceived());
        assertEquals("AddSubscriber=4, Identification=2", client2.getTypesSent());
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=2", client2.getTypesReceived());
        assertEquals(2, serverIndexesOfPublishMessageReceivedInClient2.size());
        
        // publish two more messages
        publisher1.publish(new CloneableString("three"));
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after first two messages published: actual=" + words);
        assertEquals("AddSubscriber=2, CreatePublisher=2, Identification=2, PublishMessage=4", client1.getTypesSent());
        assertEquals("ClientAccepted=2", client1.getTypesReceived());
        assertEquals("AddSubscriber=4, Identification=2", client2.getTypesSent());
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=4", client2.getTypesReceived());
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
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        startFutures.add(centralServer.start());
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        startFutures.add(client1.start());
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        waitFor(startFutures);
        sleep(250); // wait for CreatePublisher and AddSubscriber commands to be sent
        assertTrue(client1.getPublisher("hello").isPresent());
        assertEquals(3, client1.getCountTypesSent()); // Identification, CreatePublisher, AddSubscriber
        assertEquals(1, client1.getCountTypesReceived()); // ClientAccepted

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        waitFor(Collections.singletonList(client2.start()));
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // wait for CreatePublisher and AddSubscriber commands to be sent
        assertEquals(3, client1.getCountTypesSent());
        assertEquals(1, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent()); // Identification, AddSubscriber, AddSubscriber
        assertEquals(2, client2.getCountTypesReceived()); // ClientAccepted, CreatePublisher
        assertTrue(client2.getPublisher("hello").isPresent());
        
        assertEquals(6, centralServer.getCountValidTypesReceived());
        assertEquals(3, centralServer.getCountTypesSent());

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
        assertEquals(3, client1.getCountTypesReceived()); // ClientAccepted, InvalidRelayMessage, InvalidRelayMessage
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(2, client2.getCountTypesReceived()); // ClientAccepted, CreatePublisher
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1"));
        
        assertEquals(6, centralServer.getCountValidTypesReceived()); // +0 = invalid message not valid
        assertEquals(5, centralServer.getCountTypesSent()); // +2 = InvalidRelayMessage, InvalidRelayMessage
    }
    
    /**
     * Test performance.
     * There is a central server and 4 clients.
     * 3 clients publish N messages each, and one of them also publishes another N messages.
     * The 4th client receives all of the messages and has 2 subscribers.
     * 
     * <p>On my Linux laptop Intel Core i7-3720QM CPU @ 2.60GHz * 4,<br/>
     * With N as 1000 the test takes about 1.0sec at INFO level, and 2.9sec at TRACE level<br/>
     *
     * <p>On my MacOS 2.3GHz Intel Core i9,<br/>
     * With N as 1000 the test takes about 2.1sec.<br/>
     * 
     * <p>This test also tests that the server does not encounter WritePendingException
     * (where we one thread sends a message to a client while another is also sending a message to it).
     */
    @Test
    void testPerformance() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();
        
        var centralServer = createServer(Collections.emptyMap());
        startFutures.add(centralServer.start());
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001
        );
        startFutures.add(client1.start());
        
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002
        );
        startFutures.add(client2.start());
        
        var client3 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client3",
                                   30003
        );
        startFutures.add(client3.start());

        var client4 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client4",
                                   30004
        );
        startFutures.add(client4.start());

        waitFor(startFutures);
        
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
     * Test the fetch publisher API.
     * There are 7 clients.
     * 2nd client subscribes and fetches publisher.
     * 3rd client fetches publisher and subscribes.
     * 4th client fetches.
     * 1st client creates publisher.
     * 5th client fetches publisher after it is already in the server.
     * 6th client fetches a different publisher. Upon being shut down and a new one started, a CreatePublisher is not sent to the client machine with the same name.
     * Verify that server sends 4 CreatePublisher messages (to client2, client3, client4, client5).
     */
    @Test
    void testFetchPublisher() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer = createServer(Collections.emptyMap());
        startFutures.add(centralServer.start());
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001
        );
        startFutures.add(client1.start());
        
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002
        );
        startFutures.add(client2.start());
        
        var client3 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client3",
                                   30003
        );
        startFutures.add(client3.start());

        var client4 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client4",
                                   30004
        );
        startFutures.add(client4.start());
        
        var client5 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client5",
                                   30005
        );
        startFutures.add(client5.start());
        
        var client6 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client6",
                                   30006
        );
        startFutures.add(client6.start());

        waitFor(startFutures);

        // 2nd client subscribes and fetches publisher.
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> { });
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> { });
        CompletableFuture<Publisher> futurePublisher2 = client2.fetchPublisher("hello");
        
        // 3rd client fetches publisher and subscribes.
        CompletableFuture<Publisher> futurePublisher3 = client3.fetchPublisher("hello");
        client3.subscribe("hello", "ClientThreeSubscriber_First", CloneableString.class, str -> { });
        client3.subscribe("hello", "ClientThreeSubscriber_Second", CloneableString.class, str -> { });
        sleep(250); // time to let server register futurePublisher3 and deregister it because of the addSubscriber command, and to wait for futurePublisher4 to run

        // 4th client fetches.
        CompletableFuture<Publisher> futurePublisher4 = client4.fetchPublisher("hello");
        
        sleep(500); // time to let subscribe and fetchPublisher commands be registered
        assertFalse(futurePublisher2.isDone());
        assertFalse(futurePublisher3.isDone());
        assertFalse(futurePublisher4.isDone());
        assertEquals("AddSubscriber=4, FetchPublisher=3, Identification=6", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=6", centralServer.getTypesSent());
        
        // 1st client creates publisher.
        client1.createPublisher("hello", CloneableString.class);
        sleep(250); // time to let createSubscriber be handled and publishers sent down to clients
        assertTrue(futurePublisher2.isDone());
        assertTrue(futurePublisher3.isDone());
        assertTrue(futurePublisher4.isDone());
        assertEquals("AddSubscriber=4, CreatePublisher=1, FetchPublisher=3, Identification=6", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=6, CreatePublisher=3", centralServer.getTypesSent());

        // 5th client fetches publisher after it is already in the server.
        CompletableFuture<Publisher> futurePublisher5 = client5.fetchPublisher("hello");
        CompletableFuture<Publisher> futurePublisher5b = client5.fetchPublisher("hello");
        assertFalse(futurePublisher5.isDone());
        assertSame(futurePublisher5, futurePublisher5b);
        Publisher publisher5 = futurePublisher5.get(1000, TimeUnit.MILLISECONDS);
        assertEquals("hello", publisher5.getTopic());
        assertEquals("CloneableString", publisher5.getPublisherClass().getSimpleName());
        assertEquals("AddSubscriber=4, CreatePublisher=1, FetchPublisher=4, Identification=6", centralServer.getValidTypesReceived()); // FetchPublisher +1
        assertEquals("ClientAccepted=6, CreatePublisher=4", centralServer.getTypesSent()); // CreatePublisher +1

        CompletableFuture<Publisher> repeatFuturePublisher5 = client5.fetchPublisher("hello");
        assertTrue(repeatFuturePublisher5.isDone());
        assertNotSame(futurePublisher5, repeatFuturePublisher5);
        assertSame(publisher5, repeatFuturePublisher5.get());
        
        // 6th client fetches a different publisher.
        CompletableFuture<Publisher> futurePublisher6 = client5.fetchPublisher("world");
        sleep(250); // wait for server to receive fetch command
        assertFalse(futurePublisher6.isDone());
        client6.shutdown();
        sleep(250); // wait for server to detect that client6 is shutdown
        
        // Verify that server sends 4 CreatePublisher messages (to client2, client3, client4, client5).
        assertEquals("AddSubscriber=4, CreatePublisher=1, FetchPublisher=5, Identification=6", centralServer.getValidTypesReceived()); // unchanged
        assertEquals("ClientAccepted=6, CreatePublisher=4", centralServer.getTypesSent());
        
        // Upon being shut down and a new one started, a CreatePublisher is not sent to the client machine with the same name.
        var client6b = createClient(PubSub.defaultQueueCreator(),
                                    PubSub.defaultSubscriptionMessageExceptionHandler(),
                                    "client6",
                                    30006
        );
        waitFor(Collections.singletonList(client6b.start()));
        sleep(250); // wait for central server to send any CreatePublisher commands
        assertEquals("AddSubscriber=4, CreatePublisher=1, FetchPublisher=5, Identification=7", centralServer.getValidTypesReceived()); // unchanged: i.e. FetchPublisher for client6 not sent
        assertEquals("ClientAccepted=7, CreatePublisher=4", centralServer.getTypesSent()); // CreatePublisher not sent to client6b
        assertFalse(futurePublisher6.isDone());
    }

    /**
     * In this test a client sends CreatePublisher before it has sent Identification, causing the server to reject the CreatePublisher.
     * The client then starts normally and sends messages that the server does not know how to handle.
     */
    @Test
    void testUnsupportedMessages() throws IOException, NoSuchFieldException, NoSuchMethodException, SecurityException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();
        
        var centralServer = createServer(Collections.emptyMap());
        startFutures.add(centralServer.start());
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001
        );
        startFutures.add(client1.start());
        
        waitFor(startFutures);
        sleep(250);

        assertEquals("Identification=1", client1.getTypesSent());
        assertEquals("Identification=1", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=1", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        
        var messageWriterField = DistributedSocketPubSub.class.getDeclaredField("messageWriter");
        messageWriterField.setAccessible(true);
        var messageWriter = messageWriterField.get(client1);
        var sendMethod = messageWriter.getClass().getDeclaredMethod("internalPutMessage", MessageBase.class);
        sendMethod.setAccessible(true);
        
        BogusClientGeneratedMessage bogus1 = new BogusClientGeneratedMessage();
        sendMethod.invoke(messageWriter, bogus1);
        sleep(250);
        assertEquals("BogusClientGeneratedMessage=1, Identification=1", client1.getTypesSent());
        assertEquals("Identification=1", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=1, UnsupportedMessage=1", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1, UnsupportedMessage=1", client1.getTypesReceived());
        
        BogusMessage bogus2 = new BogusMessage();
        sendMethod.invoke(messageWriter, bogus2);
        sleep(250);
        assertEquals("BogusClientGeneratedMessage=1, BogusMessage=1, Identification=1", client1.getTypesSent());
        assertEquals("Identification=1", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=1, UnsupportedMessage=2", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1, UnsupportedMessage=2", client1.getTypesReceived()); // because nothing received as message reader is not started
        
        // verify that log shows
        // WARNING: Unsupported  message from client: clientMachine=client1, BogusClientGeneratedMessage
        // WARNING: Unsupported  message from client: clientMachine=/127.0.0.1:30001, BogusMessage
    }
    
    private static class BogusClientGeneratedMessage extends MessageClasses.ClientGeneratedMessage {
        private static final long serialVersionUID = 1L;

        BogusClientGeneratedMessage() {
            super(System.currentTimeMillis());
        }

        @Override
        public String toLoggingString() {
            return "BogusClientGeneratedMessage";
        }
    }
    
    private static class BogusMessage implements MessageClasses.MessageBase {
        private static final long serialVersionUID = 1L;

        @Override
        public String toLoggingString() {
            return "BogusMessage";
        }
    }
    
    /**
     * In this test a client sends CreatePublisher before it has sent Identification, causing the server to reject the CreatePublisher.
     */
    @Test
    void testRequestIdentification() throws IOException, NoSuchFieldException, SecurityException, IllegalArgumentException, IllegalAccessException {
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer = createServer(Collections.emptyMap());
        startFutures.add(centralServer.start());
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001
        );
        startFutures.add(client1.start());
        
        waitFor(startFutures);
        sleep(250);
        assertEquals("Identification=1", client1.getTypesSent());
        assertEquals("Identification=1", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=1", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        
        // remove this clientMachine from the central server
        // this simulates the case that the server does not record of a ClientMachine
        // no idea how this would happen in real life, but I guess it could
        var field = DistributedMessageServer.class.getDeclaredField("clientMachines");
        field.setAccessible(true);
        CopyOnWriteArrayList<?> clientMachines = (CopyOnWriteArrayList<?>) field.get(centralServer);
        clientMachines.clear();
        
        client1.createPublisher("hello", CloneableString.class);
        sleep(250);
        assertEquals("CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("Identification=1", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=1, RequestIdentification=1", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1, RequestIdentification=1", client1.getTypesReceived());
    }
    
    /**
     * In this test a client sends calls CreatePublisher but the write fails.
     * Verify that the client retries with exponential backoff up to a maximum of 3 times.
     * 
     * <p>The next part of the test has the server send a CreatePublisher to the client but the write fails.
     * Verify that the server retries with exponential backoff up to a maximum of 3 times.
     */
    @Test
    void testRetryWhenWriteFails() throws IOException, SecurityException, IllegalArgumentException  {
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var serverSocketTransformer = new TestSocketTransformer();
        var centralServer = createServer(serverSocketTransformer, Collections.emptyMap());
        startFutures.add(centralServer.start());
        
        var clientSocketTransformer = new TestSocketTransformer();
        var client1 = createClient(clientSocketTransformer,
                                   PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001);
        var client2 = createClient(new SocketTransformer(),
                                   PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002);
        startFutures.add(client1.start());
        startFutures.add(client2.start());
        
        waitFor(startFutures);
        sleep(250);
        assertEquals("Identification=1", client1.getTypesSent());
        assertEquals("Identification=1", client2.getTypesSent());
        assertEquals("Identification=2", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=2", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        assertEquals("ClientAccepted=1", client2.getTypesReceived());
    
        // client1 create publisher but we mock so that each call to send message to server fails
        // verify that there is one initial send and 3 retries but all fail
        System.out.println("Test client retry");
        clientSocketTransformer.setWriteFailCount();
        client1.createPublisher("hello", CloneableString.class);
        sleep(1000 + 2000 + 4000); // 1st retry after 1sec, 2nd retry after 2sec, 3rd and last retry after 4sec
        sleep(250); // wait for message to reach client
        assertEquals("Identification=1", client1.getTypesSent());
        assertEquals("Identification=1", client2.getTypesSent());
        assertEquals("Identification=2", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=2", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        assertEquals("ClientAccepted=1", client2.getTypesReceived());
        assertThat(client1.getSendFailures(), Matchers.contains("CreatePublisher: TestSocketTransformer write failure"));

        // client1 create publisher but this time the send to server succeeds
        client1.clearSendFailures();
        client1.createPublisher("world", CloneableString.class);
        sleep(250); // wait for message to reach client
        assertEquals("CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("Identification=1", client2.getTypesSent());
        assertEquals("CreatePublisher=1, Identification=2", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=2", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        assertEquals("ClientAccepted=1", client2.getTypesReceived());
        assertThat(client1.getSendFailures(), Matchers.empty());
        assertThat(centralServer.getSendFailures(), Matchers.empty());
        
        // client2 subscribes to the topic, which sends the createPublisher command down to them
        // we mock so that each call to send message to server fails
        // verify that there is one initial send and 3 retries but all fail on the server
        System.out.println("Test server retry");
        serverSocketTransformer.setWriteFailCount();
        client2.subscribe("world", "ClientTwoSubscriber", CloneableString.class, str -> { });
        sleep(1000 + 2000 + 4000); // 1st retry after 1sec, 2nd retry after 2sec, 3rd and last retry after 4sec
        sleep(250); // wait for message to reach client
        assertEquals("CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("AddSubscriber=1, Identification=1", client2.getTypesSent());
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=2", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=2", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        assertEquals("ClientAccepted=1", client2.getTypesReceived());
        assertThat(centralServer.getSendFailures(), Matchers.contains("CreatePublisher: TestSocketTransformer write failure"));
    }
    
    /**
     * Call the main function in a new process.
     * The main function starts a server and client, but does not shut them down.
     * The shutdown hook called when the process ends will shut down the client and server.
     * Without this, subsequent tests will not be able to reuse the same ports.
     */
    @Test
    void testShutdownHook() throws IOException, InterruptedException {
        var processBuilder = new ProcessBuilder(
                "java",
                "-cp",
                "target/classes:target/test-classes",
                "-ea",
                "-Djava.util.logging.config.file=target/test-classes/logging.properties",
                "myutils.pubsub.DistributedSocketPubSubTest")
                .inheritIO();
        var process = processBuilder.start();
        boolean finished = process.waitFor(5, TimeUnit.SECONDS);
        System.out.println("finished=" + finished);
        System.out.println("exitCode=" + process.exitValue());
    }

    public static void main(String[] args) throws IOException {
        DistributedMessageServer centralServer = new TestDistributedMessageServer(CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT, Collections.emptyMap());
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
        client1.start();
        
        var client2 = new TestDistributedSocketPubSub(1,
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
    
    @Test
    void codeCoverageForSubscriberEndpoint() throws NoSuchMethodException, SecurityException, InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Class<?> subscriberEndpointClass = Arrays.stream(DistributedMessageServer.class.getDeclaredClasses()).filter(clazz -> clazz.getSimpleName().equals("SubscriberEndpoint")).findFirst().get();
        Constructor<?> constructor = subscriberEndpointClass.getDeclaredConstructor(ClientMachineId.class, String.class, long.class);
        constructor.setAccessible(true);
        ClientMachineId clientMachineId1 = new ClientMachineId("client1");
        ClientMachineId clientMachineId2 = new ClientMachineId("client2");
        Object subscriberEndpoint1 = constructor.newInstance(clientMachineId1, "SubscriberName1", 500L);
        Object subscriberEndpoint1b = constructor.newInstance(clientMachineId1, "SubscriberName1", 1500L);
        Object subscriberEndpoint2 = constructor.newInstance(clientMachineId1, "SubscriberName2", 500L);
        Object subscriberEndpoint3 = constructor.newInstance(clientMachineId2, "SubscriberName3", 500L);

        assertNotEquals(subscriberEndpoint1, null);
        
        assertEquals(subscriberEndpoint1, subscriberEndpoint1b);
        assertEquals(subscriberEndpoint1.hashCode(), subscriberEndpoint1b.hashCode());
        
        assertNotEquals(subscriberEndpoint1, subscriberEndpoint2);
        assertNotEquals(subscriberEndpoint1, subscriberEndpoint3);
        assertNotEquals(subscriberEndpoint1.hashCode(), subscriberEndpoint2.hashCode());
        
        assertEquals("client1/SubscriberName1", subscriberEndpoint1.toString());
    }
    
    /**
     * Create a server and add it to list of objects to be shutdown at the end of the test function.
     */
    private TestDistributedMessageServer createServer(Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        var server = new TestDistributedMessageServer(CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT, mostRecentMessagesToKeep);
        addShutdown(server);
        return server;
    }
    
    /**
     * Create a server and add it to list of objects to be shutdown at the end of the test function.
     */
    private TestDistributedMessageServer createServer(SocketTransformer socketTransformer,
                                                      Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        var server = new TestDistributedMessageServer(socketTransformer, CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT, mostRecentMessagesToKeep);
        addShutdown(server);
        return server;
    }
    
    /**
     * Create a client and add it to list of objects to be shutdown at the end of the test function.
     */
    private TestDistributedSocketPubSub createClient(Supplier<Queue<Subscriber>> queueCreator,
                                                     SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                                     String machineId,
                                                     int localPort) throws IOException {
        var client = new TestDistributedSocketPubSub(1,
                                                     queueCreator,
                                                     subscriptionMessageExceptionHandler,
                                                     machineId,
                                                     "localhost",
                                                     localPort,
                                                     DistributedPubSubIntegrationTest.CENTRAL_SERVER_HOST,
                                                     DistributedPubSubIntegrationTest.CENTRAL_SERVER_PORT);
        addShutdown(client);
        return client;
    }
    
    /**
     * Create a client and add it to list of objects to be shutdown at the end of the test function.
     */
    private TestDistributedSocketPubSub createClient(SocketTransformer socketTransformer,
                                                     Supplier<Queue<Subscriber>> queueCreator,
                                                     SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                                     String machineId,
                                                     int port) throws IOException {
        var client = new TestDistributedSocketPubSub(socketTransformer,
                                                     1,
                                                     queueCreator,
                                                     subscriptionMessageExceptionHandler,
                                                     machineId,
                                                     "localhost",
                                                     port,
                                                     DistributedPubSubIntegrationTest.CENTRAL_SERVER_HOST,
                                                     DistributedPubSubIntegrationTest.CENTRAL_SERVER_PORT);
        addShutdown(client);
        return client;
    }
    
    private void addShutdown(Shutdowneable s) {
        shutdowns.add(s);
    }
    
    private static <T> void waitFor(List<CompletableFuture<T>> futures) {
        Instant startTime = Instant.now();
        TestUtil.toList(futures);
        Duration timeTaken = Duration.between(startTime, Instant.now());
        LOGGER.log(Level.INFO, "Time taken to start servers and clients: numObjects=" + futures.size() + ", timeTaken=" + timeTaken);
    }
}



class TestDistributedMessageServer extends DistributedMessageServer {
    private final List<String> validTypesReceived = Collections.synchronizedList(new ArrayList<>());
    private final List<String> typesSent = Collections.synchronizedList(new ArrayList<>());
    private final List<String> sendFailures = Collections.synchronizedList(new ArrayList<>());

    public TestDistributedMessageServer(String host,
                                        int port,
                                        Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        super(host, port, mostRecentMessagesToKeep);
    }

    public TestDistributedMessageServer(SocketTransformer socketTransformer,
                                        String host,
                                        int port,
                                        Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        super(socketTransformer, host, port, mostRecentMessagesToKeep);
    }

    /**
     * Set SO_REUSEADDR so that the unit tests can close and open channels immediately, not waiting for TIME_WAIT seconds.
     */
    @Override
    protected void onBeforeSocketBound(NetworkChannel channel) throws IOException {
        super.onBeforeSocketBound(channel);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    }
    
    @Override
    protected void onMessageSent(MessageBase message) {
        super.onMessageSent(message);
        typesSent.add(message.getClass().getSimpleName());
    }
    
    @Override
    protected void onValidMessageReceived(MessageBase message) {
        super.onValidMessageReceived(message);
        validTypesReceived.add(message.getClass().getSimpleName());
    }
    
    @Override
    protected void onSendMessageFailed(MessageBase message, Throwable e) {
        super.onSendMessageFailed(message, e);
        sendFailures.add(message.getClass().getSimpleName() + ": " + e.getMessage());
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
    
    Set<ClientMachine> getRemoteClients() {
        return getRemoteClientsStream().collect(Collectors.toSet());
    }
    
    List<String> getSendFailures() {
        return sendFailures;
    }
}


class TestDistributedSocketPubSub extends DistributedSocketPubSub {
    private final List<String> typesReceived = Collections.synchronizedList(new ArrayList<>());
    private final List<String> typesSent = Collections.synchronizedList(new ArrayList<>());
    private final List<String> sendFailures = Collections.synchronizedList(new ArrayList<>());
    private boolean enableTamperServerIndex;
    private Consumer<MessageBase> messageReceivedListener;
 
    public TestDistributedSocketPubSub(int numInMemoryHandlers,
                                       Supplier<Queue<Subscriber>> queueCreator,
                                       SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                       String machineId,
                                       String localServer,
                                       int localPort,
                                       String messageServerHost,
                                       int messageServerPort) throws IOException {
        super(new PubSubConstructorArgs(numInMemoryHandlers, queueCreator, subscriptionMessageExceptionHandler),
              machineId,
              localServer,
              localPort,
              messageServerHost,
              messageServerPort);
    }

    public TestDistributedSocketPubSub(SocketTransformer socketTransformer,
                                       int numInMemoryHandlers,
                                       Supplier<Queue<Subscriber>> queueCreator,
                                       SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                       String machineId,
                                       String localServer,
                                       int localPort,
                                       String messageServerHost,
                                       int messageServerPort) throws IOException {
        super(socketTransformer,
              new PubSubConstructorArgs(numInMemoryHandlers, queueCreator, subscriptionMessageExceptionHandler),
              machineId,
              localServer,
              localPort,
              messageServerHost,
              messageServerPort);
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
        super.onBeforeSocketBound(channel);
        channel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
    }
   
    @Override
    protected void onBeforeSendMessage(MessageBase message) {
        super.onBeforeSendMessage(message);
        if (enableTamperServerIndex && message instanceof RelayMessageBase) {
            RelayMessageBase relayMessage = (RelayMessageBase) message;
            relayMessage.setRelayFields(new RelayFields(System.currentTimeMillis(), ServerIndex.MIN_VALUE.increment(), new ClientMachineId("bogus")));
        }
    }
    
    @Override
    protected void onMessageSent(MessageBase message) {
        super.onMessageSent(message);
        typesSent.add(message.getClass().getSimpleName());
    }
    
    @Override
    protected void onMessageReceived(MessageBase message) {
        super.onMessageReceived(message);
        typesReceived.add(message.getClass().getSimpleName());
        if (messageReceivedListener != null) {
            messageReceivedListener.accept(message);
        }
    }

    @Override
    protected void onSendMessageFailed(MessageBase message, IOException e) {
        super.onSendMessageFailed(message, e);
        sendFailures.add(message.getClass().getSimpleName() + ": " + e.getMessage());
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
    
    void clearSendFailures() {
        sendFailures.clear();
    }
    
    List<String> getSendFailures() {
        return sendFailures;
    }
}


/**
 * Test socket transformer that ensures first N calls to read/write fail.
 */
class TestSocketTransformer extends SocketTransformer {
    private int writeFailCount;
    private int writeCount;
    private int readFailCount;
    private int readCount;
    
    void setWriteFailCount() {
        this.writeFailCount = 4;
        this.writeCount = 0;
    }
    
    void setReadFailCount() {
        this.readFailCount = 1;
        this.readCount = 0;
    }
    
    @Override
    public void writeMessageToSocket(MessageBase message, short maxLength, SocketChannel channel) throws IOException {
        if (++writeCount <= writeFailCount) {
            throw new IOException("TestSocketTransformer write failure");
        }
        super.writeMessageToSocket(message, maxLength, channel);
    }

    @Override
    public MessageBase readMessageFromSocket(SocketChannel channel) throws IOException {
        if (++readCount <= readFailCount) {
            throw new IOException("TestSocketTransformer read failure");
        }
        return super.readMessageFromSocket(channel);
    }

    @Override
    public CompletionStage<Void> writeMessageToSocketAsync(MessageBase message, short maxLength, AsynchronousSocketChannel channel) throws IOException {
        if (++writeCount <= writeFailCount) {
            return CompletableFuture.failedFuture(new IOException("TestSocketTransformer write failure"));
        }
        return super.writeMessageToSocketAsync(message, maxLength, channel);
    }

    @Override
    public CompletionStage<MessageBase> readMessageFromSocketAsync(AsynchronousSocketChannel channel) {
        if (++readCount <= readFailCount) {
            return CompletableFuture.failedFuture(new IOException("TestSocketTransformer read failure"));
        }
        return super.readMessageFromSocketAsync(channel);
    }    
}
