package org.sn.myutils.pubsub;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.sn.myutils.testutils.TestUtil.assertExceptionFromCallable;
import static org.sn.myutils.testutils.TestUtil.assertIncreasing;
import static org.sn.myutils.testutils.TestUtil.countElementsInListByType;
import static org.sn.myutils.testutils.TestUtil.sleep;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.Nullable;
import org.sn.myutils.pubsub.DistributedMessageServer.ClientMachine;
import org.sn.myutils.pubsub.DistributedSocketPubSub.DistributedPublisher;
import org.sn.myutils.pubsub.DistributedSocketPubSub.DistributedSubscriber;
import org.sn.myutils.pubsub.DistributedSocketPubSub.StartException;
import org.sn.myutils.pubsub.InMemoryPubSubIntegrationTest.CloneableString;
import org.sn.myutils.pubsub.MessageClasses.AddOrRemoveSubscriber;
import org.sn.myutils.pubsub.MessageClasses.CreatePublisher;
import org.sn.myutils.pubsub.MessageClasses.MessageBase;
import org.sn.myutils.pubsub.MessageClasses.PublishMessage;
import org.sn.myutils.pubsub.MessageClasses.RelayFields;
import org.sn.myutils.pubsub.MessageClasses.RelayMessageBase;
import org.sn.myutils.pubsub.MessageClasses.RelayTopicMessageBase;
import org.sn.myutils.pubsub.MessageClasses.TopicMessageBase;
import org.sn.myutils.pubsub.PubSub.Publisher;
import org.sn.myutils.pubsub.PubSub.Subscriber;
import org.sn.myutils.pubsub.PubSub.SubscriptionMessageExceptionHandler;
import org.sn.myutils.testutils.LogFailureToConsoleTestWatcher;
import org.sn.myutils.testutils.TestBase;
import org.sn.myutils.testutils.TestUtil;
import org.sn.myutils.util.ExceptionUtils;


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
 * - KeyToSocketAddressMapper
 * 
 * <p>In Eclipse run with the following VM arguments to get full logs:
 * <code>
-ea
-Djava.util.logging.config.file=../org.sn.myutils.testutils/target/classes/logging.properties
 * </code>
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
        sleep(250); // sleep to allow operating system to reclaim port (though this number 250 seems arbitrary)
    }
    
    //////////////////////////////////////////////////////////////////////

    private static final String CENTRAL_SERVER_HOST = "localhost";
    private static final int CENTRAL_SERVER_PORT = 2101;
    private static final int SECOND_CENTRAL_SERVER_PORT = 2102;

    /**
     * Test duplicate clients.
     * 2 clients subscribe with the same name.
     * Verify that the second one is ignored.
     */
    @Test
    void testDuplicateClient() throws IOException, InterruptedException {
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer = createServer(Collections.emptyMap());
        startFutures.add(centralServer.start());

        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001);

        startFutures.add(client1.startAsync());
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
        var startClient1bFuture = client1b.startAsync();
        sleep(250); // wait for server to receive message

        try {
            startClient1bFuture.get();
            fail();
        } catch (ExecutionException e) {
            var cause = e.getCause();
            StartException startException = (StartException) cause;
            assertEquals("[org.sn.myutils.pubsub.PubSubException: Duplicate channel: clientMachine=client1, otherClientChannel=/127.0.0.1:30001]",
                         startException.getExceptions().values().toString());
        }

        assertEquals("Identification=2", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=1, ClientRejected=1", centralServer.getTypesSent());
        assertThat(centralServer.getRemoteClients().stream().map(clientMachine -> clientMachine.getMachineId().toString()).collect(Collectors.toList()),
                   Matchers.contains("client1"));
    }

    /**
     * Basic test for publish + subscribe.
     * There is a central server, 2 clients, and one subscribers in each client.
     * 
     * @param orderString if "CreatePublisherFirst" then client1 creates publisher and waits for a response before creating the subscriber.
     *                    If "CreatePublisherAndAddSubscriberAtSameTime" then client1 creates publisher and subscriber at the same time.
     */
    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(strings = {"CreatePublisherFirst", "CreatePublisherAndAddSubscriberAtSameTime"})
    void basicTest(String orderString) throws IOException {
        boolean createPublisherFirst;
        if ("CreatePublisherFirst".equals(orderString)) {
            createPublisherFirst = true;
        } else if ("CreatePublisherAndAddSubscriberAtSameTime".equals(orderString)) {
            createPublisherFirst = false;
        } else {
            throw new UnsupportedOperationException(orderString);
        }
        
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
        startFutures.add(client1.startAsync());

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002
        );
        startFutures.add(client2.startAsync());

        waitFor(startFutures);
        assertEquals("Identification=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        assertEquals("Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1", client2.getTypesReceived());

        // client1 create publisher and subscribe, client2 subscribe
        // publish two messages while 
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        assertExceptionFromCallable(
            () -> client1.createPublisher("hello", CloneableString.class),
            IllegalStateException.class,
            "publisher already exists: topic=hello");
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        assertExceptionFromCallable(
            () -> client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a"))),
            IllegalStateException.class,
            "already subscribed: topic=hello, subscriberName=ClientTwoSubscriber_First");
        assertNull(client1.getPublisher("hello"));
        assertNull(client2.getPublisher("hello"));
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        if (createPublisherFirst) {
            sleep(250); // wait for publisher to be verified by server and activated on client1 and client2
            assertSame(publisher1, client1.getPublisher("hello"));
            assertNotNull(client2.getPublisher("hello"));
        } else {
            assertNull(client1.getPublisher("hello"));
            assertNull(client2.getPublisher("hello"));
        }
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250);
        assertSame(publisher1, client1.getPublisher("hello"));
        assertNotNull(client2.getPublisher("hello"));

        System.out.println("after publish 'one', 'two': actual=" + words);
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=2", client1.getTypesSent());
        assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=1, Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=2, SubscriberAdded=1", client2.getTypesReceived());
        if (createPublisherFirst) {
            // "one-s1", "two-s1" not printed as ClientOneSubscriber not present when publish was called
            assertThat(words, Matchers.contains("one-s2a", "two-s2a"));
        } else {
            assertThat(words, Matchers.contains("one-s1", "two-s1", "one-s2a", "two-s2a"));
            // "one-s1", "two-s1" is printed as as ClientOneSubscriber is added before the client receives the PublisherCreated message from the server
            // so the publisher is only made active once PublisherCreated and SubscriberAdded are received from the server
        }

        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, and in client2 the system will call publisher2.publish(), and similarly for client3
        words.clear();
        publisher1.publish(new CloneableString("three"));
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish 'three', 'four': actual=" + words);
        assertThat(words, Matchers.contains("three-s1", "four-s1", "three-s2a", "four-s2a"));
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=4", client1.getTypesSent());
        assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=1, Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=4, SubscriberAdded=1", client2.getTypesReceived());
        
        // add a new subscriber
        // this exercises the code path where SubscriberAdded makes the dormant subscriber active
        words.clear();
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // time to receive the SubscriberAdded confirmation
        publisher1.publish(new CloneableString("five"));
        publisher1.publish(new CloneableString("six"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish 'five', 'six': actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("five-s1", "six-s1", "five-s2a", "six-s2a", "five-s2b", "six-s2b"));
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=6", client1.getTypesSent());
        assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=6, SubscriberAdded=2", client2.getTypesReceived());
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
        startFutures.add(client1.startAsync());

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002
        );
        startFutures.add(client2.startAsync());

        var client3 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client3",
                                   30003
        );
        startFutures.add(client3.startAsync());

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
        assertNull(client1.getPublisher("hello"));
        assertNull(client2.getPublisher("hello"));
        assertNull(client3.getPublisher("hello"));
        sleep(250); // time to let subscribers be sent to server
        assertEquals(2, client1.getCountTypesSent()); // +1 = AddSubscriber
        assertEquals(2, client1.getCountTypesReceived()); // +1 = SubscriberAdded
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(3, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(2, client3.getCountTypesReceived());

        // create publisher on client1
        // this will get replicated to client2 and client3
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        assertNull(client1.getPublisher("hello"));
        sleep(250); // time to let publisher be propagated to all other clients
        sleep(250); // time to let each client sent a subscribe command to the server
        assertEquals(3, client1.getCountTypesSent()); // +1 sent CreatePublisher
        assertEquals(3, client1.getCountTypesReceived()); // +1 = PublisherCreated
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(4, client2.getCountTypesReceived()); // +1 received CreatePublisher
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(3, client3.getCountTypesReceived()); // +1 received CreatePublisher
        assertNotNull(client1.getPublisher("hello"));
        assertNotNull(client2.getPublisher("hello"));
        assertNotNull(client3.getPublisher("hello"));

        // publish two messages
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, and in client2 the system will call publisher2.publish(), and similarly for client3
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1", "one-s2a", "one-s2b", "two-s2a", "two-s2b", "one-s3", "two-s3"));
        assertEquals(5, client1.getCountTypesSent()); // +2
        assertEquals(3, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(6, client2.getCountTypesReceived()); // +2
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(5, client3.getCountTypesReceived()); // +2

        // explicitly shutdown client3 (normally this happens in the shutdown hook)
        // publish two messages
        // as client3 has been shutdown it will not receive the message
        client3.shutdown();
        sleep(250); // time to let client shutdown
        assertEquals(5, client1.getCountTypesSent()); // no changes
        assertEquals(3, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(6, client2.getCountTypesReceived());
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(5, client3.getCountTypesReceived());
        words.clear();
        publisher1.publish(new CloneableString("three"));
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after shutdown client3: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("three-s1", "four-s1", "three-s2a", "three-s2b", "four-s2a", "four-s2b"));
        assertEquals(7, client1.getCountTypesSent()); // +2
        assertEquals(3, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent());
        assertEquals(8, client2.getCountTypesReceived()); // +2
        assertEquals(2, client3.getCountTypesSent()); // unchanged
        assertEquals(5, client3.getCountTypesReceived());

        // unsubscribe one subscriber in client2
        // publish two messages
        // as client2 still has one subscriber in the "hello" topic, client2 still receives the message
        client2.unsubscribe(subscriber2b);
        sleep(250); // time to let central server know that one subscribe in client2 unsubscribed
        assertEquals(7, client1.getCountTypesSent());
        assertEquals(3, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent()); // +1 = RemoveSubscriber
        assertEquals(9, client2.getCountTypesReceived()); // +1 = SubscriberRemoved
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(5, client3.getCountTypesReceived());
        words.clear();
        publisher1.publish(new CloneableString("five"));
        publisher1.publish(new CloneableString("six"));
        sleep(250); // time to let messages be published to client2
        System.out.println("after unsubscribe2b: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("five-s1", "six-s1", "five-s2a", "six-s2a"));
        assertEquals(9, client1.getCountTypesSent()); // +2
        assertEquals(3, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent());
        assertEquals(11, client2.getCountTypesReceived()); // +2
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(5, client3.getCountTypesReceived());

        // unsubscribe the last subscriber in client2
        // publish two messages
        // as client3 has been shutdown and client2 has unsubscribed, only client1 will receive the message
        client2.unsubscribe(subscriber2a);
        sleep(250); // time to let central server know that client2 unsubscribed
        assertEquals(9, client1.getCountTypesSent());
        assertEquals(3, client1.getCountTypesReceived());
        assertEquals(5, client2.getCountTypesSent()); // +1 = RemoveSubscriber
        assertEquals(12, client2.getCountTypesReceived()); // +1 = SubscriberRemoved
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(5, client3.getCountTypesReceived());
        words.clear();
        publisher1.publish(new CloneableString("seven"));
        publisher1.publish(new CloneableString("eight"));
        sleep(250); // time to let messages be published to client2
        System.out.println("after unsubscribe2a: actual=" + words);
        assertThat(words, Matchers.containsInAnyOrder("seven-s1", "eight-s1"));
        assertEquals(11, client1.getCountTypesSent()); // +2 = PublishMessage
        assertEquals(3, client1.getCountTypesReceived());
        assertEquals(5, client2.getCountTypesSent());
        assertEquals(12, client2.getCountTypesReceived()); // unchanged, client2 does not receive messages as it is no longer subscribed
        assertEquals(2, client3.getCountTypesSent());
        assertEquals(5, client3.getCountTypesReceived());
    }
    
    /**
     * This test demonstrates how to use secret keys to allow only clients with the secret key to create a publisher and subscriber to a topic.
     * The implementor has to subclass DistributedMessageServer and DistributeSocketPubSub and override some functions.
     */
    @Test
    void testSecurity() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer = createServer(Collections.emptyMap());
        centralServer.setSecurityKey("hello123");
        startFutures.add(centralServer.start());
        sleep(250); // time to let the central server start

        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   30001);
        startFutures.add(client1.startAsync());

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002);
        startFutures.add(client2.startAsync());

        waitFor(startFutures);
        assertEquals("Identification=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        assertEquals("Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1", client2.getTypesReceived());

        // create publisher on client1 but the security key is not present or invalid so it fails
        DistributedPublisher publisher1 = (DistributedPublisher) client1.createPublisher("hello", CloneableString.class);
        sleep(250); // time to let publisher be propagated to all other clients
        assertTrue(publisher1.isInvalid());
        assertNull(client1.getPublisher("hello"));
        assertEquals("CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisherFailed=1", client1.getTypesReceived());
        assertEquals("Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1", client2.getTypesReceived());

        // create publisher on client1 successfully
        client1.enableSecurityKey("hello123");
        publisher1 = (DistributedPublisher) client1.createPublisher("hello", CloneableString.class);
        sleep(250); // time to let publisher be propagated to all other clients
        assertFalse(publisher1.isInvalid());
        assertSame(publisher1, client1.getPublisher("hello"));
        assertEquals("CreatePublisher=2, Identification=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisherFailed=1, PublisherCreated=1", client1.getTypesReceived());
        assertEquals("Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1", client2.getTypesReceived());

        // client2 subscribes but the security key is not present or invalid so it fails
        DistributedSubscriber subscriber2 = (DistributedSubscriber) client2.subscribe("hello", "ClientTwoSubscriber", CloneableString.class, str -> words.add(str.append("-s2")));
        sleep(250); // time to let subscriber be sent to server
        assertTrue(subscriber2.isInvalid());
        assertEquals("CreatePublisher=2, Identification=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisherFailed=1, PublisherCreated=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=1, Identification=1", client2.getTypesSent());
        assertEquals("AddSubscriberFailed=1, ClientAccepted=1", client2.getTypesReceived());

        // publish a message from client1
        // verify that client2 does not receive it
        publisher1.publish(new CloneableString("one"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish 'one': actual=" + words);
        assertThat(words, Matchers.empty());

        // client2 subscribes successfully
        // verify that message "one" is sent over
        client2.enableSecurityKey("hello123");
        subscriber2 = (DistributedSubscriber) client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        sleep(250); // time to let subscriber be sent to server
        System.out.println("after subscribe successful: actual=" + words);
        assertFalse(subscriber2.isInvalid());
        assertEquals("CreatePublisher=2, Identification=1, PublishMessage=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisherFailed=1, PublisherCreated=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("AddSubscriberFailed=1, ClientAccepted=1, CreatePublisher=1, SubscriberAdded=1", client2.getTypesReceived());
        assertThat(words, Matchers.empty());

        // publish a message from client1
        // verify that client2 receives it
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish 'two': actual=" + words);
        assertEquals("CreatePublisher=2, Identification=1, PublishMessage=2", client1.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisherFailed=1, PublisherCreated=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("AddSubscriberFailed=1, ClientAccepted=1, CreatePublisher=1, PublishMessage=1, SubscriberAdded=1", client2.getTypesReceived());
        assertThat(words, Matchers.contains("two-s2a"));

        // unsubscribe one subscriber in client2 but security key is missing or invalid
        client2.enableSecurityKey("wrong");
        client2.unsubscribe(subscriber2);
        sleep(250); // time to let central server know that one subscribe in client2 unsubscribed
        assertEquals("CreatePublisher=2, Identification=1, PublishMessage=2", client1.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisherFailed=1, PublisherCreated=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1, RemoveSubscriber=1", client2.getTypesSent());
        assertEquals("AddSubscriberFailed=1, ClientAccepted=1, CreatePublisher=1, PublishMessage=1, RemoveSubscriberFailed=1, SubscriberAdded=1", client2.getTypesReceived());

        // publish a message from client1
        // verify that client2 receives it
        publisher1.publish(new CloneableString("three"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish 'three': actual=" + words);
        assertEquals("CreatePublisher=2, Identification=1, PublishMessage=3", client1.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisherFailed=1, PublisherCreated=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1, RemoveSubscriber=1", client2.getTypesSent());
        assertEquals("AddSubscriberFailed=1, ClientAccepted=1, CreatePublisher=1, PublishMessage=2, RemoveSubscriberFailed=1, SubscriberAdded=1", client2.getTypesReceived());
        assertThat(words, Matchers.contains("two-s2a", "three-s2a"));

        // unsubscribe one subscriber in client2 and this time it is a success
        client2.enableSecurityKey("hello123");
        client2.unsubscribe(subscriber2);
        sleep(250); // time to let central server know that one subscribe in client2 unsubscribed
        assertEquals("CreatePublisher=2, Identification=1, PublishMessage=3", client1.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisherFailed=1, PublisherCreated=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1, RemoveSubscriber=2", client2.getTypesSent());
        assertEquals("AddSubscriberFailed=1, ClientAccepted=1, CreatePublisher=1, PublishMessage=2, RemoveSubscriberFailed=1, SubscriberAdded=1, SubscriberRemoved=1", client2.getTypesReceived());

        // publish a message from client1
        // verify that client2 does not receive it
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after publish 'four': actual=" + words);
        assertThat(words, Matchers.contains("two-s2a", "three-s2a"));
    }

    /**
     * Test a client coming online after messages have been published.
     * The new client does not receive the messages which were published earlier.
     * However, the new client can call download to retrieve the old messages.
     */
    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(strings = {"ByServerId", "ByClientTimestamp"})
    void testDownloadMessages(String method) throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        
        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 2, RetentionPriority.MEDIUM, 3));
        waitFor(Collections.singletonList(centralServer.start()));
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        AtomicReference<ServerIndex> serverIndexBanana = new AtomicReference<>(ServerIndex.MAX_VALUE); // index of first banana
        AtomicReference<ServerIndex> serverIndexCarrot = new AtomicReference<>(ServerIndex.MIN_VALUE); // index of last carrot
        centralServer.setMessageSentListener(message -> {
            if (message instanceof PublishMessage) {
                PublishMessage publishMessage = (PublishMessage) message;
                if (publishMessage.getMessage().toString().equals("CloneableString:banana")) {
                    // the idea is to capture the server id of the first banana so that we can test download within range
                    serverIndexBanana.updateAndGet(current -> TestUtil.min(current, publishMessage.getRelayFields().getServerIndex()));
                }
                if (publishMessage.getMessage().toString().equals("CloneableString:carrot")) {
                    // the idea is to capture the server id of the last carrot so that we can test download within range
                    serverIndexCarrot.updateAndGet(current -> TestUtil.max(current, publishMessage.getRelayFields().getServerIndex()));
                }
            }
        });
        waitFor(Collections.singletonList(client1.startAsync()));
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
        assertEquals(5, client1.getCountTypesReceived()); // ClientAccepted, PublisherCreated * 2, SubscriberAdded * 2
        
        // publish messages with different retentions
        // the server will remember the last 3 messages with low retention, and the last 2 messages with high retention
        // the subscriber running on client1 will pick it up immediately
        // the message will get replicated to all other subscribers, but client2 is not yet running
        helloPublisher1.publish(new CloneableString("ImportantOne"), RetentionPriority.HIGH);
        helloPublisher1.publish(new CloneableString("ImportantTwo"), RetentionPriority.HIGH);
        helloPublisher1.publish(new CloneableString("ImportantThree"), RetentionPriority.HIGH);
        helloPublisher1.publish(new CloneableString("apple"), RetentionPriority.MEDIUM);
        worldPublisher1.publish(new CloneableString("ImportantOne"), RetentionPriority.HIGH);
        worldPublisher1.publish(new CloneableString("ImportantTwo"), RetentionPriority.HIGH);
        worldPublisher1.publish(new CloneableString("ImportantThree"), RetentionPriority.HIGH);
        worldPublisher1.publish(new CloneableString("apple"), RetentionPriority.MEDIUM);
        sleep(250);
        long timeJustBeforeSendBanana = System.currentTimeMillis();
        helloPublisher1.publish(new CloneableString("banana"), RetentionPriority.MEDIUM);
        helloPublisher1.publish(new CloneableString("carrot"));
        worldPublisher1.publish(new CloneableString("banana"), RetentionPriority.MEDIUM);
        worldPublisher1.publish(new CloneableString("carrot"));
        sleep(50);
        long timeJustBeforeSendDragonfruit = System.currentTimeMillis();
        sleep(250);
        helloPublisher1.publish(new CloneableString("dragonfruit"), RetentionPriority.MEDIUM);
        worldPublisher1.publish(new CloneableString("dragonfruit"), RetentionPriority.MEDIUM);
        sleep(250); // time to let messages be published to client2
        System.out.println("before client2 exists: actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantOne-s1-hello", "ImportantTwo-s1-hello", "ImportantThree-s1-hello", "apple-s1-hello",
                                     "ImportantOne-s1-world", "ImportantTwo-s1-world", "ImportantThree-s1-world", "apple-s1-world",
                                     "banana-s1-hello", "carrot-s1-hello",
                                     "banana-s1-world", "carrot-s1-world",
                                     "dragonfruit-s1-hello",
                                     "dragonfruit-s1-world"));
        assertEquals(19, client1.getCountTypesSent()); // +14 = PublishMessage
        assertEquals(5, client1.getCountTypesReceived());

        words.clear();
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        assertNull(client2.getPublisher("hello"));
        waitFor(Collections.singletonList(client2.startAsync()));
        sleep(250); // time to let client2 start
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(5, client1.getCountTypesReceived());
        assertEquals(1, client2.getCountTypesSent());
        assertEquals("ClientAccepted=1", client2.getTypesReceived()); // does not receive as client2 not subscribed to topic "hello"
        
        client2.subscribe("hello", "ClientTwo_HelloSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a-hello")));
        client2.subscribe("hello", "ClientTwo_HelloSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b-hello")));
        client2.subscribe("world", "ClientTwo_WorldSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2-world")));
        sleep(250); // time to let client2 subscribe and for server to send down the publisher
        assertNotNull(client2.getPublisher("hello"));
        assertNotNull(client2.getPublisher("world"));
        assertThat(words, Matchers.empty());
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(5, client1.getCountTypesReceived());
        assertEquals(4, client2.getCountTypesSent()); // +3 = add subscriber
        assertEquals("ClientAccepted=1, CreatePublisher=2, SubscriberAdded=3", client2.getTypesReceived());

        abstract class Download {
            abstract void downloadFullRange(List<String> topics);

            abstract void downloadWithinRange(List<String> topics, Object startInclusive, Object endInclusive);
        }

        class DownloadByServerId extends Download {
            @Override
            void downloadFullRange(List<String> topics) {
                client2.downloadByServerId(topics, ServerIndex.MIN_VALUE, ServerIndex.MAX_VALUE);
            }

            @Override
            void downloadWithinRange(List<String> topics, Object startInclusive, Object endInclusive) {
                client2.downloadByServerId(topics, (ServerIndex) startInclusive, (ServerIndex) endInclusive);
            }
        }

        class DownloadByClientTimestamp extends Download {
            @Override
            void downloadFullRange(List<String> topics) {
                client2.downloadByClientTimestamp(topics, 0, Long.MAX_VALUE);
            }

            @Override
            void downloadWithinRange(List<String> topics, Object startInclusive, Object endInclusive) {
                client2.downloadByClientTimestamp(topics, (long) startInclusive, (long) endInclusive);
            }
        }

        Download download;
        if (method.equals("ByServerId")) {
            download = new DownloadByServerId();
        } else if (method.equals("ByClientTimestamp")) {
            download = new DownloadByClientTimestamp();
        } else {
            throw new UnsupportedOperationException(method);
        }

        download.downloadFullRange(List.of("hello", "world"));
        sleep(250); // time to let messages be sent to client2
        System.out.println("after client2 downloads (unsorted): actual=" + words);
        // because SubscriberAdded commands are received in a random order, subscriber1 may be added after subscriber2
        // so sort elements 0 and 1, elements 2 and 3, etc
        // this still proves that ImportantTwo sent first, then ImportantThree, then banana, then carrot, then dragonfruit
        words.subList(0, 2).sort(Comparator.naturalOrder());
        words.subList(2, 4).sort(Comparator.naturalOrder());
        words.subList(6, 8).sort(Comparator.naturalOrder());
        words.subList(8, 10).sort(Comparator.naturalOrder());
        words.subList(12, 14).sort(Comparator.naturalOrder());
        System.out.println("after client2 downloads (sorted  ): actual=" + words);
        assertThat(words,
                Matchers.contains("ImportantTwo-s2a-hello", "ImportantTwo-s2b-hello", "ImportantThree-s2a-hello", "ImportantThree-s2b-hello",
                                  "ImportantTwo-s2-world", "ImportantThree-s2-world",
                                  "banana-s2a-hello", "banana-s2b-hello", "carrot-s2a-hello","carrot-s2b-hello",
                                  "banana-s2-world", "carrot-s2-world",
                                  "dragonfruit-s2a-hello", "dragonfruit-s2b-hello",
                                  "dragonfruit-s2-world"));
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(5, client1.getCountTypesReceived());
        assertEquals(5, client2.getCountTypesSent()); // +1 = DownloadPublishedMessages
        assertEquals("ClientAccepted=1, CreatePublisher=2, PublishMessage=10, SubscriberAdded=3", client2.getTypesReceived()); // +10 = PublishMessage

        // verify that messages can be downloaded a second time
        words.clear();
        download.downloadFullRange(List.of("hello"));
        sleep(250); // time to let messages be sent to client2
        System.out.println("after client2 downloads a second time (unsorted): actual=" + words);
        words.subList(0, 2).sort(Comparator.naturalOrder()); // see above for comment why we sort
        words.subList(2, 4).sort(Comparator.naturalOrder());
        words.subList(4, 6).sort(Comparator.naturalOrder());
        words.subList(6, 8).sort(Comparator.naturalOrder());
        words.subList(8, 10).sort(Comparator.naturalOrder());
        System.out.println("after client2 downloads a second time (sorted  ): actual=" + words);
        assertThat(words,
                   Matchers.contains("ImportantTwo-s2a-hello", "ImportantTwo-s2b-hello", "ImportantThree-s2a-hello", "ImportantThree-s2b-hello",
                                     "banana-s2a-hello", "banana-s2b-hello", "carrot-s2a-hello", "carrot-s2b-hello", "dragonfruit-s2a-hello", "dragonfruit-s2b-hello"));
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(5, client1.getCountTypesReceived());
        assertEquals(6, client2.getCountTypesSent()); // +1 = DownloadPublishedMessages
        assertEquals("ClientAccepted=1, CreatePublisher=2, PublishMessage=15, SubscriberAdded=3", client2.getTypesReceived()); // +5 = PublishMessage

        // verify download within a range
        words.clear();
        Object startInclusive = download instanceof DownloadByServerId ? serverIndexBanana.get() : timeJustBeforeSendBanana;
        Object endInclusive = download instanceof DownloadByServerId ? serverIndexCarrot.get() : timeJustBeforeSendDragonfruit;
        download.downloadWithinRange(List.of("world"), startInclusive, endInclusive);
        sleep(250); // time to let messages be sent to client2
        System.out.println("after client2 downloads a second time within limited time range: actual=" + words);
        assertThat(words,
                   Matchers.contains("banana-s2-world", "carrot-s2-world"));
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(5, client1.getCountTypesReceived());
        assertEquals(7, client2.getCountTypesSent()); // +1 = DownloadPublishedMessages
        assertEquals("ClientAccepted=1, CreatePublisher=2, PublishMessage=17, SubscriberAdded=3", client2.getTypesReceived()); // +2 = PublishMessage

        // verify error if download a topic that does not exist
        words.clear();
        download.downloadFullRange(List.of("NewTopic"));
        sleep(250); // time to let messages be sent to client2
        System.out.println("download topic that does not exist: actual=" + words);
        assertThat(words, Matchers.empty());
        assertEquals(19, client1.getCountTypesSent());
        assertEquals(5, client1.getCountTypesReceived());
        assertEquals(8, client2.getCountTypesSent()); // +1 = DownloadPublishedMessages

        // verify error if download a topic to which we are not subscribed
        client1.createPublisher("NewTopic", CloneableString.class);
        sleep(250); // time to let messages be sent to client2
        words.clear();
        download.downloadFullRange(List.of("NewTopic"));
        sleep(250); // time to let messages be sent to client2
        System.out.println("download messages for a topic to which we are not subscribed: actual=" + words);
        assertThat(words, Matchers.empty());
        assertEquals(20, client1.getCountTypesSent());
        assertEquals(6, client1.getCountTypesReceived()); // +1 = PublisherCreated
        assertEquals(9, client2.getCountTypesSent()); // +1 = DownloadPublishedMessages
    }
    
    /**
     * In this test we shutdown the client and server.
     * Verify that restart fails.
     */
    @Test
    void testRestartFails() throws IOException, InterruptedException, ExecutionException {
        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        Future<Void> startCentralServerFuture = centralServer.start();
        //assertFalse(startCentralServerFuture.isDone()); // may be true
        startCentralServerFuture.get(); // assert no exception
        assertTrue(startCentralServerFuture.isDone());

        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        Future<Void> startClient1Future = client1.startAsync();
        //assertFalse(startClient1Future.isDone());// may be true
        startClient1Future.get(); // assert no exception
        assertTrue(startClient1Future.isDone());
        assertEquals(1, client1.getCountTypesSent()); // Identification
        assertEquals(1, client1.getCountTypesReceived()); // ClientAccepted
        
        // start client again
        Future<Void> startCentralServerFutureAgain = centralServer.start();
        assertFalse(startCentralServerFutureAgain.isDone());
        sleep(250); // time to let server start        
        assertTrue(startCentralServerFutureAgain.isDone());
        assertExceptionFromCallable(startCentralServerFutureAgain::get, ExecutionException.class, "java.nio.channels.AlreadyBoundException");

        // start another server on same host:port
        var centralServerDuplicateAddress = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        Future<Void> startServerDuplicateAddressFuture = centralServerDuplicateAddress.start();
        sleep(250); // time to let server start        
        assertTrue(startServerDuplicateAddressFuture.isDone());
        assertExceptionFromCallable(startServerDuplicateAddressFuture::get, ExecutionException.class, "java.net.BindException: Address already in use");
        
        // start client again
        Future<Void> startClient1FutureAgain = client1.startAsync();
        sleep(250); // time to let client start        
        assertTrue(startClient1FutureAgain.isDone());
        try {
            startClient1FutureAgain.get();
            fail();
        } catch (ExecutionException e) {
            var cause = e.getCause();
            StartException startException = (StartException) cause;
            assertEquals("[java.nio.channels.AlreadyConnectedException]",
                         startException.getExceptions().values().toString());
        }

        // connect another client on same host:port
        var clientDuplicateAddress = createClient(PubSub.defaultQueueCreator(),
                                                  PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                  "client2",
                                                  31001
        );
        Future<Void> startClientDuplicateAddressFuture = clientDuplicateAddress.startAsync();
        sleep(250); // time to let client start        
        assertTrue(startClientDuplicateAddressFuture.isDone());
        try {
            startClientDuplicateAddressFuture.get();
            fail();
        } catch (ExecutionException e) {
            var cause = e.getCause();
            StartException startException = (StartException) cause;
            assertEquals("[java.net.BindException: Cannot assign requested address]",
                         startException.getExceptions().values().toString());
        }
        
        client1.shutdown();
        sleep(250); // time to let client shutdown
        assertExceptionFromCallable(client1::startAsync, RejectedExecutionException.class);

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
        CompletableFuture<Void> client1Started = client1.startAsync();
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // time to let client start
        assertNull(client1.getPublisher("hello"));
        assertEquals(0, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        client2.startAsync();
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // time to let client2 start
        assertNull(client2.getPublisher("hello"));
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
        assertThat(words, Matchers.empty()); // "one-s1", "two-s1" not present because publisher and ClientOneSubscriber not active as server is down
        assertEquals(0, client1.getCountTypesSent());
        assertEquals(0, client1.getCountTypesReceived());
        assertEquals(0, client2.getCountTypesSent());
        assertEquals(0, client2.getCountTypesReceived());
        
        assertFalse(client1Started.isDone());

        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        centralServer.start();
        sleep(250); // time to let the central server start
        sleep(2000); // clients try to connect to the server every 1sec, 2sec, 4sec, 8sec, 8sec so more time to let the clients connect to the central server
        sleep(250); // time to let central server send messages down to the clients

        assertTrue(client1Started.isDone());
        System.out.println("after central server started: actual=" + words);
        assertSame(publisher1, client1.getPublisher("hello"));
        assertEquals(5, client1.getCountTypesSent()); // Identification, CreatePublisher, AddSubscriber, 2 publish messages
        assertEquals(3, client1.getCountTypesReceived()); // ClientAccepted, PublisherCreated, SubscriberAdded
        assertEquals(3, client2.getCountTypesSent()); // Identification, AddSubscriber, AddSubscriber
        assertEquals(6, client2.getCountTypesReceived()); // ClientAccepted, CreatePublisher, PublishMessage * 2, SubscriberAdded * 2
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1", "one-s2a", "one-s2b", "two-s2a", "two-s2b"));
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
        waitFor(Collections.singletonList(client1.startAsync()));
        assertEquals(1, client1.getCountTypesSent()); // Identification
        assertEquals(1, client1.getCountTypesReceived()); // ClientAccepted
        
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        waitFor(Collections.singletonList(client2.startAsync()));
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
        sleep(250); // time to let publishers and subscribers be confirmed
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let messages from client1 reach client2
        
        System.out.println("after second central server started: actual=" + words);
        assertEquals(6, client1.getCountTypesSent()); // +5 = Identification, CreatePublisher, AddSubscriber, PublishMessage, PublishMessage
        assertEquals("ClientAccepted=2, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived()); // both ClientAccepted
        assertEquals(4, client2.getCountTypesSent()); // +3 = Identification, AddSubscriber, AddSubscriber
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=2, SubscriberAdded=2", client2.getTypesReceived());
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
        waitFor(Collections.singletonList(client1.startAsync()));
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        sleep(250); // wait for CreatePublisher and AddSubscriber commands to be sent
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        waitFor(Collections.singletonList(client2.startAsync()));
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // wait for CreatePublisher and AddSubscriber commands to be sent
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisher=1, SubscriberAdded=2", client2.getTypesReceived());
        assertNotNull(client2.getPublisher("hello"));
        
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
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent()); // unchanged
        assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived()); // unchanged
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent()); // unchanged
        assertEquals("ClientAccepted=1, CreatePublisher=1, SubscriberAdded=2", client2.getTypesReceived()); // unchanged
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1"));
        
        words.clear();
        
        var centralServer2 = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        waitFor(Collections.singletonList(centralServer2.start()));
        sleep(1000); // time to let client1 connect to server as part of exponential backoff
        sleep(250); // more time to let SubscriberAdded and PublisherCreated be sent to each client
        sleep(250); // more time to let messages be downloaded to client2
        
        // client1 will send the message to centralServer2 which relays it to the other clients
        System.out.println("after central server restarted: actual=" + words);
        assertEquals("AddSubscriber=2, CreatePublisher=2, Identification=2, PublishMessage=2", client1.getTypesSent()); // +5 = Identification, CreatePublisher, AddSubscriber, PublishMessage, PublishMessage
        assertEquals("ClientAccepted=2, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=4, Identification=2", client2.getTypesSent());
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=2, SubscriberAdded=2", client2.getTypesReceived());
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
            startFutures.add(client1.startAsync());
            
            var client2 = createClient(PubSub.defaultQueueCreator(),
                                       PubSub.defaultSubscriptionMessageExceptionHandler(),
                                       "client2",
                                       31002
            );
            startFutures.add(client2.startAsync());

            waitFor(startFutures);

            Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
            client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
            client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
            client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
            sleep(250); // time to let server and clients start, and messages to be relayed
            System.out.println("after server and clients started: actual=" + words);
            assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent());
            assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
            assertEquals("ClientAccepted=1, CreatePublisher=1, SubscriberAdded=2", client2.getTypesReceived());
            assertThat(words, Matchers.empty());
            
            words.clear();
            publisher1.publish(new CloneableString("one"));
            publisher1.publish(new CloneableString("two"));
            sleep(250); // time to let server and clients start, and messages to be relayed
            System.out.println("after two messages (one, two) published: actual=" + words);
            assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=2", client1.getTypesSent());
            assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
            assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=2, SubscriberAdded=2", client2.getTypesReceived());
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
            assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent()); // unchanged
            assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=2, SubscriberAdded=2", client2.getTypesReceived()); // unchanged as client2 is shutdown
            assertThat(words, Matchers.contains("three-s1", "four-s1"));

            client1.shutdown();
            sleep(250); // time to let client shutdown
        }
        
        assertEquals("AddSubscriber=3, CreatePublisher=1, Identification=2, PublishMessage=4", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=2, PublisherCreated=1, SubscriberAdded=3", centralServer.getTypesSent());
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
            startFutures.add(client1.startAsync());
            
            var client2 = createClient(PubSub.defaultQueueCreator(),
                                       PubSub.defaultSubscriptionMessageExceptionHandler(),
                                       "client2",
                                       31002
            );
            startFutures.add(client2.startAsync());

            waitFor(startFutures);

            Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
            client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
            client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
            client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
            sleep(250); // time to let server and clients start, and messages to be relayed to server
            System.out.println("after clients restarted: actual=" + words);
            assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent());
            assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
            assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=2, SubscriberAdded=2", client2.getTypesReceived()); // differences: PublishMessage +2 because "three" and "four" sent to client
            assertThat(words, Matchers.containsInAnyOrder("three-s2a", "three-s2b", "four-s2a", "four-s2b")); // differences: handle 2 new messages
            
            words.clear();
            publisher1.publish(new CloneableString("five")); // difference: above we publish "three"
            publisher1.publish(new CloneableString("six")); // difference: above we publish "four"
            sleep(250); // time to let server and clients start, and messages to be relayed
            System.out.println("after clients restarted two messages (five, six) published: actual=" + words);
            assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=2", client1.getTypesSent());
            assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
            assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
            assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=4, SubscriberAdded=2", client2.getTypesReceived()); // PublishMessage +2
            assertThat(words, Matchers.containsInAnyOrder("five-s1", "six-s1", "five-s2a", "five-s2b", "six-s2a", "six-s2b"));
            
            assertThat(centralServer.getRemoteClients().stream().map(clientMachine -> clientMachine.getMachineId().toString()).collect(Collectors.toSet()),
                       Matchers.containsInAnyOrder("client1", "client2"));

            // END: this block of code almost the same as in both blocks
        }
        
        assertEquals("AddSubscriber=6, CreatePublisher=2, Identification=4, PublishMessage=6", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=4, CreatePublisher=2, PublishMessage=6, PublisherCreated=2, SubscriberAdded=6", centralServer.getTypesSent());
        
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
        startFutures.add(client1.startAsync());
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
            return true;
        });
        startFutures.add(client2.startAsync());
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));

        waitFor(startFutures);
        sleep(250); // wait for CreatePublisher and AddSubscriber messages to get sent
        System.out.println("after server and clients started: actual=" + words);
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisher=1, SubscriberAdded=2", client2.getTypesReceived());
        assertEquals(0, serverIndexesOfPublishMessageReceivedInClient2.size()); // because no messages published yet
        
        publisher1.publish(new CloneableString("one"));
        publisher1.publish(new CloneableString("two"));
        sleep(250); // time to let server and clients start, and messages to be relayed
        System.out.println("after first two messages published: actual=" + words);
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=1, PublishMessage=2", client1.getTypesSent());
        assertEquals("ClientAccepted=1, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=2, Identification=1", client2.getTypesSent());
        assertEquals("ClientAccepted=1, CreatePublisher=1, PublishMessage=2, SubscriberAdded=2", client2.getTypesReceived());
        assertEquals(2, serverIndexesOfPublishMessageReceivedInClient2.size());
        
        centralServer.shutdown();
        sleep(250); // time to let central server shutdown

        var centralServer2 = createServer(Map.of(RetentionPriority.HIGH, 1, RetentionPriority.MEDIUM, 3));
        waitFor(Collections.singletonList(centralServer2.start()));
        sleep(1000); // time to let client1 connect to server as part of exponential backoff
        
        System.out.println("after new server started: actual=" + words);
        assertEquals("AddSubscriber=2, CreatePublisher=2, Identification=2, PublishMessage=2", client1.getTypesSent());
        assertEquals("ClientAccepted=2, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=4, Identification=2", client2.getTypesSent());
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=2, SubscriberAdded=2", client2.getTypesReceived());
        assertEquals(2, serverIndexesOfPublishMessageReceivedInClient2.size());
        
        // publish two more messages
        publisher1.publish(new CloneableString("three"));
        publisher1.publish(new CloneableString("four"));
        sleep(250); // time to let messages be published to remote clients
        System.out.println("after first two messages published: actual=" + words);
        assertEquals("AddSubscriber=2, CreatePublisher=2, Identification=2, PublishMessage=4", client1.getTypesSent());
        assertEquals("ClientAccepted=2, PublisherCreated=1, SubscriberAdded=1", client1.getTypesReceived());
        assertEquals("AddSubscriber=4, Identification=2", client2.getTypesSent());
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=4, SubscriberAdded=2", client2.getTypesReceived());
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
        startFutures.add(client1.startAsync());
        Publisher publisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1")));
        waitFor(startFutures);
        sleep(250); // wait for CreatePublisher and AddSubscriber commands to be sent
        assertNotNull(client1.getPublisher("hello"));
        assertEquals(3, client1.getCountTypesSent()); // Identification, CreatePublisher, AddSubscriber
        assertEquals(3, client1.getCountTypesReceived()); // ClientAccepted, PublisherCreated, SubscriberAdded

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        waitFor(Collections.singletonList(client2.startAsync()));
        client2.subscribe("hello", "ClientTwoSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a")));
        client2.subscribe("hello", "ClientTwoSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b")));
        sleep(250); // wait for CreatePublisher and AddSubscriber commands to be sent
        assertEquals(3, client1.getCountTypesSent());
        assertEquals(3, client1.getCountTypesReceived());
        assertEquals(3, client2.getCountTypesSent()); // Identification, AddSubscriber, AddSubscriber
        assertEquals(4, client2.getCountTypesReceived()); // ClientAccepted, CreatePublisher, SubscriberAdded * 2
        assertNotNull(client2.getPublisher("hello"));
        
        assertEquals(6, centralServer.getCountValidTypesReceived());
        assertEquals(7, centralServer.getCountTypesSent());

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
        assertEquals(5, client1.getCountTypesReceived()); // +2 = InvalidRelayMessage * 2
        assertEquals(3, client2.getCountTypesSent()); // unchanged
        assertEquals(4, client2.getCountTypesReceived()); // unchanged
        assertThat(words, Matchers.containsInAnyOrder("one-s1", "two-s1"));
        
        assertEquals(6, centralServer.getCountValidTypesReceived()); // +0 = invalid message not valid
        assertEquals(9, centralServer.getCountTypesSent()); // +2 = InvalidRelayMessage, InvalidRelayMessage
    }
    
    /**
     * In this test the client receives the same message twice.
     * The client has been modified to save the ids of the message it has received.
     * Ensure that the client ignores the second message.
     */
    @Test
    void testClientIgnoresMessagesAlreadyProcessed() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();
        
        var centralServer = createServer(Map.of(RetentionPriority.HIGH, 2, RetentionPriority.MEDIUM, 3));
        startFutures.add(centralServer.start());
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   31001
        );
        startFutures.add(client1.startAsync());

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   31002
        );
        startFutures.add(client2.startAsync());
        
        class SaveMessageIdsListener {
            private static final int MAX_IDS_TO_SAVE = 5;
            private final Deque<ServerIndex> messagesIdsProcessed = new ArrayDeque<>();
            private boolean allowIfDownload;
            
            void setAllowIfDownload(boolean allowIfDownload) {
                this.allowIfDownload = allowIfDownload;
            }
            
            public boolean handle(MessageBase message) {
                if (message instanceof RelayTopicMessageBase) {
                    return handleRelayTopic((RelayTopicMessageBase) message);
                } else {
                    return true;
                }
            }

            private boolean handleRelayTopic(RelayTopicMessageBase message) {
                RelayTopicMessageBase relay = (RelayTopicMessageBase) message;
                ServerIndex serverIndex = relay.getRelayFields().getServerIndex();
                if (messagesIdsProcessed.stream()
                                        .filter(messageId -> messageId.equals(serverIndex))
                                        .findFirst()
                                        .isPresent()) {
                    if (allowIfDownload && message.isDownload()) {
                        return true;
                    } else {
                        LOGGER.log(Level.WARNING,
                                   "Duplicate relay message received: clientMachine={0}, messageClass={1}, serverIndex={2}",
                                   client2.getMachineId(), message.getClass().getSimpleName(), serverIndex);
                        return false;
                    }
                } else {
                    if (messagesIdsProcessed.size() >= MAX_IDS_TO_SAVE) {
                        messagesIdsProcessed.removeFirst();
                    }
                    messagesIdsProcessed.add(relay.getRelayFields().getServerIndex());
                    return true;
                }
            }
        }
        
        var saveMessageIdsListener = new SaveMessageIdsListener();
        client2.setMessageReceivedListener(saveMessageIdsListener::handle);

        waitFor(startFutures);
        
        Publisher helloPublisher1 = client1.createPublisher("hello", CloneableString.class);
        client1.subscribe("hello", "ClientOneSubscriber", CloneableString.class, str -> words.add(str.append("-s1-hello")));
        client2.subscribe("hello", "ClientTwo_HelloSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a-hello")));
        client2.subscribe("hello", "ClientTwo_HelloSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b-hello")));

        // publish messages
        helloPublisher1.publish(new CloneableString("apple"));
        helloPublisher1.publish(new CloneableString("banana"));

        // because SubscriberAdded commands are received in a random order, subscriber1 may be added after subscriber2
        // so sort elements 0 and 1, elements 2 and 3, etc
        // this still proves that ImportantTwo sent first, then ImportantThree, then banana, then carrot, then dragonfruit
        sleep(250); // time to let messages be sent to client2
        words.subList(0, 2).sort(Comparator.naturalOrder());
        words.subList(2, 4).sort(Comparator.naturalOrder());
        System.out.println("words (unsorted): actual=" + words);
        assertThat(words, Matchers.contains("apple-s2a-hello", "apple-s2b-hello", "banana-s2a-hello", "banana-s2b-hello"));

        // download all messages and verify that messages that have already been downloaded are ignored
        words.clear();
        client2.downloadByServerId(List.of("hello"), ServerIndex.MIN_VALUE, ServerIndex.MAX_VALUE);
        sleep(250); // time to let messages be sent to client2
        System.out.println("words (unsorted) after download 1: actual=" + words);
        assertThat(words, Matchers.empty());

        // download all messages and verify that messages that have already been downloaded are allowed as they arise from download
        saveMessageIdsListener.setAllowIfDownload(true);
        words.clear();
        client2.downloadByServerId(List.of("hello"), ServerIndex.MIN_VALUE, ServerIndex.MAX_VALUE);
        sleep(250); // time to let messages be sent to client2
        words.subList(0, 2).sort(Comparator.naturalOrder());
        words.subList(2, 4).sort(Comparator.naturalOrder());
        System.out.println("words (unsorted) after download 2: actual=" + words);
        assertThat(words, Matchers.contains("apple-s2a-hello", "apple-s2b-hello", "banana-s2a-hello", "banana-s2b-hello"));
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
     * With N as 1000 the test takes about 1.4sec.<br/>
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
        startFutures.add(client1.startAsync());
        
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002
        );
        startFutures.add(client2.startAsync());
        
        var client3 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client3",
                                   30003
        );
        startFutures.add(client3.startAsync());

        var client4 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client4",
                                   30004
        );
        startFutures.add(client4.startAsync());

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
        var publisher1 = client1.getPublisher("hello");
        var publisher2 = client2.getPublisher("hello");
        var publisher3 = client3.getPublisher("hello");

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
        startFutures.add(client1.startAsync());
        
        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   30002
        );
        startFutures.add(client2.startAsync());
        
        var client3 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client3",
                                   30003
        );
        startFutures.add(client3.startAsync());

        var client4 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client4",
                                   30004
        );
        startFutures.add(client4.startAsync());
        
        var client5 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client5",
                                   30005
        );
        startFutures.add(client5.startAsync());
        
        var client6 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client6",
                                   30006
        );
        startFutures.add(client6.startAsync());

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
        assertEquals("ClientAccepted=6, SubscriberAdded=4", centralServer.getTypesSent());
        
        // 1st client creates publisher.
        client1.createPublisher("hello", CloneableString.class);
        sleep(250); // time to let createSubscriber be handled and publishers sent down to clients
        assertTrue(futurePublisher2.isDone());
        assertTrue(futurePublisher3.isDone());
        assertTrue(futurePublisher4.isDone());
        assertEquals("AddSubscriber=4, CreatePublisher=1, FetchPublisher=3, Identification=6", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=6, CreatePublisher=3, PublisherCreated=1, SubscriberAdded=4", centralServer.getTypesSent());

        // 5th client fetches publisher after it is already in the server.
        CompletableFuture<Publisher> futurePublisher5 = client5.fetchPublisher("hello");
        CompletableFuture<Publisher> futurePublisher5b = client5.fetchPublisher("hello");
        assertFalse(futurePublisher5.isDone());
        assertSame(futurePublisher5, futurePublisher5b);
        Publisher publisher5 = futurePublisher5.get(1000, TimeUnit.MILLISECONDS);
        assertEquals("hello", publisher5.getTopic());
        assertEquals("CloneableString", publisher5.getPublisherClass().getSimpleName());
        assertEquals("AddSubscriber=4, CreatePublisher=1, FetchPublisher=4, Identification=6", centralServer.getValidTypesReceived()); // FetchPublisher +1
        assertEquals("ClientAccepted=6, CreatePublisher=4, PublisherCreated=1, SubscriberAdded=4", centralServer.getTypesSent()); // CreatePublisher +1

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
        assertEquals("ClientAccepted=6, CreatePublisher=4, PublisherCreated=1, SubscriberAdded=4", centralServer.getTypesSent());
        
        // Upon being shut down and a new one started, a CreatePublisher is not sent to the client machine with the same name.
        var client6b = createClient(PubSub.defaultQueueCreator(),
                                    PubSub.defaultSubscriptionMessageExceptionHandler(),
                                    "client6",
                                    30006
        );
        waitFor(Collections.singletonList(client6b.startAsync()));
        sleep(250); // wait for central server to send any CreatePublisher commands
        assertEquals("AddSubscriber=4, CreatePublisher=1, FetchPublisher=5, Identification=7", centralServer.getValidTypesReceived()); // unchanged: i.e. FetchPublisher for client6 not sent
        assertEquals("ClientAccepted=7, CreatePublisher=4, PublisherCreated=1, SubscriberAdded=4", centralServer.getTypesSent()); // CreatePublisher not sent to client6b
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
        startFutures.add(client1.startAsync());
        
        waitFor(startFutures);
        sleep(250);

        assertEquals("Identification=1", client1.getTypesSent());
        assertEquals("Identification=1", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=1", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1", client1.getTypesReceived());
        
        var messageWriterField = DistributedSocketPubSub.class.getDeclaredField("messageWriter");
        messageWriterField.setAccessible(true);
        var messageWriter = messageWriterField.get(client1);
        var sendMethod = messageWriter.getClass().getDeclaredMethod("internalPutMessage", TopicMessageBase.class);
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
    
    private static class BogusClientGeneratedMessage extends MessageClasses.ClientGeneratedTopicMessage {
        private static final long serialVersionUID = 1L;

        BogusClientGeneratedMessage() {
            super(System.currentTimeMillis(), "mytopic");
        }

        @Override
        public String toLoggingString() {
            return "BogusClientGeneratedMessage";
        }
    }
    
    private static class BogusMessage implements MessageClasses.TopicMessageBase {
        private static final long serialVersionUID = 1L;

        @Override
        public String toLoggingString() {
            return "BogusMessage";
        }
        
        @Override
        public String getTopic() {
            return "mytopic";
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
        startFutures.add(client1.startAsync());
        
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
        startFutures.add(client1.startAsync());
        startFutures.add(client2.startAsync());
        
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
        clientSocketTransformer.setWriteFailCount("CreatePublisher");
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
        assertEquals("ClientAccepted=2, PublisherCreated=1", centralServer.getTypesSent());
        assertEquals("ClientAccepted=1, PublisherCreated=1", client1.getTypesReceived());
        assertEquals("ClientAccepted=1", client2.getTypesReceived());
        assertThat(client1.getSendFailures(), Matchers.empty());
        assertThat(centralServer.getSendFailures(), Matchers.empty());
        
        // client2 subscribes to the topic, which sends the createPublisher command down to them
        // we mock so that each call to send message to server fails
        // verify that there is one initial send and 3 retries but all fail on the server
        System.out.println("Test server retry");
        serverSocketTransformer.setWriteFailCount("CreatePublisher");
        client2.subscribe("world", "ClientTwoSubscriber", CloneableString.class, str -> { });
        sleep(1000 + 2000 + 4000); // 1st retry after 1sec, 2nd retry after 2sec, 3rd and last retry after 4sec
        sleep(250); // wait for message to reach client
        assertEquals("CreatePublisher=1, Identification=1", client1.getTypesSent());
        assertEquals("AddSubscriber=1, Identification=1", client2.getTypesSent());
        assertEquals("AddSubscriber=1, CreatePublisher=1, Identification=2", centralServer.getValidTypesReceived());
        assertEquals("ClientAccepted=2, PublisherCreated=1, SubscriberAdded=1", centralServer.getTypesSent()); // CreatePublisher=1 not present
        assertEquals("ClientAccepted=1, PublisherCreated=1", client1.getTypesReceived());
        assertEquals("ClientAccepted=1, SubscriberAdded=1", client2.getTypesReceived()); // CreatePublisher=1 not present
        assertThat(centralServer.getSendFailures(), Matchers.contains("CreatePublisher: TestSocketTransformer write failure"));
    }
    
    /**
     * Basic test for sharding.
     * There are two central servers (the first handling topics A-M and the second handling topics N-Z) and 3 clients.
     * Client 1 creates two publishers, client 2 subscribes, client3 subscribes, then client 1 publishes messages to each topic.
     */
    @Test
    void testSharding() throws IOException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        List<CompletableFuture<Void>> startFutures = new ArrayList<>();

        var centralServer1 = createServer(Collections.emptyMap(), CENTRAL_SERVER_PORT);
        startFutures.add(centralServer1.start());
        var centralServer2 = createServer(Collections.emptyMap(), SECOND_CENTRAL_SERVER_PORT);
        startFutures.add(centralServer2.start());
        sleep(250); // time to let the central servers start
        
        class ImmutableShardingMapper extends KeyToSocketAddressMapper {
            private final int firstPort;
            private int currentPort;
            
            ImmutableShardingMapper(int firstPort) {
                this.firstPort = firstPort;
                this.currentPort = firstPort;
            }
            
            @Override
            public Set<SocketAddress> getRemoteUniverse() {
                return Set.of(centralServer1.getMessageServerAddress(), centralServer2.getMessageServerAddress());
            }

            @Override
            protected @NotNull SocketAddress doMapKeyToRemoteAddress(String key) {
                if (Character.toUpperCase(key.charAt(0)) <= 'M') {
                    return centralServer1.getMessageServerAddress();
                } else {
                    return centralServer2.getMessageServerAddress();
                }
            }

            @Override
            protected @NotNull SocketAddress generateLocalAddress() {
                if (currentPort == firstPort + 2) {
                    throw new IllegalStateException("too many ports");
                }
                return new InetSocketAddress("localhost", currentPort++);
            }
        }
        
        var client1 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client1",
                                   new ImmutableShardingMapper(30001)
        );
        startFutures.add(client1.startAsync());

        var client2 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client2",
                                   new ImmutableShardingMapper(30003)
        );
        startFutures.add(client2.startAsync());

        var client3 = createClient(PubSub.defaultQueueCreator(),
                                   PubSub.defaultSubscriptionMessageExceptionHandler(),
                                   "client3",
                                   new ImmutableShardingMapper(30005)
        );
        startFutures.add(client3.startAsync());

        waitFor(startFutures);
        assertEquals("Identification=2", client1.getTypesSent());
        assertEquals("ClientAccepted=2", client1.getTypesReceived());
        assertEquals("Identification=2", client2.getTypesSent());
        assertEquals("ClientAccepted=2", client2.getTypesReceived());
        assertEquals("Identification=2", client3.getTypesSent());
        assertEquals("ClientAccepted=2", client3.getTypesReceived());
        
        assertEquals("Identification=3", centralServer1.getValidTypesReceived());
        assertEquals("ClientAccepted=3", centralServer1.getTypesSent());
        assertEquals("Identification=3", centralServer2.getValidTypesReceived());
        assertEquals("ClientAccepted=3", centralServer2.getTypesSent());

        // client1 create two publisher2, client2 subscribes to both topics, client3 subscribes to one topic
        Publisher helloPublisher1 = client1.createPublisher("hello", CloneableString.class);
        Publisher worldPublisher1 = client1.createPublisher("world", CloneableString.class);
        client2.subscribe("hello", "ClientTwoHelloSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a(hello)")));
        client2.subscribe("hello", "ClientTwoHelloSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b(hello)")));
        client2.subscribe("world", "ClientTwoWorldSubscriber_First", CloneableString.class, str -> words.add(str.append("-s2a(world)")));
        client2.subscribe("world", "ClientTwoWorldSubscriber_Second", CloneableString.class, str -> words.add(str.append("-s2b(world)")));
        client3.subscribe("hello", "ClientThreeHelloSubscriber", CloneableString.class, str -> words.add(str.append("-s3(hello)")));
        sleep(250);
        
        assertEquals("CreatePublisher=2, Identification=2", client1.getTypesSent());
        assertEquals("ClientAccepted=2, PublisherCreated=2", client1.getTypesReceived());
        assertEquals("AddSubscriber=4, Identification=2", client2.getTypesSent());
        assertEquals("ClientAccepted=2, CreatePublisher=2, SubscriberAdded=4", client2.getTypesReceived());
        assertEquals("AddSubscriber=1, Identification=2", client3.getTypesSent());
        assertEquals("ClientAccepted=2, CreatePublisher=1, SubscriberAdded=1", client3.getTypesReceived());
        assertThat(words, Matchers.empty());

        assertEquals("AddSubscriber=3, CreatePublisher=1, Identification=3", centralServer1.getValidTypesReceived());
        assertEquals("ClientAccepted=3, CreatePublisher=2, PublisherCreated=1, SubscriberAdded=3", centralServer1.getTypesSent());
        assertEquals("AddSubscriber=2, CreatePublisher=1, Identification=3", centralServer2.getValidTypesReceived());
        assertEquals("ClientAccepted=3, CreatePublisher=1, PublisherCreated=1, SubscriberAdded=2", centralServer2.getTypesSent());

        // publish two messages to one topic, and three messages to the other topic
        helloPublisher1.publish(new CloneableString("one"));
        helloPublisher1.publish(new CloneableString("two"));
        worldPublisher1.publish(new CloneableString("three"));
        worldPublisher1.publish(new CloneableString("four"));
        worldPublisher1.publish(new CloneableString("five"));
        sleep(250);

        System.out.println("after publish 5 words: actual=" + words);
        assertEquals("CreatePublisher=2, Identification=2, PublishMessage=5", client1.getTypesSent());
        assertEquals("ClientAccepted=2, PublisherCreated=2", client1.getTypesReceived());
        assertEquals("AddSubscriber=4, Identification=2", client2.getTypesSent());
        assertEquals("ClientAccepted=2, CreatePublisher=2, PublishMessage=5, SubscriberAdded=4", client2.getTypesReceived());
        assertEquals("AddSubscriber=1, Identification=2", client3.getTypesSent());
        assertEquals("ClientAccepted=2, CreatePublisher=1, PublishMessage=2, SubscriberAdded=1", client3.getTypesReceived());
        assertThat(words, Matchers.containsInAnyOrder("one-s2a(hello)", "one-s2b(hello)", "one-s3(hello)",
                                                      "two-s2a(hello)", "two-s2b(hello)", "two-s3(hello)",
                                                      "three-s2a(world)", "three-s2b(world)",
                                                      "four-s2a(world)", "four-s2b(world)",
                                                      "five-s2a(world)", "five-s2b(world)"));
        
        assertEquals("AddSubscriber=3, CreatePublisher=1, Identification=3, PublishMessage=2", centralServer1.getValidTypesReceived());
        assertEquals("ClientAccepted=3, CreatePublisher=2, PublishMessage=4, PublisherCreated=1, SubscriberAdded=3", centralServer1.getTypesSent());
        assertEquals("AddSubscriber=2, CreatePublisher=1, Identification=3, PublishMessage=3", centralServer2.getValidTypesReceived());
        assertEquals("ClientAccepted=3, CreatePublisher=1, PublishMessage=3, PublisherCreated=1, SubscriberAdded=2", centralServer2.getTypesSent());
    }

    /**
     * Call the main function in a new process.
     * The main function starts a server and client, but does not shut them down.
     * The shutdown hook called when the process ends will shut down the client and server.
     * Without this, subsequent tests will not be able to reuse the same ports.
     */
    @Test
    void testShutdownHook() throws IOException, InterruptedException {
        System.out.println(new java.io.File(".").getCanonicalPath());
        var processBuilder = new ProcessBuilder(
                "java",
                "-cp",
                "target/classes:../org.sn.myutils.core/target/classes:../org.sn.myutils.testutils/target/classes:target/test-classes",
                "-ea",
                "-Djava.util.logging.config.file=../org.sn.myutils.testutils/target/classes/logging.properties",
                "org.sn.myutils.pubsub.DistributedPubSubIntegrationTest")
                .inheritIO();
        var process = processBuilder.start();
        boolean finished = process.waitFor(5, TimeUnit.SECONDS);
        System.out.println("finished=" + finished);
        System.out.println("exitCode=" + process.exitValue());
        assertTrue(finished);
        assertEquals(0, process.exitValue());
    }

    public static void main(String[] args) throws IOException {
        DistributedMessageServer centralServer = new TestDistributedMessageServer(new InetSocketAddress(CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT),
                                                                                  Collections.emptyMap());
        centralServer.start();
        sleep(250); // time to let the central server start
        
        var client1 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client1",
                                                      KeyToSocketAddressMapper.forSingleHostAndPort("localhost",
                                                                                                    30001,
                                                                                                    CENTRAL_SERVER_HOST,
                                                                                                    CENTRAL_SERVER_PORT));
        client1.startAsync();
        
        var client2 = new TestDistributedSocketPubSub(1,
                                                      PubSub.defaultQueueCreator(),
                                                      PubSub.defaultSubscriptionMessageExceptionHandler(),
                                                      "client2",
                                                      KeyToSocketAddressMapper.forSingleHostAndPort("localhost",
                                                                                                    30002,
                                                                                                    CENTRAL_SERVER_HOST,
                                                                                                    CENTRAL_SERVER_PORT));
        client2.startAsync();

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
     * Use the default port CENTRAL_SERVER_PORT.
     */
    private TestDistributedMessageServer createServer(Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        return createServer(mostRecentMessagesToKeep, CENTRAL_SERVER_PORT);
    }
    
    /**
     * Create a server and add it to list of objects to be shutdown at the end of the test function.
     */
    private TestDistributedMessageServer createServer(Map<RetentionPriority, Integer> mostRecentMessagesToKeep, int port) throws IOException {
        var server = new TestDistributedMessageServer(new InetSocketAddress(CENTRAL_SERVER_HOST, port),
                                                      mostRecentMessagesToKeep);
        addShutdown(server);
        return server;
    }
    
    /**
     * Create a server and add it to list of objects to be shutdown at the end of the test function.
     * Use the default port CENTRAL_SERVER_PORT.
     */
    private TestDistributedMessageServer createServer(SocketTransformer socketTransformer,
                                                      Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        var server = new TestDistributedMessageServer(socketTransformer,
                                                      new InetSocketAddress(CENTRAL_SERVER_HOST, CENTRAL_SERVER_PORT),
                                                      mostRecentMessagesToKeep);
        addShutdown(server);
        return server;
    }
    
    /**
     * Create a client and add it to list of objects to be shutdown at the end of the test function.
     * Use the KeyToSocketAddressMapper for a single host and port.
     */
    private TestDistributedSocketPubSub createClient(Supplier<Queue<Subscriber>> queueCreator,
                                                     SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                                     String machineId,
                                                     int localPort) throws IOException {
        var client = createClient(queueCreator,
                                  subscriptionMessageExceptionHandler,
                                  machineId,
                                  KeyToSocketAddressMapper.forSingleHostAndPort("localhost",
                                                                                localPort,
                                                                                DistributedPubSubIntegrationTest.CENTRAL_SERVER_HOST,
                                                                                DistributedPubSubIntegrationTest.CENTRAL_SERVER_PORT));
        addShutdown(client);
        return client;
    }
    
    /**
     * Create a client and add it to list of objects to be shutdown at the end of the test function.
     */
    private TestDistributedSocketPubSub createClient(Supplier<Queue<Subscriber>> queueCreator,
                                                     SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                                     String machineId,
                                                     KeyToSocketAddressMapper mapper) throws IOException {
        var client = new TestDistributedSocketPubSub(1,
                                                     queueCreator,
                                                     subscriptionMessageExceptionHandler,
                                                     machineId,
                                                     mapper);
        addShutdown(client);
        return client;
    }
    
    /**
     * Create a client and add it to list of objects to be shutdown at the end of the test function.
     * Use the KeyToSocketAddressMapper for a single host and port.
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
                                                     KeyToSocketAddressMapper.forSingleHostAndPort("localhost",
                                                                                                   port,
                                                                                                   DistributedPubSubIntegrationTest.CENTRAL_SERVER_HOST,
                                                                                                   DistributedPubSubIntegrationTest.CENTRAL_SERVER_PORT));
        addShutdown(client);
        return client;
    }
    
    private void addShutdown(Shutdowneable s) {
        shutdowns.add(s);
    }
    
    private static <T> void waitFor(List<CompletableFuture<T>> futures) {
        try {
            Instant startTime = Instant.now();
            TestUtil.toList(futures);
            Duration timeTaken = Duration.between(startTime, Instant.now());
            LOGGER.log(Level.INFO, "Time taken to start servers and clients: numObjects=" + futures.size() + ", timeTaken=" + timeTaken);
        } catch (CompletionException e) {
            var cause = ExceptionUtils.unwrapCompletionException(e);
            if (cause instanceof StartException) {
                StartException startException = (StartException) cause;
                System.err.println(startException.getMessage());
                startException.getExceptions().entrySet().forEach(entry -> System.err.println(entry.toString()));
            }
            throw e;
        }
    }
}



class TestDistributedMessageServer extends DistributedMessageServer {
    private final List<String> validTypesReceived = Collections.synchronizedList(new ArrayList<>());
    private final List<String> typesSent = Collections.synchronizedList(new ArrayList<>());
    private final List<String> sendFailures = Collections.synchronizedList(new ArrayList<>());
    private String securityKey;
    private Consumer<MessageBase> messageSentListener;

    public TestDistributedMessageServer(SocketAddress messageServer,
                                        Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        super(messageServer, mostRecentMessagesToKeep);
    }

    public TestDistributedMessageServer(SocketTransformer socketTransformer,
                                        SocketAddress messageServer,
                                        Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        super(socketTransformer, messageServer, mostRecentMessagesToKeep);
    }

    public void setSecurityKey(String key) {
        securityKey = key;
    }

    void setMessageSentListener(Consumer<MessageBase> listener) {
        if (this.messageSentListener != null) {
            throw new IllegalStateException("too many listeners");
        }
        this.messageSentListener = listener;
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
    protected @Nullable String canCreatePublisher(CreatePublisher createPublisher) {
        String error = super.canCreatePublisher(createPublisher);
        if (securityKey != null) {
            String securityKeyPassedByClient = createPublisher.getCustomProperties().get("SECURITY_KEY");
            if (!securityKey.equals(securityKeyPassedByClient)) {
                error = "Invalid security key";
            }
        }
        return error;
    }

    @Override
    protected @Nullable String canSubscribe(AddOrRemoveSubscriber addOrRemoveSubscriber) {
        String error = super.canSubscribe(addOrRemoveSubscriber);
        if (securityKey != null) {
            String securityKeyPassedByClient = addOrRemoveSubscriber.getCustomProperties().get("SECURITY_KEY");
            if (!securityKey.equals(securityKeyPassedByClient)) {
                error = "Invalid security key";
            }
        }
        return error;
    }
    
    @Override
    protected void onMessageSent(MessageBase message) {
        super.onMessageSent(message);
        typesSent.add(message.getClass().getSimpleName());
        if (messageSentListener != null) {
            messageSentListener.accept(message);
        }
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
    private String securityKey;
    private boolean enableTamperServerIndex;
    private Function<MessageBase, Boolean> messageReceivedListener;
 
    public TestDistributedSocketPubSub(int numInMemoryHandlers,
                                       Supplier<Queue<Subscriber>> queueCreator,
                                       SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                       String machineId,
                                       KeyToSocketAddressMapper messageServerLookup) throws IOException {
        super(new PubSubConstructorArgs(numInMemoryHandlers, queueCreator, subscriptionMessageExceptionHandler),
              machineId,
              messageServerLookup);
    }

    public TestDistributedSocketPubSub(SocketTransformer socketTransformer,
                                       int numInMemoryHandlers,
                                       Supplier<Queue<Subscriber>> queueCreator,
                                       SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                       String machineId,
                                       KeyToSocketAddressMapper messageServerLookup) throws IOException {
        super(socketTransformer,
              new PubSubConstructorArgs(numInMemoryHandlers, queueCreator, subscriptionMessageExceptionHandler),
              machineId,
              messageServerLookup);
    }

    void enableSecurityKey(String key) {
        securityKey = key;
    }

    void enableTamperServerIndex() {
        enableTamperServerIndex = true;        
    }
    
    void setMessageReceivedListener(Function<MessageBase, Boolean> listener) {
        if (this.messageReceivedListener != null) {
            throw new IllegalStateException("too many listeners");
        }
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
    protected void addCustomPropertiesForCreatePublisher(Map<String, String> customProperties, String topic) {
        super.addCustomPropertiesForCreatePublisher(customProperties, topic);
        if (securityKey != null) {
            customProperties.put("SECURITY_KEY", securityKey);
        }
    }

    @Override
    protected void addCustomPropertiesForAddSubscriber(Map<String, String> customProperties, String topic, String subscriberName) {
        super.addCustomPropertiesForAddSubscriber(customProperties, topic, subscriberName);
        if (securityKey != null) {
            customProperties.put("SECURITY_KEY", securityKey);
        }
    }

    @Override
    protected void addCustomPropertiesForRemoveSubscriber(Map<String, String> customProperties, String topic, String subscriberName) {
        super.addCustomPropertiesForRemoveSubscriber(customProperties, topic, subscriberName);
        if (securityKey != null) {
            customProperties.put("SECURITY_KEY", securityKey);
        }
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
    protected boolean onMessageReceived(MessageBase message) {
        boolean ok = super.onMessageReceived(message);
        typesReceived.add(message.getClass().getSimpleName());
        if (messageReceivedListener != null) {
            ok &= messageReceivedListener.apply(message);
        }
        return ok;
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
    private String writeFailCountType;
    private int writeFailCount;
    private int writeCount;
    private int readFailCount;
    private int readCount;
    
    /**
     * The first 4 attempts by the client/server to send a message of the given type will fail.
     */
    void setWriteFailCount(String type) {
        this.writeFailCountType = type;
        this.writeFailCount = 4;
        this.writeCount = 0;
    }

    /**
     * The first 1 attempt by the client/server to read a message will fail.
     */
    void setReadFailCount() {
        this.readFailCount = 1;
        this.readCount = 0;
    }
    
    @Override
    public void writeMessageToSocket(MessageBase message, short maxLength, SocketChannel channel) throws IOException {
        if (message.getClass().getSimpleName().equals(writeFailCountType)) {
            if (++writeCount <= writeFailCount) {
                throw new IOException("TestSocketTransformer write failure");
            }
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
        if (message.getClass().getSimpleName().equals(writeFailCountType)) {
            if (++writeCount <= writeFailCount) {
                return CompletableFuture.failedFuture(new IOException("TestSocketTransformer write failure"));
            }
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
