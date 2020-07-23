package myutils.pubsub;

import static myutils.pubsub.PubSubUtils.addShutdownHook;
import static myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static myutils.pubsub.PubSubUtils.closeQuietly;
import static myutils.pubsub.PubSubUtils.computeExponentialBackoff;
import static myutils.pubsub.PubSubUtils.getLocalAddress;
import static myutils.pubsub.PubSubUtils.getRemoteAddress;
import static myutils.util.concurrent.MoreExecutors.createThreadFactory;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.lang.ref.Cleaner.Cleanable;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import myutils.pubsub.MessageClasses.AddSubscriber;
import myutils.pubsub.MessageClasses.CreatePublisher;
import myutils.pubsub.MessageClasses.DownloadPublishedMessages;
import myutils.pubsub.MessageClasses.FetchPublisher;
import myutils.pubsub.MessageClasses.Identification;
import myutils.pubsub.MessageClasses.InvalidRelayMessage;
import myutils.pubsub.MessageClasses.MessageBase;
import myutils.pubsub.MessageClasses.PublishMessage;
import myutils.pubsub.MessageClasses.RelayFields;
import myutils.pubsub.MessageClasses.RelayMessageBase;
import myutils.pubsub.MessageClasses.RemoveSubscriber;
import myutils.pubsub.PubSubUtils.CallStackCapturing;

/**
 * Client class that acts as an in-memory publish/subscribe system, as well as talks to a server to send and receive publish commands.
 * 
 * <p>When a DistributedPubSub is started it connects to the DistributedMessageServer, and identifies itself to the server.
 * The identification includes the client's name, and must be unique across all machines in the system.
 * If the DistributedMessageServer is not available, this class attempts to connect to the DistributedPubSub again which capped exponential backoff. 
 * 
 * <p>Since this class inherits from PubSub, it also implements the in-memory publish/subscribe system.
 * 
 * <p>When a user calls createSubscriber, the DistributedPubSub sends this command to the server, which relays the command to all known clients.
 * When a user calls publisher.publish, the DistributedPubSub sends this command to the server, which relays the command to all known clients.
 * The DistributedPubSub also listens for messages sent from the server, which are messages relayed to it by other clients.
 * Upon receiving a message, the DistributedPubSub calls createPublisher or publisher.publish.
 * 
 * <p>Besides the identification message, each message sent to the server includes the client time and a monotonically increasing index.
 * But the server revises this number to a new number which is unique across all machines in the system.
 * 
 * <p>In implementation there is one thread that listens for messages from the server,
 * one thread that sends messages to the server,
 * and another that handles retires with exponential backoff.
 * 
 * <p>If the DistributedMessageServer dies, this DistributedPubSub detects it and stops the reader and writer threads
 * and attempts to connect to the DistributedPubSub again which capped exponential backoff.
 * Upon getting connected this class resends all publishers it created and topics it subscribed to.
 * This is to tell the new DistributedMessageServer the publishers and subscribers that exist, in case the DistributedMessageServer does not have a copy of them.
 * 
 * <p>About messages sent between client and server:
 * The first two bytes are the length of the message.
 * The next N bytes is the message, when serialized and converted to a byte stream.
 */
public class DistributedSocketPubSub extends PubSub {
    private static final System.Logger LOGGER = System.getLogger(DistributedSocketPubSub.class.getName());
    private static final ThreadLocal<RelayMessageBase> remoteRelayMessage = new ThreadLocal<>();
    private static final int MAX_RETRIES = 3;
    
    private String messageServerHost;
    private int messageServerPort;
    private final String machineId;
    private final SocketAddress localAddress;
    private final AtomicReference<SocketChannel> channelHolder;
    private final ExecutorService channelExecutor = Executors.newFixedThreadPool(2, createThreadFactory("DistributedSocketPubSub"));
    private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, createThreadFactory("DistributedSocketPubSub.Retry"));
    private final MessageWriter messageWriter;
    private final AtomicLong localMaxMessage = new AtomicLong();
    private final Map<String /*topic*/, CompletableFuture<Publisher>> fetchPublisherMap = new HashMap<>();
    private final Cleanable cleanable;
    private volatile Future<?> messageWriterTask;

    /**
     * Create an in-memory publish/subscribe system and also talk to a central
     * server to send and receive publish commands.
     * 
     * @param numInMemoryHandlers the number of threads handling messages that are published by all publishers.
     * @param queueCreator the queue to store all message across all subscribers.
     * @param subscriptionMessageExceptionHandler the general subscription handler for exceptions arising from all subscribers.
     * @param machineId the name of this machine, and if null the code will set it to this machine's hostname
     * @param localServer the local server
     * @param localPort the local port
     * @param messageServerHost the server to connect to in order to send and receive publish commands
     * @param messageServerPort the server to connect to in order to send and receive publish commands
     * @throws IOException if there is an error opening the socket channel
     */
    public DistributedSocketPubSub(int numInMemoryHandlers,
                                   Supplier<Queue<Subscriber>> queueCreator,
                                   SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler,
                                   @Nullable String machineId,
                                   String localServer,
                                   int localPort,
                                   String messageServerHost,
                                   int messageServerPort) throws IOException {
        super(numInMemoryHandlers, queueCreator, subscriptionMessageExceptionHandler);
        this.messageServerHost = messageServerHost;
        this.messageServerPort = messageServerPort;
        this.machineId = machineId != null ? machineId : InetAddress.getLocalHost().getHostName();
        this.localAddress = new InetSocketAddress(localServer, localPort);
        this.channelHolder = new AtomicReference<>(createNewSocket());
        this.messageWriter = createMessageWriter(this.machineId, messageServerHost, messageServerPort);
        this.cleanable = addShutdownHook(this,
                                         new Cleanup(machineId, channelHolder, channelExecutor, retryExecutor),
                                         DistributedSocketPubSub.class);
    }
    
    private SocketChannel createNewSocket() throws IOException {
        var channel = SocketChannel.open();
        onBeforeSocketBound(channel);
        channel.bind(localAddress);
        return channel;
    }

    private MessageWriter createMessageWriter(@Nullable String machineId,
                                              String messageServerHost,
                                              int messageServerPort) throws IOException {
        MessageWriter messageWriter = new MessageWriter();
        return messageWriter;
    }

    /**
     * Start the message client by connecting to the server and starting all threads.
     * Returns a future that is resolved when everything starts, or rejected if starting fails.
     * If the server is not available, retries connecting to the server with exponential backoff starting at 1 second, 2 seconds, 4 seconds, 8 seconds, 8 seconds.
     * If there was another IOException in starting the future is rejected with this exception.
     * 
     * @throws java.util.concurrent.RejectedExecutionException if client was shutdown
     */
    public CompletionStage<Void> start() {
        return doStart(false);
    }
    
    private CompletionStage<Void> doStart(boolean sendAllPublishersAndSubscribers) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        retryExecutor.submit(() -> doStart(sendAllPublishersAndSubscribers, future, 0));
        return future;
    }
    
    private void doStart(boolean sendAllPublishersAndSubscribers, CompletableFuture<Void> future, int retry) {
        String snippet = String.format("DistributedSocketPubSub clientMachine=%s centralServer=%s:%d",
                                       machineId,
                                       messageServerHost,
                                       messageServerPort);
        try {
            var channel = channelHolder.get();
            if (channel.isConnected()) {
                future.completeExceptionally(new AlreadyConnectedException());
                return;
            }
            try {
                channel.connect(new InetSocketAddress(messageServerHost, messageServerPort));
                LOGGER.log(Level.INFO, String.format("Started %s, localAddress=%s remoteAddress=%s", snippet, getLocalAddress(channel), getRemoteAddress(channel)));
                messageWriter.blockingSendIdentification();
                if (sendAllPublishersAndSubscribers) {
                    doSendAllPublishersAndSubscribers();
                }
                messageWriterTask = channelExecutor.submit(messageWriter);
                channelExecutor.submit(new MessageReader());
                future.complete(null);
            } catch (ConnectException e) {
                int nextRetry = retry + 1;
                long delayMillis = computeExponentialBackoff(1000, nextRetry, 4);
                this.channelHolder.set(createNewSocket());
                LOGGER.log(Level.INFO, String.format("Failed to connect %s. Retrying in %d millis...", snippet, delayMillis));
                retryExecutor.schedule(() -> doStart(sendAllPublishersAndSubscribers, future, nextRetry), delayMillis, TimeUnit.MILLISECONDS);
            }
        } catch (IOException | RuntimeException | Error e) {
            LOGGER.log(Level.ERROR, String.format("Failed to start %s: %s", snippet, e.toString()));
            future.completeExceptionally(e);
        }
    }
    
    private void doSendAllPublishersAndSubscribers() {
        forEachPublisher(basePublisher -> {
            DistributedPublisher publisher = (DistributedPublisher) basePublisher;
            if (!publisher.isRemote()) {
                messageWriter.createPublisher(publisher.getTopic(), publisher.getPublisherClass(), publisher.getRemoteRelayFields(), /*isResend*/ true);
            }
            for (var subscriberName : publisher.getSubsciberNames()) {
                messageWriter.addSubscriber(publisher.getTopic(), subscriberName, /*isResend*/ true);
            }
        });
    }

    /**
     * Download as many messages starting with startIndex from the server.
     * The server does not retain messages forever, so it may not find the oldest messages.
     * 
     * @param startIndexInclusive the start index. Use 0 or 1 for no minimum.
     * @param endIndexInclusive the end index. Use Long.MAX_VALUE for no maximum.
     * @see DistributedMessageServer#DistributedMessageServer(String, int, java.util.Map) for the number of messages of each RetentionPriority to remember
     * @see RetentionPriority
     */
    public void download(long startIndexInclusive, long endIndexInclusive) {
        messageWriter.download(startIndexInclusive, endIndexInclusive);
    }

    /**
     * Thread that writes messages to the message server.
     */
    private class MessageWriter implements Runnable {
        private final BlockingQueue<MessageBase> queue = new LinkedBlockingQueue<>();

        private MessageWriter() {
        }

        private void blockingSendIdentification() throws IOException {
            try {
                var future = send(new Identification(machineId));
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new IOException(e);
            }
        }

        @Override
        public void run() {
            try {
                while (channelHolder.get().isConnected()) {
                    MessageBase message = queue.take();
                    send(message);
                }
            } catch (InterruptedException ignored) {
                LOGGER.log(Level.INFO, "MessageWriter interrupted: machine={0}", machineId);
            } catch (RuntimeException | Error e) {
                LOGGER.log(Level.ERROR, "Unexpected exception: machine=" + machineId, e);
            }
        }

        private CompletableFuture<Void> send(MessageBase message) {
            LOGGER.log(Level.TRACE,
                       String.format("Sending message to server: clientMachine=%s, %s",
                                     machineId,
                                     message.toLoggingString()));
            CompletableFuture<Void> future = new CompletableFuture<Void>();
            send(future, message, 0);
            return future;
        }
        
        private void send(CompletableFuture<Void> future, MessageBase message, int retry) {
            try {
                DistributedSocketPubSub.this.onBeforeSendMessage(message);
                SocketTransformer.writeMessageToSocket(message, Short.MAX_VALUE, channelHolder.get());
                DistributedSocketPubSub.this.onMessageSent(message);
                future.complete(null);
            } catch (IOException e) {
                boolean retryDone = retry >= MAX_RETRIES || SocketTransformer.isClosed(e);
                Level level = retryDone ? Level.WARNING : Level.DEBUG;
                LOGGER.log(level,
                    () -> String.format("Send message failed: machine=%s, retry=%d, retryDone=%b, exception=%s",
                                        machineId, retry, retryDone, e.toString()));
                if (!retryDone) {
                    int nextRetry = retry + 1;
                    long delayMillis = computeExponentialBackoff(1000, nextRetry, MAX_RETRIES);
                    retryExecutor.schedule(() -> send(future, message, nextRetry), delayMillis, TimeUnit.MILLISECONDS);
                } else {
                    future.completeExceptionally(e);
                }
            }
        }

        private void addSubscriber(@Nonnull String topic, @Nonnull String subscriberName, boolean isResend) {
            internalPutMessage(new AddSubscriber(topic, subscriberName, true, isResend));
        }

        private void removeSubscriber(@Nonnull String topic, @Nonnull String subscriberName) {
            internalPutMessage(new RemoveSubscriber(topic, subscriberName));
        }

        private void createPublisher(@Nonnull String topic, @Nonnull Class<?> publisherClass, RelayFields relayFields, boolean isResend) {
            var createPublisher = new CreatePublisher(localMaxMessage.incrementAndGet(), topic, publisherClass, isResend);
            createPublisher.setRelayFields(relayFields);
            internalPutMessage(createPublisher);
        }

        private void publishMessage(@Nonnull String topic, @Nonnull CloneableObject<?> message, RetentionPriority priority, RelayFields relayFields) {
            var publishMessage = new PublishMessage(localMaxMessage.incrementAndGet(), topic, message, priority);
            publishMessage.setRelayFields(relayFields);
            internalPutMessage(publishMessage);
        }

        public void download(long startIndexInclusive, long endIndexInclusive) {
            internalPutMessage(new DownloadPublishedMessages(startIndexInclusive, endIndexInclusive));
        }

        public void addFetchPublisher(String topic) {
            internalPutMessage(new FetchPublisher(topic));
        }

        private void internalPutMessage(MessageBase message) {
            try {
                queue.put(message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        public void interrupt() {
            if (messageWriterTask != null) {
                messageWriterTask.cancel(true);
                messageWriterTask = null;
            }
        }
    }

    /**
     * Thread that retrieves messages from the message server.
     */
    private class MessageReader implements Runnable {
        private MessageReader() {
        }

        @Override
        public void run() {
            boolean attemptRestart = false;
            
            var channel = channelHolder.get();
            while (channel.isConnected()) {
                try {
                    MessageBase message = SocketTransformer.readMessageFromSocket(channel);
                    LOGGER.log(Level.TRACE,
                               String.format("Received message from server: clientMachine=%s, %s",
                                             DistributedSocketPubSub.this.machineId,
                                             message.toLoggingString()));
                    DistributedSocketPubSub.this.onMessageReceived(message);
                    if (message instanceof CreatePublisher) {
                        CreatePublisher createPublisher = (CreatePublisher) message;
                        remoteRelayMessage.set(createPublisher);
                        DistributedSocketPubSub.this.createPublisher(createPublisher.getTopic(),
                                                                     createPublisher.getPublisherClass());
                    } else if (message instanceof PublishMessage) {
                        PublishMessage publishMessage = (PublishMessage) message;
                        remoteRelayMessage.set(publishMessage);
                        Optional<Publisher> publisher = DistributedSocketPubSub.this.getPublisher(publishMessage.getTopic());
                        if (publisher.isPresent()) {
                            publisher.get().publish(publishMessage.getMessage());
                        }
                    } else if (message instanceof InvalidRelayMessage) {
                        InvalidRelayMessage invalid = (InvalidRelayMessage) message;
                        LOGGER.log(Level.WARNING, invalid.getError());
                    } else {
                        LOGGER.log(Level.WARNING, "Unrecognized object type received: clientMachine={0}, messageClass={1}",
                                   DistributedSocketPubSub.this.machineId, message.getClass().getSimpleName());
                    }
                } catch (IOException e) {
                    if (SocketTransformer.isClosed(e)) {
                        LOGGER.log(Level.INFO, "Socket closed, ending reader: {0}", e.toString());
                        attemptRestart = true;
                        if (channel.isOpen()) {
                            closeQuietly(channel);
                        }
                    }
                    LOGGER.log(Level.WARNING, "Socket exception: machine={0}, exception={1}",
                               DistributedSocketPubSub.this.machineId, e.toString());
                } catch (RuntimeException | Error e) {
                    LOGGER.log(Level.ERROR, "Unexpected exception: machine=" + DistributedSocketPubSub.this.machineId, e);
                } finally {
                    remoteRelayMessage.set(null);
                }
            } // end while
            
            messageWriter.interrupt();
            
            if (attemptRestart) {
                DistributedSocketPubSub.this.doRestart();
            }
        }
    }
    
    private void doRestart() {
        channelHolder.set(null);
        try {
            channelHolder.set(createNewSocket());
            doStart(true).exceptionally(e -> {
                LOGGER.log(Level.ERROR, "Unable to restart DistributedSocketPubSub: %s", e.toString());
                return null;
            });
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Unable to restart DistributedSocketPubSub: %s", e.toString());
        }
    }
    
    /**
     * Publisher that forwards publish commands to the message server.
     */
    public final class DistributedPublisher extends Publisher {
        private final RelayFields remoteRelayFields;

        private DistributedPublisher(@Nonnull String topic, @Nonnull Class<?> publisherClass, RelayFields remoteRelayFields) {
            super(topic, publisherClass);
            this.remoteRelayFields = remoteRelayFields;
        }

        boolean isRemote() {
            return remoteRelayFields != null;
        }

        RelayFields getRemoteRelayFields() {
            return remoteRelayFields;
        }

        @Override
        public <T extends CloneableObject<?>> void publish(@Nonnull T message, RetentionPriority priority) {
            super.publish(message, priority);
            var relayFields = Optional.ofNullable(remoteRelayMessage.get()).map(RelayMessageBase::getRelayFields).orElse(null);
            if (relayFields == null) {
                // this DistributedPubSub is publishing a new message
                // so send it to the central server
                DistributedSocketPubSub.this.messageWriter.publishMessage(getTopic(), message, priority, null);
            }
        }
    }

    /**
     * Subscriber that is no different than the base class.
     */
    public final class DistributedSubscriber extends Subscriber {
        private DistributedSubscriber(@Nonnull String topic,
                                      @Nonnull String subscriberName,
                                      @Nonnull Class<? extends CloneableObject<?>> subscriberClass,
                                      @Nonnull Consumer<CloneableObject<?>> callback) {
            super(topic, subscriberName, subscriberClass, callback);
        }
    }

    /**
     * Create a publisher, and send a CreatePublisher command to the message server.
     */
    @Override
    protected <T> DistributedPublisher newPublisher(String topic, Class<T> publisherClass) {
        var relayFields = Optional.ofNullable(remoteRelayMessage.get()).map(RelayMessageBase::getRelayFields).orElse(null);
        var publisher = new DistributedPublisher(topic, publisherClass, relayFields);
        if (relayFields == null) {
            // this DistributedPubSub is creating a new publisher
            // so send it to the central server
            messageWriter.createPublisher(topic, publisherClass, null, /*isResend*/ false);
        }
        return publisher;
    }

    @Override
    protected DistributedSubscriber newSubscriber(@Nonnull String topic,
                                                  @Nonnull String subscriberName,
                                                  @Nonnull Class<? extends CloneableObject<?>> subscriberClass,
                                                  @Nonnull Consumer<CloneableObject<?>> callback) {
        return new DistributedSubscriber(topic, subscriberName, subscriberClass, callback);
    }
    
    /**
     * Fetch the publisher from central server.
     */
    public final CompletableFuture<Publisher> fetchPublisher(@Nonnull String topic) {
        synchronized (fetchPublisherMap) {
            var publisher = getPublisher(topic);
            if (publisher.isPresent()) {
                return CompletableFuture.completedFuture(publisher.get());
            } else {
                return fetchPublisherMap.computeIfAbsent(topic, t -> {
                    messageWriter.addFetchPublisher(t);
                    return new CompletableFuture<Publisher>();
                });
            }
        }
    }

    private void resolveFetchPublisher(Publisher publisher) {
        synchronized (fetchPublisherMap) {
            var future = fetchPublisherMap.remove(publisher.getTopic());
            if (future != null) {
                future.complete(publisher);
            }
        }
    }

    /**
     * Shutdown this object.
     * Object cannot be restarted after shutdown.
     */
    @Override
    public void shutdown() {
        super.shutdown();
        cleanable.clean();
    }
    
    /**
     * Resolve any futures waiting for this publisher to be created.
     */
    @Override
    protected void onPublisherAdded(Publisher publisher) {
        resolveFetchPublisher(publisher);
    }
    
    /**
     * Send the subscriber to the central server.
     */
    @Override
    protected void onAddSubscriber(Subscriber subscriber) {
        messageWriter.addSubscriber(subscriber.getTopic(),subscriber.getSubscriberName(), /*isResend*/ false);
    }

    /**
     * Remove the subscriber from the central server.
     * When all subscribers to the topic from this machine are removed, the central server will no longer send messages published to this topic to this machine. 
     */
    @Override
    protected void onRemoveSubscriber(Subscriber subscriber) {
        messageWriter.removeSubscriber(subscriber.getTopic(),subscriber.getSubscriberName());
    }

    /**
     * Override this function to set socket options.
     * For example, the unit tests set SO_REUSEADDR to true.
     */
    protected void onBeforeSocketBound(NetworkChannel channel) throws IOException {
    }

    /**
     * Override this function to do something before sending a message.
     */
    protected void onBeforeSendMessage(MessageBase message) {
    }

    /**
     * Override this function to do something after sending a message.
     * For example, the unit tests override this to record the number of messages sent.
     */
    protected void onMessageSent(MessageBase message) {
    }

    /**
     * Override this function to do something upon receiving a message.
     * For example, the unit tests override this to record the number of messages received.
     */
    protected void onMessageReceived(MessageBase message) {
    }


    /**
     * Cleanup this class. Close the socket channel and shutdown the executor.
     */
    private static class Cleanup extends CallStackCapturing implements Runnable {
        private final String machineId;
        private final AtomicReference<SocketChannel> channelHolder;
        private final ExecutorService channelExecutor;
        private final ExecutorService retryExecutor;

        private Cleanup(String machineId, AtomicReference<SocketChannel> channelHolder, ExecutorService channelExecutor, ExecutorService retryExecutor) {
            this.machineId = machineId;
            this.channelHolder = channelHolder;
            this.channelExecutor = channelExecutor;
            this.retryExecutor = retryExecutor;
        }

        @Override
        public void run() {
            LOGGER.log(Level.DEBUG, "Cleaning up " + machineId + " " + DistributedSocketPubSub.class.getSimpleName() + " " + getLocalAddress(channelHolder.get()) + getCallStack());
            closeExecutorQuietly(channelExecutor);
            closeExecutorQuietly(retryExecutor);
            closeQuietly(channelHolder.get());
        }
    }
}
