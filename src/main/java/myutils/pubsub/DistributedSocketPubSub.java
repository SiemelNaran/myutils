package myutils.pubsub;

import static myutils.pubsub.PubSubUtils.addShutdownHook;
import static myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static myutils.pubsub.PubSubUtils.closeQuietly;
import static myutils.pubsub.PubSubUtils.computeExponentialBackoff;
import static myutils.pubsub.PubSubUtils.getLocalAddress;
import static myutils.pubsub.PubSubUtils.getRemoteAddress;
import static myutils.util.concurrent.MoreExecutors.createThreadFactory;

import java.io.EOFException;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import myutils.pubsub.MessageClasses.AddSubscriber;
import myutils.pubsub.MessageClasses.ClientAccepted;
import myutils.pubsub.MessageClasses.ClientRejected;
import myutils.pubsub.MessageClasses.CreatePublisher;
import myutils.pubsub.MessageClasses.DownloadPublishedMessages;
import myutils.pubsub.MessageClasses.FetchPublisher;
import myutils.pubsub.MessageClasses.Identification;
import myutils.pubsub.MessageClasses.InvalidRelayMessage;
import myutils.pubsub.MessageClasses.MessageBase;
import myutils.pubsub.MessageClasses.PublishMessage;
import myutils.pubsub.MessageClasses.PublisherCreated;
import myutils.pubsub.MessageClasses.RelayFields;
import myutils.pubsub.MessageClasses.RelayMessageBase;
import myutils.pubsub.MessageClasses.RemoveSubscriber;
import myutils.pubsub.MessageClasses.SubscriberAdded;
import myutils.pubsub.MessageClasses.SubscriberRemoved;
import myutils.pubsub.PubSubUtils.CallStackCapturing;

/**
 * Client class that acts as an in-memory publish/subscribe system, as well as talks to a server to send and receive publish commands.
 * 
 * <p>When a DistributedPubSub is started it connects to the DistributedMessageServer, and identifies itself to the server.
 * The identification includes the client's name, and must be unique across all machines in the system.
 * If the DistributedMessageServer is not available, this class attempts to connect to the DistributedMessageServer again which capped exponential backoff.
 * 
 * <p>Since this class inherits from PubSub, it also implements the in-memory publish/subscribe system.
 * 
 * <p>When a user calls createPublisher, the new publisher is created as dormant, and it cannot be used to publish messages.
 * A call to pubsub.getPublisher will not find the publisher.
 * The server may verify if the client passed in security credentials that allow them to subscribe to a topic, so the server may send back a CreatePublisherInvalid command.
 * Upon receiving the confirmation message PublisherCreated from the server, the publisher is made live.
 *
 * <p>When a user calls subscribe, the DistributedPubSub sends this command to the server,
 * and the server responds with a SubscriberAdded and a CreatePublisher command, or AddSubscriberInvalid if the security credentials are not valid.
 * When a user calls publisher.publish, the DistributedPubSub sends this command to the server, which relays the command to all clients subscribed to the topic.
 * The DistributedPubSub also listens for messages sent from the server, which are messages relayed to it by other clients.
 * Upon receiving a message, the DistributedPubSub calls createPublisher or publisher.publish.
 * 
 * <p>This class does not provide security credentials support out of the box, but implementors of derived classes can add in this functionality via custom properties.
 * 
 * <p>Besides the identification message, each message sent to the server includes the client time and a monotonically increasing index.
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
    private static final ThreadLocal<RelayMessageBase> threadLocalRemoteRelayMessage = new ThreadLocal<>();
    private static final int MAX_RETRIES = 3;
    
    private final Map<String /*topic*/, DormantInfo> dormantInfoMap = new ConcurrentHashMap<>();
    private final SocketTransformer socketTransformer;
    private final String messageServerHost;
    private final int messageServerPort;
    private final ClientMachineId machineId;
    private final SocketAddress localAddress;
    private final AtomicReference<SocketChannel> channelHolder;
    private final ExecutorService channelExecutor = Executors.newFixedThreadPool(2, createThreadFactory("DistributedSocketPubSub", true)); // read thread and write thread
    private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, createThreadFactory("DistributedSocketPubSub.Retry", true));
    private final MessageWriter messageWriter;
    private final AtomicLong localMaxMessage = new AtomicLong();
    private final Map<String /*topic*/, CompletableFuture<Publisher>> fetchPublisherMap = new HashMap<>();
    private final Cleanable cleanable;
    private final OnClientAccepted onClientAccepted = new OnClientAccepted();
    private volatile Future<?> messageWriterTask; // task that represents the running MessageWriter, so that it can be interrupted
    
    /**
     * This class is about things to happen when the client is started and receives the ClientAccepted message.
     * @author snaran
     *
     */
    private static class OnClientAccepted {
        private volatile CompletableFuture<Void> startFuture;
        private volatile boolean sendAllPublishersAndSubscribers;
        
        void setAll(CompletableFuture<Void> startFuture, boolean sendAllPublishersAndSubscribers) {
            this.startFuture = startFuture;
            this.sendAllPublishersAndSubscribers = sendAllPublishersAndSubscribers;
        }

        boolean isSendAllPublishersAndSubscribers() {
            return sendAllPublishersAndSubscribers;
        }
        
        CompletableFuture<Void> getStartFuture() {
            return startFuture;
        }
    }
    
    /**
     * Create an in-memory publish/subscribe system and also talk to a central
     * server to send and receive publish commands.
     * 
     * @param baseArgs the arguments for the in-memory pubsub system
     * @param machineId the name of this machine, and if null the code will set it to this machine's hostname
     * @param localServer the local server
     * @param localPort the local port
     * @param messageServerHost the server to connect to in order to send and receive publish commands
     * @param messageServerPort the server to connect to in order to send and receive publish commands
     * @throws IOException if there is an error opening the socket channel
     * @see PubSubConstructorArgs for the arguments to the super class
     */
    public DistributedSocketPubSub(PubSubConstructorArgs baseArgs,
            @Nullable String machineId,
            String localServer,
            int localPort,
            String messageServerHost,
            int messageServerPort) throws IOException {
        this(new SocketTransformer(), baseArgs, machineId, localServer, localPort, messageServerHost, messageServerPort);
    }
    
    DistributedSocketPubSub(SocketTransformer socketTransformer,
                            PubSubConstructorArgs baseArgs,
                            @Nullable String machineId,
                            String localServer,
                            int localPort,
                            String messageServerHost,
                            int messageServerPort) throws IOException {
        super(baseArgs);
        this.socketTransformer = socketTransformer;
        this.messageServerHost = messageServerHost;
        this.messageServerPort = messageServerPort;
        this.machineId = new ClientMachineId(machineId != null ? machineId : InetAddress.getLocalHost().getHostName());
        this.localAddress = new InetSocketAddress(localServer, localPort);
        this.channelHolder = new AtomicReference<>(createNewSocket());
        this.messageWriter = createMessageWriter();
        this.cleanable = addShutdownHook(this,
                                         new Cleanup(this.machineId, channelHolder, channelExecutor, retryExecutor),
                                         DistributedSocketPubSub.class);
    }
    
    private SocketChannel createNewSocket() throws IOException {
        var channel = SocketChannel.open();
        onBeforeSocketBound(channel);
        channel.bind(localAddress);
        return channel;
    }

    private MessageWriter createMessageWriter() {
        return new MessageWriter();
    }

    /**
     * Start the message client asynchronously by connecting to the server and starting all threads.
     * Returns a future that is resolved when everything starts, or rejected if starting fails.
     * If the server is not available, retries connecting to the server with exponential backoff starting at 1 second, 2 seconds, 4 seconds, 8 seconds, 8 seconds.
     * If there was another IOException in starting the future is rejected with this exception.
     * 
     * @throws java.util.concurrent.RejectedExecutionException if client was shutdown
     */
    public CompletableFuture<Void> start() {
        return doStart(false);
    }
    
    private CompletableFuture<Void> doStart(boolean sendAllPublishersAndSubscribers) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        retryExecutor.submit(() -> doStart(sendAllPublishersAndSubscribers, future, 0));
        return future;
    }
    
    private void doStart(boolean sendAllPublishersAndSubscribers, CompletableFuture<Void> future, int retry) {
        String snippet = getStartLoggingSnippet();
        try {
            var channel = getInternalSocketChannel();
            if (channel.isConnected()) {
                future.completeExceptionally(new AlreadyConnectedException());
                return;
            }
            try {
                channel.connect(new InetSocketAddress(messageServerHost, messageServerPort));
                LOGGER.log(Level.INFO, String.format("Connected %s, localAddress=%s remoteAddress=%s", snippet, getLocalAddress(channel), getRemoteAddress(channel)));
                messageWriter.sendIdentificationNow();
                channelExecutor.submit(new MessageReader());
                onClientAccepted.setAll(future, sendAllPublishersAndSubscribers);
            } catch (ConnectException e) {
                int nextRetry = retry + 1;
                long delayMillis = computeExponentialBackoff(1000, nextRetry, 4);
                this.channelHolder.set(createNewSocket());
                LOGGER.log(Level.INFO, String.format("Failed to connect %s. Retrying in %d millis...", snippet, delayMillis));
                retryExecutor.schedule(() -> doStart(sendAllPublishersAndSubscribers, future, nextRetry), delayMillis, TimeUnit.MILLISECONDS);
            }
        } catch (IOException | RuntimeException | Error e) {
            LOGGER.log(Level.ERROR, String.format("Failed to start %s", snippet), e);
            future.completeExceptionally(e);
        }
    }
    
    private void startWriterThread(ClientAccepted clientAccepted) {
        try {
            messageWriterTask = channelExecutor.submit(messageWriter);
            String snippet = getStartLoggingSnippet();
            var channel = getInternalSocketChannel();
            LOGGER.log(Level.INFO, String.format("Started %s, localAddress=%s remoteAddress=%s, centralServerId=%s",
                                                 snippet,
                                                 getLocalAddress(channel),
                                                 getRemoteAddress(channel),
                                                 clientAccepted.getCentralServerId()));
            if (onClientAccepted.isSendAllPublishersAndSubscribers()) {
                doSendAllPublishersAndSubscribers();
            }
            onClientAccepted.getStartFuture().complete(null);
        } catch (RuntimeException | Error e) {
            onClientAccepted.getStartFuture().completeExceptionally(e);
        }
    }
    
    private void failedToStart(ClientRejected clientRejected) {
        onClientAccepted.getStartFuture().completeExceptionally(clientRejected.toException());
        try {
            var channel = getInternalSocketChannel();
            PubSubUtils.closeQuietly(channel);
            this.channelHolder.set(createNewSocket());
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Failed to reset create new channel");
        }
    }
    
    private String getStartLoggingSnippet() {
        return String.format("DistributedSocketPubSub: clientMachine=%s, centralServer=%s:%d",
                             machineId,
                             messageServerHost,
                             messageServerPort);
    }

    private void doSendAllPublishersAndSubscribers() {
        forEachPublisher(basePublisher -> {
            DistributedPublisher publisher = (DistributedPublisher) basePublisher;
            String topic = publisher.getTopic();
            if (!publisher.isRemote()) {
                messageWriter.createPublisher(publisher.getCreatedAtTimestamp(), topic, publisher.getPublisherClass(), publisher.getRemoteRelayFields(), /*isResend*/ true);
            }
            for (var subscriber : publisher.getSubscibers()) {
                messageWriter.addSubscriber(subscriber.getCreatedAtTimestamp(), topic, subscriber.getSubscriberName(), /*isResend*/ true);
            }
        });
    }
    
    /**
     * Return the socket channel.
     * Used for testing.
     */
    private SocketChannel getInternalSocketChannel() {
        return channelHolder.get();
    }

    /**
     * Download as many messages starting with startIndex from the server.
     * The server does not retain messages forever, so it may not find the oldest messages.
     * 
     * @param topics the topics to download
     * @param startIndexInclusive the start index. Use 0 or 1 for no minimum.
     * @param endIndexInclusive the end index. Use ServerIndex.MAX_VALUE for no maximum.
     * @see DistributedMessageServer#DistributedMessageServer(String, int, java.util.Map) for the number of messages of each RetentionPriority to remember
     * @see RetentionPriority
     */
    public void download(Collection<String> topics, ServerIndex startIndexInclusive, ServerIndex endIndexInclusive) {
        messageWriter.download(topics, startIndexInclusive, endIndexInclusive);
    }

    /**
     * Thread that writes messages to the message server.
     */
    private class MessageWriter implements Runnable {
        private final BlockingQueue<MessageBase> queue = new LinkedBlockingQueue<>();

        private MessageWriter() {
        }

        private void sendIdentificationNow() throws IOException {
            send(new Identification(machineId));
        }

        @Override
        public void run() {
            try {
                while (getInternalSocketChannel().isConnected()) {
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
            LOGGER.log(
                Level.TRACE,
                () -> String.format("Sending message to server: clientMachine=%s, %s",
                                    machineId,
                                    message.toLoggingString()));
            CompletableFuture<Void> future = new CompletableFuture<>();
            send(future, message, 0);
            return future;
        }
        
        private void send(CompletableFuture<Void> future, MessageBase message, int retry) {
            try {
                DistributedSocketPubSub.this.onBeforeSendMessage(message);
                socketTransformer.writeMessageToSocket(message, Short.MAX_VALUE, getInternalSocketChannel());
                DistributedSocketPubSub.this.onMessageSent(message);
                future.complete(null);
            } catch (IOException e) {
                boolean retryDone = retry >= MAX_RETRIES || SocketTransformer.isClosed(e);
                Level level = retryDone ? Level.WARNING : Level.DEBUG;
                LOGGER.log(level,
                           String.format("Send message failed: machine=%s, retry=%d, retryDone=%b",
                                         machineId, retry, retryDone),
                           e);
                if (!retryDone) {
                    int nextRetry = retry + 1;
                    long delayMillis = computeExponentialBackoff(1000, nextRetry, MAX_RETRIES);
                    retryExecutor.schedule(() -> send(future, message, nextRetry), delayMillis, TimeUnit.MILLISECONDS);
                } else {
                    future.completeExceptionally(e);
                    onSendMessageFailed(message, e);
                }
            } catch (RuntimeException | Error e) {
                LOGGER.log(Level.WARNING,
                           String.format("Send message failed: machine=%s, retry=%d, retryDone=%b",
                                         machineId, retry, true),
                           e);
                future.completeExceptionally(e);
            }
        }

        private void addSubscriber(long createdAtTimestamp, @Nonnull String topic, @Nonnull String subscriberName, boolean isResend) {
            internalPutMessage(new AddSubscriber(createdAtTimestamp, topic, subscriberName, true, isResend));
        }

        private void removeSubscriber(@Nonnull String topic, @Nonnull String subscriberName) {
            internalPutMessage(new RemoveSubscriber(topic, subscriberName));
        }

        private void createPublisher(long createdAtTimestamp, @Nonnull String topic, @Nonnull Class<?> publisherClass, RelayFields relayFields, boolean isResend) {
            var createPublisher = new CreatePublisher(createdAtTimestamp, localMaxMessage.incrementAndGet(), topic, publisherClass, isResend);
            createPublisher.setRelayFields(relayFields);
            internalPutMessage(createPublisher);
        }

        private void publishMessage(@Nonnull String topic, @Nonnull CloneableObject<?> message, RetentionPriority priority) {
            var publishMessage = new PublishMessage(localMaxMessage.incrementAndGet(), topic, message, priority);
            internalPutMessage(publishMessage);
        }

        public void download(Collection<String> topics, ServerIndex startIndexInclusive, ServerIndex endIndexInclusive) {
            internalPutMessage(new DownloadPublishedMessages(topics, startIndexInclusive, endIndexInclusive));
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
            
            var channel = getInternalSocketChannel();
            while (channel.isConnected()) {
                try {
                    MessageBase message = socketTransformer.readMessageFromSocket(channel);
                    LOGGER.log(
                        Level.TRACE,
                        () -> String.format("Received message from server: clientMachine=%s, %s",
                                            DistributedSocketPubSub.this.machineId,
                                            message.toLoggingString()));
                    DistributedSocketPubSub.this.onMessageReceived(message);
                    if (message instanceof CreatePublisher) {
                        CreatePublisher createPublisher = (CreatePublisher) message;
                        threadLocalRemoteRelayMessage.set(createPublisher);
                        String topic = createPublisher.getTopic();
                        DistributedPublisher publisher = (DistributedPublisher) DistributedSocketPubSub.this.createPublisher(topic,
                                                                                                                             createPublisher.getPublisherClass());
                        int numSubscribersMadeLive = 0;
                        var dormantInfo = DistributedSocketPubSub.this.dormantInfoMap.get(topic);
                        if (dormantInfo != null) {
                            numSubscribersMadeLive = dormantInfo.addConfirmedSubscribers(publisher);
                        }
                        LOGGER.log(Level.TRACE, "Remote publisher created: clientMachine={0}, topic={1}, numSubscribersMadeLive={2}",
                                   DistributedSocketPubSub.this.machineId, topic, numSubscribersMadeLive);
                    } else if (message instanceof PublisherCreated) {
                        PublisherCreated publisherCreated = (PublisherCreated) message;
                        String topic = publisherCreated.getTopic();
                        var dormantInfo = DistributedSocketPubSub.this.dormantInfoMap.get(topic);
                        var publisher = dormantInfo.makeDormantPublisherLive();
                        int numSubscribersMadeLive = dormantInfo.addConfirmedSubscribers(publisher);
                        publisher.setRemoteRelayFields(publisherCreated.getRelayFields());
                        LOGGER.log(Level.TRACE, "Confirm publisher created: clientMachine={0}, topic={1}, numSubscribersMadeLive={2}",
                                   DistributedSocketPubSub.this.machineId, publisherCreated.getTopic(), numSubscribersMadeLive);
                    } else if (message instanceof SubscriberAdded) {
                        SubscriberAdded subscriberAdded = (SubscriberAdded) message;
                        String topic = subscriberAdded.getTopic();
                        String subscriberName = subscriberAdded.getSubscriberName();
                        var dormantInfo = DistributedSocketPubSub.this.dormantInfoMap.get(topic);
                        var publisher = getPublisher(subscriberAdded.getTopic());
                        String liveStatus = dormantInfo.makeDormantSubscriberLive(publisher, subscriberName);
                        LOGGER.log(Level.TRACE, "Confirm subscriber added: clientMachine={0}, topic={1}, subscriberName={2}, liveStatus={3}",
                                   DistributedSocketPubSub.this.machineId, subscriberAdded.getTopic(), subscriberName, liveStatus);
                    } else if (message instanceof SubscriberRemoved) {
                        SubscriberRemoved subscriberRemoved = (SubscriberRemoved) message;
                        LOGGER.log(Level.TRACE, "Confirm subscriber removed: topic={0}, subscriberName={1}", subscriberRemoved.getTopic(), subscriberRemoved.getSubscriberName());
                    } else if (message instanceof PublishMessage) {
                        PublishMessage publishMessage = (PublishMessage) message;
                        threadLocalRemoteRelayMessage.set(publishMessage);
                        Publisher publisher = DistributedSocketPubSub.this.getPublisher(publishMessage.getTopic());
                        publisher.publish(publishMessage.getMessage());
                    } else if (message instanceof InvalidRelayMessage) {
                        InvalidRelayMessage invalid = (InvalidRelayMessage) message;
                        LOGGER.log(Level.WARNING, invalid.getError());
                    } else if (message instanceof ClientAccepted) {
                        ClientAccepted clientAccepted = (ClientAccepted) message;
                        DistributedSocketPubSub.this.startWriterThread(clientAccepted);
                    } else if (message instanceof ClientRejected) {
                        ClientRejected clientRejected = (ClientRejected) message;
                        DistributedSocketPubSub.this.failedToStart(clientRejected);
                    } else {
                        LOGGER.log(Level.WARNING, "Unrecognized object type received: clientMachine={0}, messageClass={1}",
                                   DistributedSocketPubSub.this.machineId, message.getClass().getSimpleName());
                    }
                } catch (IOException e) {
                    if (e instanceof EOFException) {
                        LOGGER.log(Level.WARNING, "Socket reached end of stream, ending reader");
                        attemptRestart = true;
                        if (channel.isOpen()) {
                            closeQuietly(channel);
                        }        
                    } else if (SocketTransformer.isClosed(e)) {
                        LOGGER.log(Level.INFO, "Socket closed, ending reader: {0}", e.toString());
                    } else {
                        LOGGER.log(Level.WARNING,
                                   String.format("Socket exception: machine=%s", DistributedSocketPubSub.this.machineId),
                                   e);
                    }
                } catch (RuntimeException | Error e) {
                    LOGGER.log(Level.ERROR, "Unexpected exception: machine=" + DistributedSocketPubSub.this.machineId, e);
                } finally {
                    threadLocalRemoteRelayMessage.set(null);
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
                LOGGER.log(Level.ERROR, "Unable to restart DistributedSocketPubSub", e);
                return null;
            });
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Unable to restart DistributedSocketPubSub", e);
        }
    }
    
    /**
     * Publisher that forwards publish commands to the message server.
     */
    public final class DistributedPublisher extends Publisher {
        private boolean isDormant;
        private RelayFields remoteRelayFields;
        private List<DeferredMessage> messagesWhileDormant = new ArrayList<>();

        private DistributedPublisher(@Nonnull String topic, @Nonnull Class<?> publisherClass, RelayFields remoteRelayFields) {
            super(topic, publisherClass);
            this.isDormant = true;
            this.remoteRelayFields = remoteRelayFields;
        }
        
        void setRemoteRelayFields(RelayFields remoteRelayFields) {
            assert this.remoteRelayFields == null;
            this.remoteRelayFields = remoteRelayFields;
        }

        boolean isRemote() {
            return remoteRelayFields != null && !remoteRelayFields.getSourceMachineId().equals(DistributedSocketPubSub.this.machineId);
        }

        RelayFields getRemoteRelayFields() {
            return remoteRelayFields;
        }

        /**
         * Publish a message.
         * If the publisher is dormant then add the message to a queue to be published once the publisher goes live.
         */
        @Override
        public <T extends CloneableObject<?>> void publish(@Nonnull T message, RetentionPriority priority) {
            if (isDormant) {
                messagesWhileDormant.add(new DeferredMessage(message, priority));
            } else {
                super.publish(message, priority);
                if (threadLocalRemoteRelayMessage.get() == null) {
                    // this DistributedPubSub is publishing a new message
                    // so send it to the central server
                    DistributedSocketPubSub.this.messageWriter.publishMessage(getTopic(), message, priority);
                }
            }
        }
        
        private void setLive() {
            isDormant = false;
            for (var message : messagesWhileDormant) {
                publish(message.message, message.retentionPriority);
            }
            messagesWhileDormant.clear();
        }
    }

    private static class DeferredMessage {
        private final CloneableObject<?> message;
        private final RetentionPriority retentionPriority;
        
        DeferredMessage(CloneableObject<?> message, RetentionPriority retentionPriority) {
            this.message = message;
            this.retentionPriority = retentionPriority;
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
        var relayFields = Optional.ofNullable(threadLocalRemoteRelayMessage.get()).map(RelayMessageBase::getRelayFields).orElse(null);
        return new DistributedPublisher(topic, publisherClass, relayFields);
    }

    /**
     * If this machine has created a publisher then register a new publisher as dormant.
     * Upon receiving the PublisherCreated message, make the publisher live by calling the base class's protected addPublisher command.
     * Otherwise call the base class's protected addPublisher command right away.
     */
    @Override
    protected void registerPublisher(Publisher publisher) {
        var info = dormantInfoMap.computeIfAbsent(publisher.getTopic(), topic -> new DormantInfo());
        if (info != null && info.dormantPublisher != null) {
            throw new IllegalStateException("publisher already exists: topic=" + publisher.getTopic());
        }
        info.setDormantPublisher((DistributedPublisher) publisher);
        var relayFields = Optional.ofNullable(threadLocalRemoteRelayMessage.get()).map(RelayMessageBase::getRelayFields).orElse(null);
        if (relayFields == null) {
            // this DistributedPubSub is creating a brand new publisher
            // so send it to the central server
            messageWriter.createPublisher(publisher.getCreatedAtTimestamp(), publisher.getTopic(), publisher.getPublisherClass(), null, /*isResend*/ false);
        } else {
            info.makeDormantPublisherLive();
        }
    }
    
    /**
     * Add the new subscriber as dormant and send a message to the central server about this subscriber.
     * Later on, the server will send a SubscriberAdded command and the code will make the subscriber live.
     * 
     * @param publisher the publisher to add the subscriber too if the publisher is live, or null if the publisher is dormant or does not exist
     * @param subscriber the subscriber to register
     * @param deferred false when subscriber is first registered, true when the subscriber is registered after a CreatePublisher command comes down from the server
     */
    @Override
    protected void registerSubscriber(@Nullable Publisher publisher, Subscriber subscriber, boolean deferred) {
        if (!deferred) {
            DistributedSubscriber distributedSubscriber = (DistributedSubscriber) subscriber;
            var info = dormantInfoMap.computeIfAbsent(subscriber.getTopic(), topic -> new DormantInfo());
            info.addDormantSubscriber(distributedSubscriber);
            messageWriter.addSubscriber(subscriber.getCreatedAtTimestamp(), subscriber.getTopic(),subscriber.getSubscriberName(), /*isResend*/ false);
        }
    }
    
    private class DormantInfo {
        private DistributedPublisher dormantPublisher;
        private final Map<String /*subscriberName*/, DistributedSubscriber> dormantSubscribers = new HashMap<>();
        private final List<DistributedSubscriber> confirmedSubscribers = new ArrayList<>(); // subscribers which are dormant but for which we received the SubscriberAdded confirmation
        
        DormantInfo() {
        }
        
        void setDormantPublisher(DistributedPublisher publisher) {
            assert this.dormantPublisher == null;
            this.dormantPublisher = publisher;
        }
        
        /**
         * Add subscriber to the dormant subscriber collection.
         * 
         * @return true if subscriber added, false if one with the same name already exists.
         */
        boolean addDormantSubscriber(DistributedSubscriber subscriber) {
            var existingSubscriber = dormantSubscribers.putIfAbsent(subscriber.getSubscriberName(), subscriber);
            return existingSubscriber == null;
        }
        
        DistributedPublisher makeDormantPublisherLive() {
            var publisher = dormantPublisher;
            dormantPublisher = null;
            publisher.setLive();
            DistributedSocketPubSub.this.addPublisher(publisher);
            return publisher;
        }
        
        /**
         * Called when the client receives a SubscriberAdded command.
         * Attempt to make the subscriber live.
         * 
         * @param publisher the publisher this subscriber belongs too, null if publisher is dormant
         * @param subscriberName the name of the dormant subscriber
         * @return status to be used in logs
         */
        String makeDormantSubscriberLive(@Nullable DistributedPublisher publisher, String subscriberName) {
            DistributedSubscriber subscriber = Objects.requireNonNull(dormantSubscribers.remove(subscriberName));
            if (publisher != null) {
                // use case: live publisher exists, subscriber added to it and server sends SubscriberAdded
                publisher.addSubscriber(subscriber);
                return "Added";
            } else if (dormantPublisher != null) {
                // use case: client just created a brand new publisher and not yet confirmed by server,
                // and client adds a subscriber to this publisher and server sends a SubscriberAdded
                dormantPublisher.addSubscriber(subscriber);
                return "AddedToDormantPublisher";
            } else {
                // use case: remote client subscribes and just received SubscriberAdded, but the CreatePublisher has not come down yet 
                confirmedSubscribers.add(subscriber);
                return "ActivateLater";
            }
        }
        
        int addConfirmedSubscribers(DistributedPublisher publisher) {
            int numConfirmedSubscribers = confirmedSubscribers.size();
            for (var subscriber : confirmedSubscribers) {
                publisher.addSubscriber(subscriber);
            }
            confirmedSubscribers.clear();
            return numConfirmedSubscribers;
        }
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
            if (publisher != null) {
                return CompletableFuture.completedFuture(publisher);
            } else {
                return fetchPublisherMap.computeIfAbsent(topic, t -> {
                    messageWriter.addFetchPublisher(t);
                    return new CompletableFuture<>();
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
    
    @Override
    public DistributedPublisher getPublisher(@Nonnull String topic) {
        return (DistributedPublisher) super.getPublisher(topic);
    }
    
    /**
     * Resolve any futures waiting for this publisher to be created.
     */
    @Override
    protected void onPublisherAdded(Publisher publisher) {
        resolveFetchPublisher(publisher);
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
     * Override this function to do something when sending a message failed.
     * For example, the unit tests override this to record the failures.
     */
    protected void onSendMessageFailed(MessageBase message, IOException e) {
    }


    /**
     * Cleanup this class. Close the socket channel and shutdown the executor.
     */
    private static class Cleanup extends CallStackCapturing implements Runnable {
        private final ClientMachineId machineId;
        private final AtomicReference<SocketChannel> channelHolder;
        private final ExecutorService channelExecutor;
        private final ExecutorService retryExecutor;

        private Cleanup(ClientMachineId machineId, AtomicReference<SocketChannel> channelHolder, ExecutorService channelExecutor, ExecutorService retryExecutor) {
            this.machineId = machineId;
            this.channelHolder = channelHolder;
            this.channelExecutor = channelExecutor;
            this.retryExecutor = retryExecutor;
        }

        @Override
        public void run() {
            LOGGER.log(Level.INFO, "Shutting down " + DistributedSocketPubSub.class.getSimpleName()
                    + ": clientId=" + machineId + ", clientAddress=" + getLocalAddress(channelHolder.get()));
            LOGGER.log(Level.TRACE, "Call stack at creation:" + getCallStack());
            closeQuietly(channelHolder.get());
            closeExecutorQuietly(channelExecutor);
            closeExecutorQuietly(retryExecutor);
        }
    }
}
