package org.sn.myutils.pubsub;

import static org.sn.myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static org.sn.myutils.pubsub.PubSubUtils.closeQuietly;
import static org.sn.myutils.pubsub.PubSubUtils.computeExponentialBackoff;
import static org.sn.myutils.pubsub.PubSubUtils.getLocalAddress;
import static org.sn.myutils.pubsub.PubSubUtils.getRemoteAddress;
import static org.sn.myutils.util.ExceptionUtils.unwrapCompletionException;
import static org.sn.myutils.util.concurrent.MoreExecutors.createThreadFactory;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serial;
import java.lang.System.Logger.Level;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.SocketAddress;
import java.nio.channels.AlreadyConnectedException;
import java.nio.channels.NetworkChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.NotThreadSafe;
import org.sn.myutils.annotations.Nullable;
import org.sn.myutils.pubsub.MessageClasses.AddSubscriber;
import org.sn.myutils.pubsub.MessageClasses.AddSubscriberFailed;
import org.sn.myutils.pubsub.MessageClasses.ClientAccepted;
import org.sn.myutils.pubsub.MessageClasses.ClientRejected;
import org.sn.myutils.pubsub.MessageClasses.CreatePublisher;
import org.sn.myutils.pubsub.MessageClasses.CreatePublisherFailed;
import org.sn.myutils.pubsub.MessageClasses.DownloadPublishedMessages;
import org.sn.myutils.pubsub.MessageClasses.DownloadPublishedMessagesByClientTimestamp;
import org.sn.myutils.pubsub.MessageClasses.DownloadPublishedMessagesByServerId;
import org.sn.myutils.pubsub.MessageClasses.FetchPublisher;
import org.sn.myutils.pubsub.MessageClasses.Identification;
import org.sn.myutils.pubsub.MessageClasses.InvalidRelayMessage;
import org.sn.myutils.pubsub.MessageClasses.MessageBase;
import org.sn.myutils.pubsub.MessageClasses.MessageWrapper;
import org.sn.myutils.pubsub.MessageClasses.PublishMessage;
import org.sn.myutils.pubsub.MessageClasses.PublisherCreated;
import org.sn.myutils.pubsub.MessageClasses.RelayFields;
import org.sn.myutils.pubsub.MessageClasses.RelayMessageBase;
import org.sn.myutils.pubsub.MessageClasses.RemoveSubscriber;
import org.sn.myutils.pubsub.MessageClasses.RemoveSubscriberFailed;
import org.sn.myutils.pubsub.MessageClasses.SubscriberAdded;
import org.sn.myutils.pubsub.MessageClasses.SubscriberRemoved;
import org.sn.myutils.pubsub.MessageClasses.TopicMessageBase;

/**
 * Client class that acts as an in-memory publish/subscribe system, as well as talks to a server to send and receive publish commands.
 * 
 * <p>This client class supports sharding. Users provide a messageServerLookup that maps a topic to a message server.
 * 
 * <p>When a DistributedPubSub is started it connects to the DistributedMessageServer, and identifies itself to the server.
 * The identification includes the client's name, and must be unique across all machines in the system.
 * If the DistributedMessageServer is not available, this class attempts to connect to the DistributedMessageServer again which capped exponential backoff.
 * 
 * <p>Since this class inherits from PubSub, it also implements the in-memory publish/subscribe system.
 * When a client becomes a publisher, it sends its publisher to the central server.
 * When another client subscribes, the central server sends the publisher over to this other client.
 * So it's as if each client has a replica of the in-memory publish-subscribe.
 * When the first client publishes a message, it gets relayed to the other client, and it's as if someone called pubsub.publish on the other client.
 * 
 * <p>When a user calls createPublisher, the new publisher is created as dormant, and it cannot be used to publish messages.
 * A call to pubsub.getPublisher will not find the publisher.
 * The server may verify if the client passed in security credentials that allow them to subscribe to a topic, so the server may send back a CreatePublisherInvalid command.
 * Upon receiving the confirmation message PublisherCreated from the server, the publisher is made live.
 *
 * <p>When a user calls subscribe, the DistributedPubSub sends this command to the server,
 * and the server responds with a SubscriberAdded and a CreatePublisher command, or AddSubscriberInvalid if the security credentials are not valid.
 * When a user calls `publisher.publish`, the DistributedPubSub sends this command to the server, which relays the command to all clients subscribed to the topic.
 * The DistributedPubSub also listens for messages sent from the server, which are messages relayed to it by other clients.
 * Upon receiving a message, the DistributedPubSub calls createPublisher or publisher.publish.
 * 
 * <p>This class does not provide security credentials support out of the box, but implementors of derived classes can add in this functionality via custom properties.
 * 
 * <p>Besides the identification message, each message sent to the server includes the client time and a monotonically increasing index.
 *
 * <p>In implementation,
 * there is one thread per message server that listens for messages from the server,
 * and one thread that sends messages to the all servers,
 * and another that handles retries with exponential backoff.
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
    private static final int MAX_RETRIES_FOR_SEND = 3;
    private static final int MAX_RETRIES_FOR_RECONNECT = 4;

    private final Map<String /*topic*/, DormantInfo> dormantInfoMap = new ConcurrentHashMap<>();
    private final SocketTransformer socketTransformer;
    private final KeyToSocketAddressMapper messageServerLookup;
    private final ClientMachineId machineId;
    private final Map<SocketAddress /*messageServer*/, SocketChannel> messageServers;
    private final Consumer<NetworkChannel> onBeforeBindChannel;
    private final ExecutorService channelExecutor = Executors.newCachedThreadPool(createThreadFactory("DistributedSocketPubSub", true)); // one write thread, multiple read threads
    private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, createThreadFactory("DistributedSocketPubSub.Retry", true));
    private final MessageWriter messageWriter;
    private final AtomicLong localClientIndex = new AtomicLong();
    private final AtomicInteger localCommandIndex = new AtomicInteger();
    private final Map<String /*topic*/, CompletableFuture<Publisher>> fetchPublisherMap = new HashMap<>();
    private final MessageServerConnectionListener messageServerConnectionListener = new MessageServerConnectionListener();

    /**
     * Create an in-memory publish/subscribe system and also talk to a central
     * server to send and receive publish commands.
     *
     * @param baseArgs the arguments for the in-memory pubsub system
     * @param machineId the name of this machine, and if null the code will set it to this machine's hostname
     * @param messageServerLookup a function that maps the topic to a message server (used for sharding) and generates a local host/port
     * @throws IOException if there is an error opening the socket channel
     * @see PubSubConstructorArgs for the arguments to the super class
     */
    public static DistributedSocketPubSub create(PubSubConstructorArgs baseArgs,
                                                 @Nullable String machineId,
                                                 KeyToSocketAddressMapper messageServerLookup) throws IOException {
        return create(baseArgs, machineId, messageServerLookup, new SocketTransformer(), ignored -> { });
    }

    static DistributedSocketPubSub create(PubSubConstructorArgs baseArgs,
                                          @Nullable String machineId,
                                          KeyToSocketAddressMapper messageServerLookup,
                                          SocketTransformer socketTransformer,
                                          Consumer<NetworkChannel> onBeforeBindChannel) throws IOException {
        var pubsub = new DistributedSocketPubSub(baseArgs, machineId, messageServerLookup, socketTransformer, onBeforeBindChannel);
        pubsub.registerCleanable();
        return pubsub;
    }

    // package private for tests
    DistributedSocketPubSub(PubSubConstructorArgs baseArgs,
                            @Nullable String machineId,
                            KeyToSocketAddressMapper messageServerLookup,
                            SocketTransformer socketTransformer,
                            Consumer<NetworkChannel> onBeforeBindChannel) throws IOException {
        super(baseArgs);
        this.socketTransformer = socketTransformer;
        this.messageServerLookup = messageServerLookup;
        this.machineId = new ClientMachineId(machineId != null ? machineId : InetAddress.getLocalHost().getHostName());
        this.messageServers = createNewSockets(messageServerLookup, onBeforeBindChannel);
        this.onBeforeBindChannel = onBeforeBindChannel;
        this.messageWriter = createMessageWriter();
    }

    @Override
    protected Runnable shutdownAction() {
        Runnable baseRunnable = super.shutdownAction();
        return () -> {
            Collection<SocketChannel> socketChannels = messageServers.values();
            LOGGER.log(Level.INFO, "Details: clientId={0}, sockets={1}",
                       machineId,
                       socketChannels.stream()
                                     .map(channel -> getLocalAddress(channel) + " -> " + getRemoteAddress(channel))
                                     .toList());
            socketChannels.forEach(PubSubUtils::closeQuietly);
            closeExecutorQuietly(channelExecutor);
            closeExecutorQuietly(retryExecutor);

            baseRunnable.run();
        };
    }

    /**
     * Create a socket for each message server.
     * Each socket is bound to a generated local address and to the remote address of the server.
     */
    private static Map<SocketAddress, SocketChannel> createNewSockets(KeyToSocketAddressMapper messageServerLookup,
                                                                      Consumer<NetworkChannel> onBeforeBindChannel)
            throws IOException {
        Map<SocketAddress, SocketChannel> messageServers = new HashMap<>();
        for (SocketAddress messageServer : messageServerLookup.getRemoteUniverse()) {
            var localAddress = messageServerLookup.getLocalAddress(messageServer);
            SocketChannel socketChannel = createNewSocket(localAddress, onBeforeBindChannel);
            messageServers.put(messageServer, socketChannel);
        }
        assert messageServers.size() == messageServerLookup.getRemoteUniverse().size();
        return messageServers;
    }
    
    private static SocketChannel createNewSocket(SocketAddress localAddress, Consumer<NetworkChannel> onBeforeBindChannel) throws IOException {
        var channel = SocketChannel.open();
        onBeforeBindChannel.accept(channel);
        channel.bind(localAddress);
        return channel;
    }

    private MessageWriter createMessageWriter() {
        return new MessageWriter();
    }
    
    public @NotNull ClientMachineId getMachineId() {
        return machineId;
    }

    /**
     * Start the message client asynchronously by connecting to all the message servers and starting all threads.
     * Returns a future that is resolved when everything starts, or rejected with StartException if anything fails.
     * If the server is not available, retries connecting to the server with exponential backoff starting at 1 second, 2 seconds, 4 seconds, 8 seconds, 8 seconds.
     * If there was another IOException in starting the future is rejected with a StartException.
     * 
     * <p>This function is @NotThreadSafe.
     * 
     * @throws java.util.concurrent.RejectedExecutionException if client was shutdown
     */
    @NotThreadSafe
    public CompletableFuture<Void> startAsync() {
        startWriterThreadIfNotStarted();
        return doStartAllAsync(false);
    }
    
    public static class StartException extends PubSubException {
        @Serial
        private static final long serialVersionUID = 1L;
        
        @SuppressWarnings("serial")
        private final Map<SocketAddress, Throwable> exceptions;

        public StartException(String error, Map<SocketAddress, Throwable> exceptions) {
            super(error);
            this.exceptions = Collections.unmodifiableMap(exceptions);
        }

        public Map<SocketAddress, Throwable> getExceptions() {
            return exceptions;
        }
    }
    
    private void startWriterThreadIfNotStarted() {
        if (messageWriter.isStarted()) {
            return;
        }
        messageWriter.setStarted();
        channelExecutor.submit(messageWriter);
        LOGGER.log(Level.INFO, "Start DistributedSocketPubSub writer");
    }
    
    private CompletableFuture<Void> doStartAllAsync(boolean sendAllPublishersAndSubscribers) {
        var future = messageServerConnectionListener.setAll(sendAllPublishersAndSubscribers);
        for (SocketAddress messageServer : messageServers.keySet()) {
            doStartOneAsync(messageServer);
        }
        return future;
    }
    
    private void doStartOneAsync(SocketAddress messageServer) {
        String snippet = String.format("DistributedSocketPubSub: clientMachine=%s, centralServer=%s",
                                       machineId,
                                       messageServer.toString());
        messageServerConnectionListener.registerFuture(messageServer);
        try {
            retryExecutor.submit(() -> doStartOne(messageServer, snippet, 0));
        } catch (RejectedExecutionException e) {
            messageServerConnectionListener.completeFutureExceptionally(messageServer, unwrapCompletionException(e));
            throw e;
        } catch (RuntimeException | Error e) {
            LOGGER.log(Level.ERROR, String.format("Failed to start %s", snippet), e);
            messageServerConnectionListener.completeFutureExceptionally(messageServer, unwrapCompletionException(e));
        }
    }
    
    private void doStartOne(SocketAddress messageServer, String snippet, int retry) {
        var channel = getInternalSocketChannel(messageServer);
        try {
            if (channel.isConnected()) {
                throw new AlreadyConnectedException();
            }
            var localAddress = channel.getLocalAddress();
            try {
                channel.connect(messageServer);
                LOGGER.log(Level.INFO, String.format("Connected %s, localAddress=%s remoteAddress=%s", snippet, getLocalAddress(channel), getRemoteAddress(channel)));
                messageWriter.sendIdentificationNow(messageServer);
                channelExecutor.submit(new MessageReader(messageServer));
            } catch (ConnectException e) {
                int nextRetry = retry + 1;
                long delayMillis = getTimeBeforeAttemptingToReconnect(nextRetry);
                replaceInternalSocketChannel(messageServer, createNewSocket(localAddress, onBeforeBindChannel));
                LOGGER.log(Level.INFO, String.format("Failed to connect %s. Retrying in %d millis...", snippet, delayMillis));
                retryExecutor.schedule(() -> doStartOne(messageServer, snippet, nextRetry), delayMillis, TimeUnit.MILLISECONDS);
            }
        } catch (IOException | RuntimeException | Error e) {
            messageServerConnectionListener.completeFutureExceptionally(messageServer, unwrapCompletionException(e));
        }
    }


    protected static final long getMaxTimeBeforeAttemptingToReconnect() {
        return getTimeBeforeAttemptingToReconnect(MAX_RETRIES_FOR_RECONNECT);
    }

    protected static final long getMinTimeBeforeAttemptingToReconnect() {
        return getTimeBeforeAttemptingToReconnect(1);
    }

    private static long getTimeBeforeAttemptingToReconnect(int retryNumber) {
        return computeExponentialBackoff(1000, retryNumber, MAX_RETRIES_FOR_RECONNECT);
    }

    /**
     * This class is about things to happen when the client is started and receives the ClientAccepted message.
     */
    private static class MessageServerConnectionListener {
        private volatile CompletableFuture<Void> summaryStartFuture;
        private volatile boolean sendAllPublishersAndSubscribers;
        private final Set<SocketAddress> pendingFutures = new HashSet<>();
        private final Map<SocketAddress, Throwable> exceptions = new HashMap<>();
        
        CompletableFuture<Void> setAll(boolean sendAllPublishersAndSubscribers) {
            this.summaryStartFuture = new CompletableFuture<>();
            this.sendAllPublishersAndSubscribers = sendAllPublishersAndSubscribers;
            pendingFutures.clear();
            exceptions.clear();
            return summaryStartFuture;
        }
        
        boolean isSendAllPublishersAndSubscribers() {
            return sendAllPublishersAndSubscribers;
        }
        
        synchronized void registerFuture(SocketAddress messageServer) {
            pendingFutures.add(messageServer);
        }

        synchronized void completeFuture(SocketAddress messageServer) {
            internalComplete(messageServer);
        }
        
        synchronized void completeFutureExceptionally(SocketAddress messageServer, Throwable exception) {
            exceptions.put(messageServer, exception);
            internalComplete(messageServer);
        }
        
        private void internalComplete(SocketAddress messageServer) {
            pendingFutures.remove(messageServer);
            if (pendingFutures.isEmpty()) {
                if (exceptions.isEmpty()) {
                    summaryStartFuture.complete(null);
                } else {
                    summaryStartFuture.completeExceptionally(new StartException("Error connecting to message servers", exceptions));
                }
            }
        }
    }
    
    private void onMessageServerConnected(SocketAddress messageServer, ClientAccepted clientAccepted) {
        try {
            LOGGER.log(Level.INFO, "Connected: clientMachine={0}, messageServer={1}, clientAccepted={2}", machineId, messageServer, clientAccepted.toLoggingString());
            if (messageServerConnectionListener.isSendAllPublishersAndSubscribers()) {
                doSendAllPublishersAndSubscribers(messageServer);
            }
            messageWriter.queueSendDeferredMessages(messageServer);
            messageServerConnectionListener.completeFuture(messageServer);
        } catch (RuntimeException | Error e) {
            messageServerConnectionListener.completeFutureExceptionally(messageServer, e);
        }
    }
    
    private void onMessageServerFailedToConnect(SocketAddress messageServer, ClientRejected clientRejected) {
        messageServerConnectionListener.completeFutureExceptionally(messageServer, clientRejected.toException());
        try {
            var channel = getInternalSocketChannel(messageServer);
            PubSubUtils.closeQuietly(channel);
            var localAddress = messageServerLookup.getLocalAddress(messageServer);
            replaceInternalSocketChannel(messageServer, createNewSocket(localAddress, onBeforeBindChannel));
        } catch (IOException e) {
            LOGGER.log(Level.ERROR, "Failed to reset create new channel");
        }
    }
    
    private void doSendAllPublishersAndSubscribers(SocketAddress sendToMessageServer) {
        forEachPublisher(basePublisher -> {
            DistributedPublisher publisher = (DistributedPublisher) basePublisher;
            String topic = publisher.getTopic();
            SocketAddress messageServer = DistributedSocketPubSub.this.messageServerLookup.mapKeyToRemoteAddress(topic);
            if (messageServer == sendToMessageServer) {
                if (!publisher.isRemote()) {
                    messageWriter.queueSendCreatePublisher(publisher.getCreatedAtTimestamp(),
                                                           topic,
                                                           publisher.getPublisherClass(),
                                                           publisher.getRemoteRelayFields(),
                                                           /*isResend*/ true);
                }
                for (var subscriber : publisher.getSubscribers()) {
                    messageWriter.queueSendAddSubscriber(subscriber.getCreatedAtTimestamp(),
                                                         topic,
                                                         subscriber.getSubscriberName(),
                                                         /*isResend*/ true);
                }
            }
        });
    }
    
    /**
     * Return the socket channel that points to the given message server.
     */
    private SocketChannel getInternalSocketChannel(SocketAddress messageServer) {
        return messageServers.get(messageServer);
    }

    /**
     * Replace the socket channel that points to the given message server.
     */
    private void replaceInternalSocketChannel(SocketAddress messageServer, SocketChannel channel) {
        messageServers.replace(messageServer, channel);
    }

    /**
     * Download as many messages starting with startIndex from the server.
     * The server does not retain messages forever, so it may not find the oldest messages.
     * 
     * @param topics the topics to download
     * @param startIndexInclusive the start index. Use ServerIndex.MIN_VALUE for no minimum.
     * @param endIndexInclusive the end index. Use ServerIndex.MAX_VALUE for no maximum.
     * @see RetentionPriority
     */
    public void downloadByServerId(Collection<String> topics, @NotNull ServerIndex startIndexInclusive, @NotNull ServerIndex endIndexInclusive) {
        messageWriter.queueSendDownloadByServerId(topics, startIndexInclusive, endIndexInclusive);
    }

    /**
     * Download as many messages starting with start from the server.
     * The server does not retain messages forever, so it may not find the oldest messages.
     *
     * @param topics the topics to download
     * @param startInclusive the start client time. Use 0 for no minimum.
     * @param endInclusive the end client time. Use Long.MAX_VALUE for no maximum.
     * @see RetentionPriority
     */
    public void downloadByClientTimestamp(Collection<String> topics, long startInclusive, long endInclusive) {
        messageWriter.queueSendDownloadByClientTimestamp(topics, startInclusive, endInclusive);
    }

    private interface MessageWriterMessage {
    }
    
    /**
     * Class representing an action to send all deferred messages to the particular messageServer
     * as that particular messageServer is now connected.
     */
    private record SendDeferredMessages(SocketAddress messageServer) implements MessageWriterMessage {
    }

    /**
     * A message and the message server it must be sent to.
     */
    private record RegularMessage(MessageBase message, SocketAddress messageServer) implements MessageWriterMessage {
    }
    
    /**
     * Thread that writes messages to a message server.
     * 
     * <p>This class is thread safe as there is only one MessageWriter thread accessing member variable deferredQueues,
     * and member variable queue is inherently thread safe.
     */
    private class MessageWriter implements Runnable {
        private final BlockingQueue<MessageWriterMessage> queue = new LinkedBlockingQueue<>();
        private final Map<SocketAddress /*messageServer*/, Deque<RegularMessage>> deferredQueues = new HashMap<>(); // messages put off because messageServer is down
        private boolean started;

        private MessageWriter() {
        }

        private void setStarted() {
            this.started = true;
        }

        private boolean isStarted() {
            return started;
        }

        private void sendIdentificationNow(SocketAddress messageServer) {
            send(new RegularMessage(new Identification(machineId), messageServer));
        }

        @Override
        public void run() {
            while (true) {
                try {
                    MessageWriterMessage messageWriterMessage = queue.take();
                    if (messageWriterMessage instanceof SendDeferredMessages sendDeferredMessages) {
                        handleSendDeferredMessages(sendDeferredMessages);
                    } else if (messageWriterMessage instanceof RegularMessage regularMessage) {
                        handleSendRegularMessage(regularMessage);
                    } else {
                        throw new UnsupportedOperationException();
                    }
                } catch (InterruptedException ignored) {
                    LOGGER.log(Level.INFO, "MessageWriter interrupted: machine={0}", machineId);
                    break;
                } catch (RuntimeException | Error e) {
                    LOGGER.log(Level.ERROR, "Unexpected exception: machine=" + machineId, e);
                }
            }
        }
        
        private void handleSendDeferredMessages(SendDeferredMessages sendDeferredMessages) {
            Deque<RegularMessage> deferredMessages = deferredQueues.remove(sendDeferredMessages.messageServer());
            LOGGER.log(Level.INFO, "Send deferred messages for machine={0} numMessages={1}",
                       machineId,
                       deferredMessages != null ? deferredMessages.size() : 0);
            if (deferredMessages != null) {
                for (var deferredMessage : deferredMessages) {
                    send(deferredMessage);
                }
            }
        }
        
        private void handleSendRegularMessage(RegularMessage regularMessage) {
            var messageServer = regularMessage.messageServer;
            var channel = getInternalSocketChannel(messageServer);
            if (channel.isConnected()) {
                send(regularMessage);
            } else {
                var deferredMessages = deferredQueues.computeIfAbsent(messageServer, ignored -> new LinkedList<>());
                deferredMessages.add(regularMessage);
            }
        }

        private CompletableFuture<Void> send(RegularMessage destinationMessage) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            send(future, destinationMessage, 0);
            return future;
        }
        
        private void send(CompletableFuture<Void> future, RegularMessage destinationMessage, int retry) {
            var message = destinationMessage.message;
            var channel = getInternalSocketChannel(destinationMessage.messageServer);
            LOGGER.log(
                    Level.TRACE,
                    () -> String.format("Sending message to server: clientAddress=%s, clientMachine=%s, messageServer=%s, %s",
                                        getLocalAddress(channel),
                                        machineId,
                                        destinationMessage.messageServer,
                                        destinationMessage.message.toLoggingString()));
            try {
                DistributedSocketPubSub.this.onBeforeSendMessage(message);
                socketTransformer.writeMessageToSocket(message, Short.MAX_VALUE, channel);
                DistributedSocketPubSub.this.onMessageSent(message);
                future.complete(null);
            } catch (IOException e) {
                boolean retryDone = retry >= MAX_RETRIES_FOR_SEND || SocketTransformer.isClosed(e);
                Level level = retryDone ? Level.WARNING : Level.DEBUG;
                LOGGER.log(level,
                           String.format("Send message failed: localAddress=%s, machine=%s, retry=%d, retryDone=%b",
                                         getLocalAddress(channel), machineId, retry, retryDone),
                           e);
                if (!retryDone) {
                    int nextRetry = retry + 1;
                    long delayMillis = computeExponentialBackoff(1000, nextRetry, MAX_RETRIES_FOR_SEND);
                    retryExecutor.schedule(() -> send(future, destinationMessage, nextRetry), delayMillis, TimeUnit.MILLISECONDS);
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

        void queueSendAddSubscriber(long createdAtTimestamp, @NotNull String topic, @NotNull String subscriberName, boolean isResend) {
            var addSubscriber = new AddSubscriber(createdAtTimestamp, topic, subscriberName, localCommandIndex.incrementAndGet(), true, isResend);
            Map<String, String> customProperties = new LinkedHashMap<>();
            DistributedSocketPubSub.this.addCustomPropertiesForAddSubscriber(customProperties, topic, subscriberName);
            addSubscriber.setCustomProperties(customProperties);
            internalPutMessage(addSubscriber);
        }

        void queueSendRemoveSubscriber(@NotNull String topic, @NotNull String subscriberName) {
            var removeSubscriber = new RemoveSubscriber(topic, subscriberName, localCommandIndex.incrementAndGet());
            Map<String, String> customProperties = new LinkedHashMap<>();
            DistributedSocketPubSub.this.addCustomPropertiesForRemoveSubscriber(customProperties, topic, subscriberName);
            removeSubscriber.setCustomProperties(customProperties);
            internalPutMessage(removeSubscriber);
        }

        void queueSendCreatePublisher(long createdAtTimestamp, @NotNull String topic, @NotNull Class<?> publisherClass, RelayFields relayFields, boolean isResend) {
            var createPublisher = new CreatePublisher(createdAtTimestamp, localClientIndex.incrementAndGet(), topic, publisherClass, isResend);
            createPublisher.setRelayFields(relayFields);
            Map<String, String> customProperties = new LinkedHashMap<>();
            DistributedSocketPubSub.this.addCustomPropertiesForCreatePublisher(customProperties, topic);
            createPublisher.setCustomProperties(customProperties);
            internalPutMessage(createPublisher);
        }

        void queueSendPublishMessage(@NotNull String topic, @NotNull CloneableObject<?> message, RetentionPriority priority) {
            var publishMessage = new PublishMessage(localClientIndex.incrementAndGet(), topic, message, priority);
            internalPutMessage(publishMessage);
        }

        void queueSendDownloadByServerId(Collection<String> topics, ServerIndex startIndexInclusive, ServerIndex endIndexInclusive) {
            internalPutDownloadMessages(new DownloadPublishedMessagesByServerId(localCommandIndex.incrementAndGet(), topics, startIndexInclusive, endIndexInclusive));
        }

        void queueSendDownloadByClientTimestamp(Collection<String> topics, long startInclusive, long endInclusive) {
            internalPutDownloadMessages(new DownloadPublishedMessagesByClientTimestamp(localCommandIndex.incrementAndGet(), topics, startInclusive, endInclusive));
        }

        void queueSendFetchPublisher(String topic) {
            internalPutMessage(new FetchPublisher(topic, localCommandIndex.incrementAndGet()));
        }
        
        void queueSendDeferredMessages(SocketAddress messageServer) {
            internalPutMessageWriterMessage(new SendDeferredMessages(messageServer));
        }

        /**
         * See comment in MessageClasses for more context.
         *
         * @see DownloadPublishedMessagesByServerId#cloneTo(Collection)
         * @see DownloadPublishedMessagesByClientTimestamp#cloneTo(Collection)
         */
        private <DownloadType extends DownloadPublishedMessages<DownloadType>> void internalPutDownloadMessages(DownloadType message) {
            Map<SocketAddress, List<String /*topic*/>> messageServers =
                    message.getTopics()
                           .stream()
                           .collect(Collectors.groupingBy(DistributedSocketPubSub.this.messageServerLookup::mapKeyToRemoteAddress,
                                                          Collectors.mapping(Function.identity(), Collectors.toList())));
                           
            for (var entry : messageServers.entrySet()) {
                DownloadType newMessage = message.cloneTo(entry.getValue());
                try {
                    SocketAddress messageServer = entry.getKey();
                    queue.put(new RegularMessage(newMessage, messageServer));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        private void internalPutMessage(TopicMessageBase message) {
            var messageServer = DistributedSocketPubSub.this.messageServerLookup.mapKeyToRemoteAddress(message.getTopic());
            internalPutMessageWriterMessage(new RegularMessage(message, messageServer));
        }

        private void internalPutMessageWriterMessage(MessageWriterMessage message) {
            try {
                queue.put(message);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    /**
     * Thread that retrieves messages from the message server.
     */
    private class MessageReader implements Runnable {
        private final SocketAddress messageServer;
        
        private MessageReader(SocketAddress messageServer) {
            this.messageServer = messageServer;
        }

        @Override
        public void run() {
            boolean attemptRestart = false;
            
            var channel = getInternalSocketChannel(messageServer);
            while (channel.isConnected()) {
                try {
                    MessageWrapper wrapper = socketTransformer.readMessageFromSocket(channel);
                    LOGGER.log(
                        Level.TRACE,
                        () -> String.format("Received message from server: clientMachine=%s, %s",
                                            DistributedSocketPubSub.this.machineId,
                                            wrapper.toLoggingString()));
                    boolean ok = DistributedSocketPubSub.this.onMessageReceived(wrapper);
                    if (ok) {
                        processMessage(messageServer, wrapper.getMessage());
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
                    threadLocalRemoteRelayMessage.remove();
                }
            } // end while
            
            if (attemptRestart) {
                DistributedSocketPubSub.this.doRestart(messageServer);
            }
        }
    }
    
    private void processMessage(SocketAddress messageServer, MessageBase message) {
        switch (message) {
            case CreatePublisher createPublisher ->
                // we are receiving a CreatePublisher from the server in response to a client.subscribe
                handleCreatePublisher(createPublisher);
            case CreatePublisherFailed createPublisherFailed -> handleCreatePublisherFailed(createPublisherFailed);
            case PublisherCreated publisherCreated ->
                // we are receiving a PublisherCreated from the server in response to a client.createPublisher
                handlePublisherCreated(publisherCreated);
            case SubscriberAdded subscriberAdded -> handleSubscriberAdded(subscriberAdded);
            case AddSubscriberFailed addSubscriberFailed -> handleAddSubscriberFailed(addSubscriberFailed);
            case SubscriberRemoved subscriberRemoved -> handleSubscriberRemoved(subscriberRemoved);
            case RemoveSubscriberFailed removeSubscriberFailed -> handleRemoveSubscriberFailed(removeSubscriberFailed);
            case PublishMessage publishMessage -> {
                String topic = publishMessage.getTopic();
                threadLocalRemoteRelayMessage.set(publishMessage);
                Publisher publisher = DistributedSocketPubSub.this.getPublisher(topic);
                if (publisher == null) {
                    publisher = DistributedSocketPubSub.this.dormantInfoMap.get(topic).getDormantPublisher();
                }
                publisher.publish(publishMessage.getMessage());
            }
            case InvalidRelayMessage invalidRelayMessage -> LOGGER.log(Level.WARNING, invalidRelayMessage.getError());
            case ClientAccepted clientAccepted ->
                    DistributedSocketPubSub.this.onMessageServerConnected(messageServer, clientAccepted);
            case ClientRejected clientRejected ->
                    DistributedSocketPubSub.this.onMessageServerFailedToConnect(messageServer, clientRejected);
            default ->
                    LOGGER.log(Level.WARNING, "Unrecognized object type received: clientMachine={0}, messageClass={1}",
                            DistributedSocketPubSub.this.machineId, message.getClass().getSimpleName());
        }
    }

    private void doRestart(SocketAddress messageServer) {
        try {
            var localAddress = messageServerLookup.getLocalAddress(messageServer);
            replaceInternalSocketChannel(messageServer, createNewSocket(localAddress, onBeforeBindChannel));
            doStartAllAsync(true).exceptionally(e -> {
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
        private final List<DeferredMessage> messagesWhileDormant = new ArrayList<>();
        private volatile boolean invalid;
        private volatile boolean isDormant;
        private RelayFields remoteRelayFields;

        private DistributedPublisher(@NotNull String topic, @NotNull Class<?> publisherClass, RelayFields remoteRelayFields) {
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
        public <T extends CloneableObject<?>> void publish(@NotNull T message, RetentionPriority priority) {
            boolean isRemoteMessage = threadLocalRemoteRelayMessage.get() != null;
            doPublish(message, priority, isRemoteMessage);
        }
        
        private void doPublish(@NotNull CloneableObject<?> message, RetentionPriority priority, boolean isRemoteMessage) {
            if (isDormant) {
                messagesWhileDormant.add(new DeferredMessage(message, priority, isRemoteMessage));
            } else {
                super.publish(message, priority);
                if (!isRemoteMessage) {
                    // this DistributedPubSub is publishing a new message
                    // so send it to the central server
                    DistributedSocketPubSub.this.messageWriter.queueSendPublishMessage(getTopic(), message, priority);
                }
            }
        }
        
        private void setLive() {
            isDormant = false;
            for (var message : messagesWhileDormant) {
                doPublish(message.message, message.retentionPriority, message.isRemoteMessage);
            }
            messagesWhileDormant.clear();
        }

        public boolean isInvalid() {
            return invalid;
        }

        void markInvalid() {
            assert isDormant;
            invalid = true;
        }
    }

    private record DeferredMessage(CloneableObject<?> message,
                                   RetentionPriority retentionPriority,
                                   boolean isRemoteMessage) {
    }

    public final class DistributedSubscriber extends Subscriber {
        private volatile boolean invalid;
        
        private DistributedSubscriber(@NotNull String topic,
                                      @NotNull String subscriberName,
                                      @NotNull Class<? extends CloneableObject<?>> subscriberClass,
                                      @NotNull Consumer<CloneableObject<?>> callback) {
            super(topic, subscriberName, subscriberClass, callback);
        }

        public boolean isInvalid() {
            return invalid;
        }

        void markInvalid() {
            invalid = true;
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
     * Register a new publisher as dormant.
     * The publisher will be made live upon receiving a PublisherCreated or SubscriberAdded command if there are no more dormant subscribers.
     */
    @Override
    protected void registerPublisher(Publisher publisher) {
        var info = dormantInfoMap.computeIfAbsent(publisher.getTopic(), _ -> new DormantInfo());
        if (info.dormantPublisher != null) {
            throw new IllegalStateException("publisher already exists: topic=" + publisher.getTopic());
        }
        info.setDormantPublisher((DistributedPublisher) publisher);
        var relayFields = Optional.ofNullable(threadLocalRemoteRelayMessage.get()).map(RelayMessageBase::getRelayFields).orElse(null);
        if (relayFields == null) {
            // this DistributedPubSub is creating a brand-new publisher
            // so send it to the central server
            messageWriter.queueSendCreatePublisher(publisher.getCreatedAtTimestamp(), publisher.getTopic(), publisher.getPublisherClass(), null, /*isResend*/ false);
        } else {
            info.setReadyToGoLive();
        }
    }
    
    /**
     * Add the new subscriber as dormant and send a message to the central server about this subscriber.
     * Later on, the server will send a SubscriberAdded command and the code will make the subscriber live.
     * 
     * @param publisher the publisher to add the subscriber to if the publisher is live, or null if the publisher is dormant or does not exist
     * @param subscriber the subscriber to register
     * @param deferred false when subscriber is first registered, true when the subscriber is registered after a CreatePublisher command comes down from the server
     */
    @Override
    protected void registerSubscriber(@Nullable Publisher publisher, Subscriber subscriber, boolean deferred) {
        if (!deferred) {
            DistributedSubscriber distributedSubscriber = (DistributedSubscriber) subscriber;
            var info = dormantInfoMap.computeIfAbsent(subscriber.getTopic(), ignoredTopic -> new DormantInfo());
            info.addDormantSubscriber(distributedSubscriber);
            messageWriter.queueSendAddSubscriber(subscriber.getCreatedAtTimestamp(), subscriber.getTopic(),subscriber.getSubscriberName(), /*isResend*/ false);
        }
    }
    
    /**
     * Send a message to the central server to delete this subscriber.
     * Later on, the server will send a SubscriberRemoved command and the code will really remove the subscriber.
     * When all subscribers to the topic from this machine are removed, the central server will no longer send messages published to this topic to this machine. 
     * 
     * @param publisher the publisher to remove the subscriber from if the publisher is live, or null if the publisher is dormant or does not exist
     * @param subscriber the subscriber to unregister
     * @param isDeferred false when subscriber is first registered, true when the subscriber is registered after a CreatePublisher command comes down from the server
     */
    @Override
    protected void unregisterSubscriber(@Nullable Publisher publisher, Subscriber subscriber, boolean isDeferred) {
        if (!isDeferred) {
            messageWriter.queueSendRemoveSubscriber(subscriber.getTopic(),subscriber.getSubscriberName());
        }
    }
    
    private void handleCreatePublisher(CreatePublisher createPublisher) {
        threadLocalRemoteRelayMessage.set(createPublisher);
        String topic = createPublisher.getTopic();
        DistributedSocketPubSub.this.createPublisher(topic,
                                                     createPublisher.getPublisherClass());
        LOGGER.log(Level.TRACE, "Remote publisher created as dormant: clientMachine={0}, topic={1}",
                   DistributedSocketPubSub.this.machineId, topic);
        var dormantInfo = dormantInfoMap.get(topic);
        dormantInfo.tryToMakeDormantPublisherLive();
    }
    
    private void handleCreatePublisherFailed(CreatePublisherFailed createPublisherFailed) {
        String topic = createPublisherFailed.getTopic();
        LOGGER.log(Level.TRACE, "Create publisher failed: clientMachine={0}, topic={1}, error={2}",
                   DistributedSocketPubSub.this.machineId, topic, createPublisherFailed.getError());
        var dormantInfo = dormantInfoMap.get(topic);
        dormantInfo.markDormantPublisherAsInvalid();
        dormantInfoMap.remove(topic);
    }
    
    private void handlePublisherCreated(PublisherCreated publisherCreated) {
        String topic = publisherCreated.getTopic();
        LOGGER.log(Level.TRACE, "Confirm publisher created: clientMachine={0}, topic={1}",
                   DistributedSocketPubSub.this.machineId, topic);
        var dormantInfo = dormantInfoMap.get(topic);
        dormantInfo.setReadyToGoLive(publisherCreated.getRelayFields());
        dormantInfo.tryToMakeDormantPublisherLive();
    }
    
    private void handleSubscriberAdded(SubscriberAdded subscriberAdded) {
        String topic = subscriberAdded.getTopic();
        String subscriberName = subscriberAdded.getSubscriberName();
        var dormantInfo = DistributedSocketPubSub.this.dormantInfoMap.get(topic);
        var publisher = getPublisher(topic);
        var subscriberResult = dormantInfo.tryMakeDormantSubscriberLive(publisher, subscriberName);
        LOGGER.log(Level.TRACE, "Confirm subscriber added: clientMachine={0}, topic={1}, subscriberName={2}, subscriberResult={3}",
                   DistributedSocketPubSub.this.machineId, topic, subscriberName, subscriberResult);
        if (subscriberResult.shouldAttemptToMakePublisherLive()) {
            dormantInfo.tryToMakeDormantPublisherLive();
        }
    }
    
    private void handleAddSubscriberFailed(AddSubscriberFailed addSubscriberFailed) {
        String topic = addSubscriberFailed.getTopic();
        String subscriberName = addSubscriberFailed.getSubscriberName();
        LOGGER.log(Level.TRACE, "Add subscriber failed: clientMachine={0}, topic={1}, subscriberName={2}, error='{3}'",
                   DistributedSocketPubSub.this.machineId, topic, subscriberName, addSubscriberFailed.getError());
        var dormantInfo = DistributedSocketPubSub.this.dormantInfoMap.get(topic);
        Subscriber subscriber = dormantInfo.removeDormantSubscriber(subscriberName);
        unsubscribe(subscriber);
        var publisher = getPublisher(topic);
        if (publisher != null) {
            dormantInfo.tryToMakeDormantPublisherLive();
        }
    }
    
    private void handleSubscriberRemoved(SubscriberRemoved subscriberRemoved) {
        String topic = subscriberRemoved.getTopic();
        String subscriberName = subscriberRemoved.getSubscriberName();
        LOGGER.log(Level.TRACE, "Confirm subscriber removed: topic={0}, subscriberName={1}",
                   topic, subscriberName);
        super.basicUnsubscribe(topic, subscriberName, false);
        var dormantInfo = DistributedSocketPubSub.this.dormantInfoMap.get(topic);
        if (dormantInfo != null) {
            dormantInfo.removeDormantSubscriber(subscriberName);
            dormantInfo.tryToMakeDormantPublisherLive();
        }
    }

    private void handleRemoveSubscriberFailed(RemoveSubscriberFailed removeSubscriberFailed) {
        String topic = removeSubscriberFailed.getTopic();
        String subscriberName = removeSubscriberFailed.getSubscriberName();
        LOGGER.log(Level.TRACE, "Remove subscriber failed: clientMachine={0}, topic={1}, subscriberName={2}, error='{3}'",
                   DistributedSocketPubSub.this.machineId, topic, subscriberName, removeSubscriberFailed.getError());
    }
    
    private class DormantInfo {
        private DistributedPublisher dormantPublisher;
        private boolean readyToGoLive;
        private final Map<String /*subscriberName*/, DistributedSubscriber> dormantSubscribers = new HashMap<>();
        private final List<DistributedSubscriber> confirmedSubscribers = new ArrayList<>(); // subscribers which are dormant but for which we received SubscriberAdded
        
        DormantInfo() {
        }
        
        synchronized void setDormantPublisher(DistributedPublisher publisher) {
            assert this.dormantPublisher == null;
            this.dormantPublisher = publisher;
        }
        
        synchronized void markDormantPublisherAsInvalid() {
            assert this.dormantPublisher != null;
            dormantPublisher.markInvalid();
        }

        DistributedPublisher getDormantPublisher() {
            return dormantPublisher;
        }

        /**
         * Add subscriber to the dormant subscriber collection.
         * 
         * @return true if subscriber added, false if one with the same name already exists.
         */
        synchronized boolean addDormantSubscriber(DistributedSubscriber subscriber) {
            var existingSubscriber = dormantSubscribers.putIfAbsent(subscriber.getSubscriberName(), subscriber);
            return existingSubscriber == null;
        }
        
        /**
         * Remove a subscriber from the dormant subscriber collection.
         * Marks the subscriber as invalid, so that if anyone has a pointer to it, they will see it as invalid.
         */
        synchronized DistributedSubscriber removeDormantSubscriber(String subscriberName) {
            var existingSubscriber = dormantSubscribers.remove(subscriberName);
            if (existingSubscriber != null) {
                existingSubscriber.markInvalid();
            }
            return existingSubscriber;
        }
        
        /**
         * Set the publisher as ready to go live.
         * This is set upon receiving the PublisherCreated message.
         * Remote publishers are always created as live.
         */
        synchronized void setReadyToGoLive(@NotNull RelayFields relayFields) {
            assert dormantPublisher.getRemoteRelayFields() == null;
            dormantPublisher.setRemoteRelayFields(relayFields);
            readyToGoLive = true;
        }
        
        synchronized void setReadyToGoLive() {
            assert dormantPublisher.getRemoteRelayFields() != null;
            readyToGoLive = true;
        }
        
        /**
         * Make the dormant publisher live if is ready to go live and there are no dormant subscribers.
         */
        synchronized void tryToMakeDormantPublisherLive() {
            if (dormantPublisher == null) {
                return;
            }
            if (readyToGoLive && dormantSubscribers.isEmpty()) {
                var publisher = dormantPublisher;
                dormantPublisher = null;
                int numSubscribersMadeLive = addConfirmedSubscribersInternal(publisher);
                LOGGER.log(Level.TRACE, "Publisher live: clientMachine={0}, topic={1}, numSubscribersMadeLive={2}",
                           DistributedSocketPubSub.this.machineId, publisher.getTopic(), numSubscribersMadeLive);
                publisher.setLive();
                DistributedSocketPubSub.this.addPublisher(publisher);
            } else {
                LOGGER.log(Level.TRACE, "Publisher still dormant: clientMachine={0}, topic={1}, live=false",
                           DistributedSocketPubSub.this.machineId, dormantPublisher.getTopic());
            }
        }
        
        /**
         * Add the confirmed subscribers (those which received a SubscriberAdded response from the server) to the live publisher.
         */
        private int addConfirmedSubscribersInternal(DistributedPublisher publisher) {
            int numConfirmedSubscribers = confirmedSubscribers.size();
            for (var subscriber : confirmedSubscribers) {
                publisher.addSubscriber(subscriber);
            }
            confirmedSubscribers.clear();
            return numConfirmedSubscribers;
        }
        
        /**
         * Called when the client receives a SubscriberAdded command.
         * Try to make the subscriber live.
         * 
         * @param publisher the publisher this subscriber belongs too, null if publisher is dormant
         * @param subscriberName the name of the dormant subscriber
         * @return status to be used in logs
         */
        synchronized TryMakeDormantSubscriberLiveResult tryMakeDormantSubscriberLive(@Nullable DistributedPublisher publisher, String subscriberName) {
            DistributedSubscriber subscriber = Objects.requireNonNull(dormantSubscribers.remove(subscriberName));
            if (publisher != null) {
                // use case: live publisher exists, subscriber added to it and server sends SubscriberAdded
                publisher.addSubscriber(subscriber);
                return TryMakeDormantSubscriberLiveResult.ADDED;
            } else if (dormantPublisher != null) {
                // use case: client just created a brand-new publisher and not yet confirmed by server,
                // and client adds a subscriber to this publisher and server sends a SubscriberAdded
                dormantPublisher.addSubscriber(subscriber);
                return TryMakeDormantSubscriberLiveResult.ADDED_TO_DORMANT_PUBLISHER;
            } else {
                // use case: remote client subscribes and just received SubscriberAdded, but the CreatePublisher has not come down yet 
                confirmedSubscribers.add(subscriber);
                return TryMakeDormantSubscriberLiveResult.ACTIVATE_LATER;
            }
        } 
    }
    
    private enum TryMakeDormantSubscriberLiveResult {
        ADDED(false),
        ADDED_TO_DORMANT_PUBLISHER(true),
        ACTIVATE_LATER(true);
        
        private final boolean attemptToMakePublisherLive;
        
        TryMakeDormantSubscriberLiveResult(boolean attemptToMakePublisherLive) {
            this.attemptToMakePublisherLive = attemptToMakePublisherLive;
        }
        
        boolean shouldAttemptToMakePublisherLive() {
            return attemptToMakePublisherLive;
        }
    }

    @Override
    protected DistributedSubscriber newSubscriber(@NotNull String topic,
                                                  @NotNull String subscriberName,
                                                  @NotNull Class<? extends CloneableObject<?>> subscriberClass,
                                                  @NotNull Consumer<CloneableObject<?>> callback) {
        return new DistributedSubscriber(topic, subscriberName, subscriberClass, callback);
    }
    
    /**
     * Fetch the publisher from central server.
     */
    public final synchronized CompletableFuture<Publisher> fetchPublisher(@NotNull String topic) {
        var publisher = getPublisher(topic);
        if (publisher != null) {
            return CompletableFuture.completedFuture(publisher);
        } else {
            return fetchPublisherMap.computeIfAbsent(topic, t -> {
                messageWriter.queueSendFetchPublisher(t);
                return new CompletableFuture<>();
            });
        }
    }

    // the function that calls this, i.e. PubSub.createPublisher, is also synchronized
    private synchronized void resolveFetchPublisher(Publisher publisher) {
        var future = fetchPublisherMap.remove(publisher.getTopic());
        if (future != null) {
            future.complete(publisher);
        }
    }

    @Override
    public DistributedPublisher getPublisher(@NotNull String topic) {
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
     * Add custom properties to a create publisher command.
     * Derived classes may add a secret key. The implementor should override a corresponding function in the server class to verify the secret key.
     */
    protected void addCustomPropertiesForCreatePublisher(Map<String, String> customProperties, String topic) {
    }

    /**
     * Add custom properties to an add subscriber command.
     * Derived classes may add a secret key. The implementor should override a corresponding function in the server class to verify the secret key.
     */
    protected void addCustomPropertiesForAddSubscriber(Map<String, String> customProperties, String topic, String subscriberName) {
    }

    /**
     * Add custom properties to a remove subscriber command.
     * Derived classes may add a secret key. The implementor should override a corresponding function in the server class to verify the secret key.
     */
    protected void addCustomPropertiesForRemoveSubscriber(Map<String, String> customProperties, String topic, String subscriberName) {
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
     * 
     * @return true if this message is valid
     */
    protected boolean onMessageReceived(MessageWrapper wrapper) {
        return true;
    }
    
    /**
     * Override this function to do something when sending a message failed.
     * For example, the unit tests override this to record the failures.
     */
    protected void onSendMessageFailed(MessageBase message, IOException e) {
    }
}
