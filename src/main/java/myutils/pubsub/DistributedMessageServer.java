package myutils.pubsub;

import static myutils.pubsub.PubSubUtils.addShutdownHook;
import static myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static myutils.pubsub.PubSubUtils.closeQuietly;
import static myutils.pubsub.PubSubUtils.computeExponentialBackoff;
import static myutils.pubsub.PubSubUtils.extractIndex;
import static myutils.pubsub.PubSubUtils.getLocalAddress;
import static myutils.pubsub.PubSubUtils.getRemoteAddress;
import static myutils.util.concurrent.MoreExecutors.createThreadFactory;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.lang.ref.Cleaner.Cleanable;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import myutils.pubsub.MessageClasses.AddSubscriber;
import myutils.pubsub.MessageClasses.ClientGeneratedMessage;
import myutils.pubsub.MessageClasses.CreatePublisher;
import myutils.pubsub.MessageClasses.DownloadPublishedMessages;
import myutils.pubsub.MessageClasses.FetchPublisher;
import myutils.pubsub.MessageClasses.Identification;
import myutils.pubsub.MessageClasses.InvalidRelayMessage;
import myutils.pubsub.MessageClasses.MessageBase;
import myutils.pubsub.MessageClasses.PublishMessage;
import myutils.pubsub.MessageClasses.RelayFields;
import myutils.pubsub.MessageClasses.RelayMessageBase;
import myutils.pubsub.MessageClasses.RelayTopicMessageBase;
import myutils.pubsub.MessageClasses.RemoveSubscriber;
import myutils.pubsub.MessageClasses.RequestIdentification;
import myutils.pubsub.MessageClasses.Resendable;
import myutils.pubsub.PubSubUtils.CallStackCapturing;
import myutils.util.ZipMinIterator;


/**
 * Server class that receives messages from a client and relays it to all other clients.
 * When a client connects, they send an Identification message identifying their name, and the name must be unique.
 * A client can then send createPublisher and publisher.publish commands, and they will be relayed to other clients who are subscribed to the topic.
 * If sending a message to a client fails, it is retried with exponential backoff up to a maximum number of times.
 * 
 * <p>In implementation, there is one accept thread with listens for socket connections by calling asyncServerSocketChannel.accept().
 * Once a connection is available, we submit the channel to a pool of channel threads to read a message.
 * Reading happens asynchronously in a thread managed by the AsynchronousServerSocketChannel classes.
 * Upon receiving the message, we handle the message in the pool in channel threads.
 * We then resubmit the channel to the pool of channel threads to read a message.
 * There is another thread that handles retries with exponential backoff.
 * 
 * <p>When a client connects for the first time, they should send an Identification message, identifying their machine name.
 * If a second client connects with the same name, we log a warning and ignore the client.
 * We then add the client to our list of clients and set SO_KEEPALIVE to true.
 * 
 * <p>Thereafter clients may send createPublisher or publisher.add commands, and these will be relayed to all other clients.
 * Upon receiving a message to relay, the server generates a monotonically  increasing integer and sets the machineId of the machine which sent the message.
 * These are part of the message sent to each client who is subscribed to the topic.
 * 
 * <p>A note on infinite recursion: if server relays a message to client2, that client2 must not send that message back to the server
 * as in theory that would send the message back to client1.
 * However, using the field serverIndex, the server detects that it already processed the message and therefore ignores it.
 * But clients should still not send the message to avoid unnecessary network traffic.
 * 
 * <p>The server caches the last N messages of each RententionPriority.
 * Clients can download all publish message commands from a particular index, and all messages in the cache from this time up to the time of download
 * will be sent to that client.
 * 
 * <p>About messages sent between client and server if using a socket:
 * The first two bytes are the length of the message.
 * The next N bytes is the message, when serialized and converted to a byte stream.
 */
public class DistributedMessageServer implements Shutdowneable {
    private static final System.Logger LOGGER = System.getLogger(DistributedMessageServer.class.getName());
    private static final int NUM_CHANNEL_THREADS = 4;
    private static final int MAX_RETRIES = 3;

    private final String host;
    private final int port;
    private AsynchronousServerSocketChannel asyncServerSocketChannel;
    private final ExecutorService acceptExecutor;
    private final ExecutorService channelExecutor;
    private final ScheduledExecutorService retryExecutor;
    private final List<ClientMachine> clientMachines = new CopyOnWriteArrayList<>();
    private final AtomicLong maxMessage = new AtomicLong();
    private final PublishersAndSubscribers publishersAndSubscribers = new PublishersAndSubscribers();
    private final MostRecentMessages mostRecentMessages;
    private final Cleanable cleanable;
    
    
    /**
     * Class representing a remote machine.
     * Key fields are machineId (a string) and channel (an AsynchronousSocketChannel).
     */
    private static final class ClientMachine {
        private final String machineId;
        private final String remoteAddress;
        private final AsynchronousSocketChannel channel;
        private final WriteManager writeManager = new WriteManager();

        private ClientMachine(String machineId, AsynchronousSocketChannel channel) {
            this.machineId = machineId;
            this.remoteAddress = getRemoteAddress(channel);
            this.channel = channel;
        }

        private AsynchronousSocketChannel getChannel() {
            return channel;
        }
        
        private String getMachineId() {
            return machineId;
        }
        
        @Override
        public String toString() {
            return machineId + '@' + remoteAddress;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(machineId, remoteAddress);
        }
        
        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof ClientMachine)) {
                return false;
            }
            ClientMachine that = (ClientMachine) thatObject;
            return this.machineId.equals(that.machineId) && this.remoteAddress.equals(that.remoteAddress);
        }
        
        WriteManager getWriteManager() {
            return writeManager;
        }

        static class WriteManager {
            private final AtomicBoolean writeLock = new AtomicBoolean();
            private final Queue<MessageBase> writeQueue = new LinkedList<>();

            /**
             * Acquire a write lock on this channel.
             * But if it is not available, add the message to send to the write queue.
             */
            synchronized boolean acquireWriteLock(@Nonnull MessageBase message) {
                boolean acquired = writeLock.compareAndSet(false, true);
                if (!acquired) {
                    writeQueue.add(message);
                }
                return acquired;
            }

            /**
             * This function is called with the write lock held.
             * If the write queue is not empty, return the head of it and keep the write lock held.
             * If it is empty, release the lock.
             */
            @Nullable synchronized MessageBase returnHeadOfHeadQueueOrReleaseLock() {
                var nextMessage = writeQueue.poll();
                if (nextMessage == null) {
                    writeLock.set(false);            
                }
                return nextMessage;
            }
        }
    }
    
    /**
     * List of all publishers and subscribers in the system.
     */
    private static class PublishersAndSubscribers {
        private static class TopicInfo {
            private CreatePublisher createPublisher;
            private final Set<SubscriberEndpoint> subscriberEndpoints = new LinkedHashSet<>(); 
            private Set<ClientMachine> notifyClients; // clients to notify when a publisher is created
            
            private void setNotifyClientsToNullIfEmpty() {
                if (notifyClients != null && notifyClients.isEmpty()) {
                    notifyClients = null;
                }
            }
        }

        private final Map<String /*topic*/, TopicInfo> topicMap = new LinkedHashMap<>();
        
        /**
         * Add subscriber to this topic.
         * Note that the publisher may not yet be created.
         * 
         * @return the CreatePublisher if one exists and the client machine is not already subscribed 
         */
        synchronized @Nullable CreatePublisher addSubscriberEndpoint(String topic, String subscriberName, ClientMachine clientMachine) {
            TopicInfo info = topicMap.computeIfAbsent(topic, unused -> new TopicInfo());
            boolean clientMachineAlreadySubscribedToTopic = info.subscriberEndpoints.stream().anyMatch(endpoint -> endpoint.getClientMachine().equals(clientMachine));
            info.subscriberEndpoints.add(new SubscriberEndpoint(subscriberName, clientMachine));
            if (info.notifyClients != null) {
                info.notifyClients.removeIf(c -> c.equals(clientMachine));
                info.setNotifyClientsToNullIfEmpty();
            }
            if (info.createPublisher != null && !clientMachineAlreadySubscribedToTopic) {
                return info.createPublisher;
            } else {
                return null;
            }
        }

        public synchronized void removeSubscriberEndpoint(String topic, String subscriberName) {
            TopicInfo info = topicMap.computeIfAbsent(topic, unused -> new TopicInfo());
            info.subscriberEndpoints.removeIf(subscriberEndpoint -> subscriberEndpoint.getSubscriberName().equals(subscriberName));
        }
        
        /**
         * Add a client to notify upon the publisher getting created.
         * Does not add if there is already a subscriber and the publisher has not yet been created.
         *
         * @return the CreatePublisher if one exists
         */
        synchronized CreatePublisher maybeAddNotifyClient(String topic, ClientMachine clientMachine) {
            TopicInfo info = topicMap.computeIfAbsent(topic, unused -> new TopicInfo());
            if (info.createPublisher == null && info.subscriberEndpoints.stream().anyMatch(subscriberEndpoint -> subscriberEndpoint.getClientMachine().equals(clientMachine))) {
                // clientMachine is already subscribed to the topic (even though the publisher does not yet exist)
                // and will be notified anyway upon CreatePublisher
                // so no need to notify it again
                return null;
            }
            if (info.createPublisher != null) {
                return info.createPublisher;
            }
            if (info.notifyClients == null) {
                info.notifyClients = new LinkedHashSet<>();
            }
            info.notifyClients.add(clientMachine);
            return null;
        }
        
        /**
         * Add createPublisher command.
         * Used when a new client comes online and we have to relay the CreatePublisher command to them.
         * 
         * @throws IllegalArgumentException if the publisher already exists
         */
        synchronized void savePublisher(CreatePublisher createPublisher) {
            var topic = createPublisher.getTopic();
            TopicInfo info = topicMap.computeIfAbsent(topic, unused -> new TopicInfo());
            if (info.createPublisher != null) {
                throw new IllegalStateException(
                    "Publisher already exists: topic=" + topic
                        + ", publisherClass=" + info.createPublisher.getPublisherClass().getSimpleName()
                        + ", newPublisherClass=" + createPublisher.getPublisherClass().getSimpleName());
            }
            info.createPublisher = createPublisher;
        }

        /**
         * A channel has been closed.
         * Remove all subscriber endpoints and notify clients for this channel.
         */
        synchronized void removeAllClientMachines(AsynchronousSocketChannel channel) {
            for (var entry : topicMap.entrySet()) {
                TopicInfo info = entry.getValue();
                info.subscriberEndpoints.removeIf(subscriberEndpoint -> subscriberEndpoint.getClientMachine().getChannel() == channel);
                if (info.notifyClients != null) {
                    info.notifyClients.removeIf(clientMachine -> clientMachine.getChannel() == channel);
                    info.setNotifyClientsToNullIfEmpty();
                }
            }
        }

        /**
         * Return the client machines subscribed to this topic or who want a notification when a publisher is created.
         * Side effect of this function is to remove elements from the notifyClients collection.
         */
        synchronized Stream<ClientMachine> getClientsInterestedInTopic(String topic) {
            TopicInfo info = topicMap.get(topic);
            if (info == null) {
                return Stream.empty();
            }
            Stream<ClientMachine> subscribedClients = info.subscriberEndpoints.stream().map(SubscriberEndpoint::getClientMachine).distinct();
            Stream<ClientMachine> notifyClients = info.notifyClients != null ? info.notifyClients.stream() : Stream.empty();
            info.notifyClients = null;
            return Stream.concat(subscribedClients, notifyClients);
        }
    }
    
    /**
     * Class to what subscribers and client machine has.
     * Used in the map of topic to list of subscribers for that topic.
     * One client machine can have two subscribers for the same topic.
     */
    private static final class SubscriberEndpoint {
        private final String subscriberName;
        private final ClientMachine clientMachine;
        
        private SubscriberEndpoint(String subscriberName, ClientMachine clientMachine) {
            this.subscriberName = subscriberName;
            this.clientMachine = clientMachine;
        }
        
        private String getSubscriberName() {
            return subscriberName;
        }
        
        private ClientMachine getClientMachine() {
            return clientMachine;
        }
        
        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof SubscriberEndpoint)) {
                return false;
            }
            SubscriberEndpoint that = (SubscriberEndpoint) thatObject;
            return this.clientMachine.equals(that.clientMachine) && this.subscriberName.equals(that.subscriberName);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(clientMachine, subscriberName);
        }
    }

    private static class MostRecentMessages {
        private final int[] mostRecentMessagesToKeep;
        private final List<Deque<PublishMessage>> messages = List.of(new LinkedList<>(), new LinkedList<>());
        private final Map<ClientMachine, Map<String /*topic*/, Long /*maxIndex*/>> highestIndexMap = new HashMap<>();
        
        MostRecentMessages(Map<RetentionPriority, Integer> mostRecentMessagesToKeep) {
            this.mostRecentMessagesToKeep = computeMostRecentMessagesToKeep(mostRecentMessagesToKeep);
        }
        
        private static int[] computeMostRecentMessagesToKeep(Map<RetentionPriority, Integer> mostRecentMessagesToKeep) {
            var result = new int[RetentionPriority.values().length];
            for (var entry : mostRecentMessagesToKeep.entrySet()) {
                result[entry.getKey().ordinal()] = entry.getValue();
            }
            return result;
        }

        synchronized void save(ClientMachine clientMachine, PublishMessage publishMessage) {
            int ordinal = publishMessage.getPriority().ordinal();
            Deque<PublishMessage> deque = messages.get(ordinal);
            deque.add(publishMessage);
            if (deque.size() > mostRecentMessagesToKeep[ordinal]) {
                deque.removeFirst();
            }
            setMaxIndexIfLarger(clientMachine, publishMessage.getTopic(), publishMessage.getRelayFields().getServerIndex());
        }

        /**
         * Retrieve messages to send to clientMachine.
         * 
         * <p>Messages that originated from clientMachine are not sent to it because save/onMessageRelayed would have been called,
         * setting the maxIndex for this clientMachine and topic,
         * and this functions only retrieves messages larger than maxIndex.
         * 
         * <p>If topic is null it means find all messages (used when user downloads/replays messages).
         * If topic is not null is means only messages published to this topic (used when user calls addSubscriber).
         * 
         * <p>If minClientTimestamp is null it means find all messages (used when user downloads/replays messages).
         * If minClientTimestamp is not null is means only messages published on or after this client date (used when user calls addSubscriber).
         */
        synchronized int retrieveMessages(ClientMachine clientMachine,
                                          @Nullable String topic,
                                          @Nullable Long minClientTimestamp,
                                          Long lowerBoundInclusive,
                                          long upperBoundInclusive,
                                          Consumer<PublishMessage> consumer) {
            int count = 0;
            if (lowerBoundInclusive == null) {
                lowerBoundInclusive = getMaxIndex(clientMachine, topic) + 1;
            }
            PublishMessage lastMessage = null;
            Comparator<PublishMessage> comparator = (lhs, rhs) -> Long.compare(lhs.getRelayFields().getServerIndex(), rhs.getRelayFields().getServerIndex()); 
            Iterator<PublishMessage> iter = new ZipMinIterator<PublishMessage>(messages, comparator); 
                
            while (iter.hasNext()) {
                PublishMessage message = iter.next();
                if (message.getRelayFields().getServerIndex() > upperBoundInclusive) {
                    break;
                }
                if (message.getRelayFields().getServerIndex() < lowerBoundInclusive) {
                    continue;
                }
                if (topic == null || topic.equals(message.getTopic())) {
                    if (minClientTimestamp == null || message.getClientTimestamp() >= minClientTimestamp) {
                        consumer.accept(message);
                        count++;
                        lastMessage = message;
                    }
                }
            }
            if (lastMessage != null) {
                onMessageRelayed(clientMachine, lastMessage);
            }
            return count;
        }

        synchronized void onMessageRelayed(ClientMachine clientMachine, PublishMessage publishMessage) {
            setMaxIndexIfLarger(clientMachine, publishMessage.getTopic(), publishMessage.getRelayFields().getServerIndex());
        }

        private long getMaxIndex(ClientMachine clientMachine, String topic) {
            Map<String, Long> map = highestIndexMap.computeIfAbsent(clientMachine, unused -> new LinkedHashMap<>());
            Long index = map.get(topic);
            return index != null ? index : 0;
        }

        private void setMaxIndexIfLarger(ClientMachine clientMachine, String topic, long newMax) {
            Map<String, Long> map = highestIndexMap.computeIfAbsent(clientMachine, unused -> new LinkedHashMap<>());
            Long index = map.get(topic);
            if (index == null || newMax > index) {
                map.put(topic, newMax);
            }
        }
    }
    
    /**
     * Create a message server.
     * 
     * @param host (the host of this server, may be "localhost")
     * @param port (a unique port)
     * @param mostRecentMessagesToKeep the number of most recent messages of the given priority to keep (and zero if message not in this list)
     * @throws IOException if there is an error opening a socket (but no error if the host:port is already in use)
     */
    public DistributedMessageServer(@Nonnull String host, int port, Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        this.host = host;
        this.port = port;
        this.asyncServerSocketChannel = AsynchronousServerSocketChannel.open();
        this.acceptExecutor = Executors.newSingleThreadExecutor(createThreadFactory("DistributedMessageServer.accept"));
        this.channelExecutor = Executors.newFixedThreadPool(NUM_CHANNEL_THREADS, createThreadFactory("DistributedMessageServer.socket"));
        this.retryExecutor = Executors.newScheduledThreadPool(1, createThreadFactory("DistributedMessageServer.Retry"));
        this.mostRecentMessages = new MostRecentMessages(mostRecentMessagesToKeep);
        this.cleanable = addShutdownHook(this,
                                         new Cleanup(asyncServerSocketChannel,
                                                     acceptExecutor,
                                                     channelExecutor,
                                                     retryExecutor,
                                                     clientMachines),
                                         DistributedMessageServer.class);
    }
    
    /**
     * Start the message server.
     * Returns a future that is resolved when everything starts, or rejected if starting fails.
     * If there was an IOException in starting the future is rejected with this exception.
     * 
     * @throws java.util.concurrent.RejectedExecutionException if server was shutdown
     * @throws IOException if there was an IOException such as socket already in use
     */
    public CompletionStage<Void> start() throws IOException {
        CompletableFuture<Void> future = new CompletableFuture<>();
        retryExecutor.submit(() -> doStart(future));
        return future;
    }
    
    private void doStart(CompletableFuture<Void> future) {
        String snippet = String.format("DistributedMessageServer centralServer=%s:%d",
                                       host,
                                       port);
        try {
            openServerSocket();
            acceptExecutor.submit(new AcceptThread());
            future.complete(null);
        } catch (IOException | RuntimeException | Error e) {
            LOGGER.log(Level.WARNING, String.format("Failed to start %s: %s", snippet, e.toString()));
            future.completeExceptionally(e);
        }
    }

    private void openServerSocket() throws IOException {
        onBeforeSocketBound(asyncServerSocketChannel);
        asyncServerSocketChannel.bind(new InetSocketAddress(host, port));
        LOGGER.log(Level.INFO, String.format("Started DistributedMessageServer: localHostAndPort=%s:%d, localServer=%s", host, port, getLocalAddress(asyncServerSocketChannel)));
    }
    
    /**
     * Thread that calls accept on a server socket channel.
     * There is only one instance of this thread.
     */
    private class AcceptThread implements Runnable {
        @Override
        public void run() {
            while (asyncServerSocketChannel.isOpen()) {
                try {
                    Future<AsynchronousSocketChannel> future = asyncServerSocketChannel.accept();
                    AsynchronousSocketChannel channel = future.get();
                    if (channel != null && channel.isOpen()) {
                        DistributedMessageServer.this.submitReadFromChannelJob(channel);
                    }
                } catch (ExecutionException e) {
                    LOGGER.log(Level.WARNING, "Exception during serverSocketChannel.accept(): exception={0}", e.toString()); 
                } catch (InterruptedException e) {
                    LOGGER.log(Level.DEBUG, "SocketThread interrupted");
                    break;
                }
            }
        }
    }

    /**
     * Thread that reads from a socket.
     * As the run function calls AsynchronousSocketChannel's read function, the reading actually happens in a thread managed by the AsynchronousSocketChannel framework.
     * Handling the client request happens in a thread managed by the channelExecutor.
     */
    private class ChannelThread implements Runnable {
        private final AsynchronousSocketChannel channel;

        ChannelThread(AsynchronousSocketChannel channel) {
            this.channel = channel;
        }

        @Override
        public void run() {
            SocketTransformer.readMessageFromSocketAsync(channel)
                             .thenAcceptAsync(message -> handle(channel, message), channelExecutor)
                             .whenComplete((unused, exception) -> onComplete(exception));
        }
        
        private void handle(AsynchronousSocketChannel channel, MessageBase message) {
            String unhandledClientMachine = null;
            if (message instanceof ClientGeneratedMessage) {
                if (message instanceof Identification) {
                    LOGGER.log(Level.TRACE,
                               String.format("Received message from client: clientAddress=%s, %s",
                                             getRemoteAddress(channel),
                                             message.toLoggingString()));
                    onValidMessageReceived(message);
                    Identification identification = (Identification) message;
                    DistributedMessageServer.this.addIfNotPresentAndSendPublishers(identification, channel);
                } else {
                    ClientMachine clientMachine = findClientMachineByChannel(channel);
                    if (clientMachine == null) {
                        sendRequestIdentification(channel, message);
                    } else {
                        Runnable logging = () -> {
                            LOGGER.log(Level.TRACE,
                                       String.format("Received message from client: clientMachine=%s, %s",
                                                     clientMachine.getMachineId(),
                                                     message.toLoggingString()));
                            onValidMessageReceived(message);
                        };
                        
                        if (message instanceof RelayMessageBase
                                    && ((RelayMessageBase) message).getRelayFields() != null
                                    && (!(message instanceof Resendable) || !((Resendable)message).isResend())) {
                            RelayMessageBase relayMessage = (RelayMessageBase) message;
                            DistributedMessageServer.this.sendInvalidRelayMessage(clientMachine,
                                                                                  relayMessage,
                                                                                  ErrorMessageEnum.MESSAGE_ALREADY_PROCESSED.format(relayMessage.getClientIndex()));
                        } else if (message instanceof FetchPublisher) {
                            logging.run();
                            DistributedMessageServer.this.handleFetchPublisher(clientMachine, ((FetchPublisher) message).getTopic());
                        } else if (message instanceof AddSubscriber) {
                            logging.run();
                            DistributedMessageServer.this.handleAddSubscriber(clientMachine, (AddSubscriber) message);
                        } else if (message instanceof RemoveSubscriber) {
                            logging.run();
                            DistributedMessageServer.this.handleRemoveSubscriber(clientMachine, (RemoveSubscriber) message);
                        } else if (message instanceof RelayMessageBase) {
                            logging.run();
                            RelayMessageBase relayMessage = (RelayMessageBase) message;
                            DistributedMessageServer.this.handleRelayMessage(clientMachine, relayMessage);
                        } else if (message instanceof DownloadPublishedMessages) {
                            logging.run();
                            DistributedMessageServer.this.handleDownload(clientMachine, (DownloadPublishedMessages) message);
                        } else {
                            unhandledClientMachine = clientMachine.getMachineId();
                        }
                    }
                }
            } else {
                unhandledClientMachine = getRemoteAddress(channel);
            }
            if (unhandledClientMachine != null) {
                LOGGER.log(Level.DEBUG,
                           String.format("Unhandled message from client: clientMachine=%s, %s",
                                         unhandledClientMachine,
                                         message.toLoggingString()));
            }
        }
        
        private void onComplete(Throwable exception) {
            if (exception != null) {
                if (SocketTransformer.isClosed(exception)) {
                    logChannelClosed();
                    removeChannel(channel);
                    return;
                } else {
                    logException(exception);
                }
            }
            DistributedMessageServer.this.submitReadFromChannelJob(channel);
        }
        
        private void logChannelClosed() {
            ClientMachine clientMachine = DistributedMessageServer.this.findClientMachineByChannel(channel);
            LOGGER.log(Level.INFO, "Channel closed: clientMachine={0}", clientMachine != null ? clientMachine.getMachineId() : "<unknown>");
        }
        
        private void logException(Throwable e) {
            LOGGER.log(Level.WARNING, "Error reading from remote socket: clientMachine={0}, exception={1}", getRemoteAddress(channel), e.toString());
        }
    }
    
    /**
     * Add a client machine to the topology.
     * If a machine with this id is already present, we log a warning message.
     * If a machine with this channel is already present, we log a warning message.
     * 
     * @param identification the identification sent by the client
     * @param channel the channel the message was sent on
     */
    private void addIfNotPresentAndSendPublishers(Identification identification, AsynchronousSocketChannel channel) {
        if (findClientMachineByChannel(channel) != null) {
            LOGGER.log(Level.WARNING, "Channel channel already present: clientChannel={0}", getRemoteAddress(channel));
            return;
        }
        if (findClientMachineByMachineId(identification.getMachineId()) != null) {
            LOGGER.log(Level.WARNING, "Client machine already present: clientMachine={0}", identification.getMachineId());
            return;
        }
        
        setChannelOptions(channel);
        var clientMachine = new ClientMachine(identification.getMachineId(), channel);
        clientMachines.add(clientMachine);
        LOGGER.log(Level.INFO, "Added client machine: clientMachine={0}, clientAddress={1}", clientMachine.getMachineId(), getRemoteAddress(channel));
    }
    
    /**
     * Set the channel options when a new channel is opened.
     */
    private static void setChannelOptions(AsynchronousSocketChannel channel) {
        try {
            channel.setOption(StandardSocketOptions.SO_KEEPALIVE, Boolean.TRUE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Search the topology to find if there is a channel for the given input argument channel.
     * Running time O(N).
     */
    private ClientMachine findClientMachineByChannel(AsynchronousSocketChannel channel) {
        for (var clientMachine : clientMachines) {
            if (clientMachine.getChannel() == channel) {
                return clientMachine;
            }
        }
        return null;
    }

    /**
     * Search the topology to find if there is a channel for the given input argument machine id.
     * Running time O(N).
     */
    private ClientMachine findClientMachineByMachineId(String machineId) {
        for (var clientMachine : clientMachines) {
            if (clientMachine.getMachineId().equals(machineId)) {
                return clientMachine;
            }
        }
        return null;
    }
    
    /**
     * Remove the given channel from topology.
     * Running time O(N).
     */
    private void removeChannel(AsynchronousSocketChannel channel) {
        for (var iter = clientMachines.iterator(); iter.hasNext(); ) {
            var clientMachine = iter.next();
            if (clientMachine.getChannel().equals(channel)) {
                iter.remove();
                LOGGER.log(Level.INFO, "Removed client machine: clientMachine={0} clientChannel={1}", clientMachine.getMachineId(), getRemoteAddress(channel));
            }
        }
        publishersAndSubscribers.removeAllClientMachines(channel);
    }
    
    private synchronized void handleFetchPublisher(ClientMachine clientMachine, String topic) {
        CreatePublisher createPublisher = publishersAndSubscribers.maybeAddNotifyClient(topic, clientMachine);
        if (createPublisher != null) {
            send(createPublisher, clientMachine, 0);
        }
    }
    
    private synchronized void handleAddSubscriber(ClientMachine clientMachine, AddSubscriber subscriberInfo) {
        String topic = subscriberInfo.getTopic();
        String subscriberName = subscriberInfo.getSubscriberName();
        CreatePublisher createPublisher = publishersAndSubscribers.addSubscriberEndpoint(topic, subscriberName, clientMachine);
        LOGGER.log(Level.INFO,
                   "Added subscriber : topic={0} subscriberName={1} clientMachine={2} sendingPublisher={3}",
                   topic, subscriberName, clientMachine.getMachineId(), createPublisher != null);
        if (createPublisher != null) {
            send(createPublisher, clientMachine, 0);
        }
        if (subscriberInfo.shouldTryDownload()) {
            download(clientMachine, topic, subscriberInfo.getClientTimestamp(), null, Long.MAX_VALUE);
        }
    }
    
    private synchronized void handleRemoveSubscriber(ClientMachine clientMachine, RemoveSubscriber subscriberInfo) {
        String topic = subscriberInfo.getTopic();
        String subscriberName = subscriberInfo.getSubscriberName();
        publishersAndSubscribers.removeSubscriberEndpoint(topic, subscriberName);
        LOGGER.log(Level.INFO, "Removed subscriber : topic={0} subscriberName={1} clientMachine={2}", topic, subscriberName, clientMachine.getMachineId());
    }

    private void handleRelayMessage(@Nonnull ClientMachine clientMachine, RelayMessageBase relay) {
        if (relay.getRelayFields() == null) {
            relay.setRelayFields(new RelayFields(System.currentTimeMillis(), maxMessage.incrementAndGet(), clientMachine.getMachineId()));
        }
        if (relay instanceof RelayTopicMessageBase) {
            handleRelayTopicMessage(clientMachine, (RelayTopicMessageBase) relay);
        }
    }
    
    private void handleRelayTopicMessage(ClientMachine clientMachine, RelayTopicMessageBase relay) {
        boolean isResend = false;
        if (relay instanceof CreatePublisher) {
            CreatePublisher createPublisher = (CreatePublisher) relay;
            publishersAndSubscribers.savePublisher(createPublisher);
            LOGGER.log(Level.INFO, "Added publisher: topic={0}, topicClass={1}", createPublisher.getTopic(), createPublisher.getPublisherClass().getSimpleName());
            if (createPublisher.isResend()) {
                isResend = true;
            }
        } else if (relay instanceof PublishMessage) {
            PublishMessage publishMessage = (PublishMessage) relay;
            mostRecentMessages.save(clientMachine, publishMessage);
        }
        if (!isResend) {
            publishersAndSubscribers.getClientsInterestedInTopic(relay.getTopic())
                                    .filter(otherClientMachine -> !otherClientMachine.equals(clientMachine))
                                    .forEach(otherClientMachine -> send(relay, otherClientMachine, 0));
        }
    }
    
    private void handleDownload(ClientMachine clientMachine, DownloadPublishedMessages download) {
        download(clientMachine, null, null, download.getStartServerIndexInclusive(), download.getEndServerIndexInclusive());
    }
    
    private void download(ClientMachine clientMachine,
                          @Nullable String topic,
                          @Nullable Long minClientTimestamp,
                          Long lowerBoundInclusive,
                          long upperBoundInclusive) {
        int numMessages = mostRecentMessages.retrieveMessages(
            clientMachine,
            topic,
            minClientTimestamp,
            lowerBoundInclusive, 
            upperBoundInclusive,
            publishMessage -> send(publishMessage, clientMachine, 0));
        LOGGER.log(Level.INFO, String.format("Download messages to client: clientMachine=%s numMessages=%d", clientMachine.getMachineId(), numMessages));
    }

    private void sendRequestIdentification(AsynchronousSocketChannel channel, MessageBase message) {
        ClientMachine clientMachine = new ClientMachine("<unregistered>", channel);
        RequestIdentification request = new RequestIdentification(message.getClass(), extractIndex(message));
        send(request, clientMachine, 0);
    }

    private void sendInvalidRelayMessage(ClientMachine clientMachine, RelayMessageBase relayMessage, String error) {
        InvalidRelayMessage invalid = new InvalidRelayMessage(relayMessage.getClientIndex(), error);
        send(invalid, clientMachine, 0);
    }
    
    private void send(MessageBase message, ClientMachine clientMachine, int retry) {
        if (clientMachine.getWriteManager().acquireWriteLock(message)) {
            internalSend(message, clientMachine, 0);
        }
    }
    
    /**
     * Send a message to client asynchronously.
     * If we are already writing another message to the client machine, the message to added to a queue.
     * Upon sending a message, this function sends the first of any queued messages by calling itself.
     */
    private void internalSend(MessageBase message, ClientMachine clientMachine, int retry) {
        try {
            SocketTransformer.writeMessageToSocketAsync(message, Short.MAX_VALUE, clientMachine.getChannel())
                             .thenAcceptAsync(unused -> afterMessageSent(clientMachine, message), channelExecutor)
                             .exceptionally(e -> retrySend(message, clientMachine, retry, e))
                             .thenRun(() -> sendQueuedMessageOrReleaseLock(clientMachine));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,
                       String.format("Send message failed: clientMachine=%s, retry=%d, retryDone=%b, exception=%s",
                                     clientMachine.getMachineId(), retry, true, e.toString()));
        }
    }
    
    private void afterMessageSent(ClientMachine clientMachine, MessageBase message) {
        LOGGER.log(Level.TRACE,
                   String.format("Sent message to client: clientMachine=%s, %s",
                                 clientMachine.getMachineId(),
                                 message.toLoggingString()));
        onMessageSent(message);
        if (message instanceof PublishMessage) {
            PublishMessage publishMessage = (PublishMessage) message;
            mostRecentMessages.onMessageRelayed(clientMachine, publishMessage);
        }
    }
    
    private Void retrySend(MessageBase message, ClientMachine clientMachine, int retry, Throwable e) {
        boolean retryDone = retry >= MAX_RETRIES || SocketTransformer.isClosed(e);
        Level level = retryDone ? Level.WARNING : Level.TRACE;
        LOGGER.log(level, () -> String.format("Send message failed: clientMachine=%s, retry=%d, retryDone=%b, exception=%s",
                                              clientMachine.getMachineId(), retry, retryDone, e.toString()));
        if (!retryDone) {
            int nextRetry = retry + 1;
            long delayMillis = computeExponentialBackoff(1000, nextRetry, MAX_RETRIES);
            retryExecutor.schedule(() -> send(message, clientMachine, nextRetry), delayMillis, TimeUnit.MILLISECONDS);
        }
        return null;
    }

    private void sendQueuedMessageOrReleaseLock(ClientMachine clientMachine) {
        var nextMessage = clientMachine.getWriteManager().returnHeadOfHeadQueueOrReleaseLock();
        if (nextMessage != null) {
            internalSend(nextMessage, clientMachine, 0);
        }
    }

    private void submitReadFromChannelJob(AsynchronousSocketChannel channel) {
        channelExecutor.submit(new ChannelThread(channel));
    }
    
    /**
     * Shutdown this object.
     * Object cannot be restarted after shutdown.
     */
    @Override
    public void shutdown() {
        cleanable.clean();
    }

    /**
     * Override this function to set socket options.
     * For example, the unit tests set SO_REUSEADDR to true.
     */
    protected void onBeforeSocketBound(NetworkChannel channel) throws IOException {
    }

    /**
     * Override this function to do something before sending a message.
     * For example, the unit tests override this to record the number of messages sent.
     */
    protected void onMessageSent(MessageBase message) {
    }

    /**
     * Override this function to do something upon receiving a message.
     * For example, the unit tests override this to record the number of messages received.
     * 
     * <p>This function is only called for valid messages received.
     */
    protected void onValidMessageReceived(MessageBase message) {
    }

    private enum ErrorMessageEnum {
        /**
         * Server already saw this message and gave it a serverIndex.
         * Yet client sent this message back to the server.
         */
        MESSAGE_ALREADY_PROCESSED("Message already processed by server: clientIndex=%s");
        
        private String formatString;

        ErrorMessageEnum(String formatString) {
            this.formatString = formatString;
        }

        String format(Object... args) {
            return String.format(formatString, args);
        }
    }
    
    /**
     * Cleanup this class.
     * Close all connections, close the socket server channel, and shutdown all executors.
     */
    private static class Cleanup extends CallStackCapturing implements Runnable {
        private final NetworkChannel channel;
        private final ExecutorService acceptExecutor;
        private final ExecutorService channelExecutor;
        private final ExecutorService retryExecutor;
        private final List<ClientMachine> clientMachines;

        private Cleanup(NetworkChannel channel,
                        ExecutorService acceptExecutor,
                        ExecutorService channelExecutor,
                        ExecutorService retryExecutor,
                        List<ClientMachine> clientMachines) {
            this.channel = channel;
            this.acceptExecutor = acceptExecutor;
            this.channelExecutor = channelExecutor;
            this.retryExecutor = retryExecutor;
            this.clientMachines = clientMachines;
        }

        @Override
        public void run() {
            LOGGER.log(Level.DEBUG, "Shutting down " + DistributedMessageServer.class.getSimpleName() + " " + getLocalAddress(channel) + getCallStack());
            closeExecutorQuietly(acceptExecutor);
            closeExecutorQuietly(channelExecutor);
            closeExecutorQuietly(retryExecutor);
            clientMachines.forEach(clientMachine -> closeQuietly(clientMachine.channel));
            closeQuietly(channel);
        }
    }
}

