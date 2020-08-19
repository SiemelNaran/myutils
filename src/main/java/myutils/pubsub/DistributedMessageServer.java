package myutils.pubsub;

import static myutils.pubsub.PubSubUtils.addShutdownHook;
import static myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static myutils.pubsub.PubSubUtils.closeQuietly;
import static myutils.pubsub.PubSubUtils.computeExponentialBackoff;
import static myutils.pubsub.PubSubUtils.extractClientIndex;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
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
import myutils.pubsub.MessageClasses.UnsupportedMessage;
import myutils.pubsub.PubSubUtils.CallStackCapturing;
import myutils.util.MoreCollections;
import myutils.util.ZipMinIterator;


/**
 * Server class that receives messages from a client and relays it to all subscribed to the topic.
 * When a client connects, they send an Identification message identifying their name, and the name must be unique.
 * A client can then send createPublisher and publisher.publish commands, and they will be relayed to other clients who are subscribed to the topic.
 * If sending a message to a client fails, it is retried with exponential backoff up to a maximum number of times.
 * 
 * <p>In implementation, there is one accept thread with listens for socket connections by calling asyncServerSocketChannel.accept().
 * Once a connection is available, we submit the channel to a pool of channel threads to read a message.
 * Reading happens asynchronously in a thread managed by the AsynchronousServerSocketChannel classes.
 * Upon receiving the message, we handle the message in the pool in channel threads.
 * We then resubmit the channel to the pool of channel threads to read another message.
 * There is another thread that handles retries with exponential backoff.
 * 
 * <p>When a client connects for the first time, they should send an Identification message, identifying their machine name.
 * If a second client connects with the same name, we log a warning and ignore the client.
 * We then add the client to our list of clients and set SO_KEEPALIVE to true.
 * 
 * <p>Thereafter clients may send createPublisher, publisher.publisher, subscribe, unsubscribe, or other commands,
 * and these will be relayed to all other clients.
 * Upon receiving a message to relay, the server generates a monotonically  increasing integer and sets the machineId of the machine which sent the message.
 * These are part of the message sent to each client who is subscribed to the topic.
 * The server id is guaranteed to be unique monotonically increasing even if the server is restarted,
 * so so long as all machines have the correct time,
 * because the id consists of the server start time followed by a monotonically increasing long number.
 *
 * <p>If the server dies the clients keep polling for a new server to come online with capped exponential backoff.
 * Once a server comes online, the clients resend all publishers they created, and all topics they are subscribed to.
 * This is needed in case the server did not save the publishers and subscribers to disk.
 *
 * <p>A note on infinite recursion: if server relays a message to client2, that client2 must not send that message back to the server
 * as in theory that would send the message back to client1.
 * However, using the field serverIndex, the server detects that it already processed the message and therefore ignores it.
 * But clients should still not send the message to avoid unnecessary network traffic.
 * 
 * <p>The server caches the last N messages of each RententionPriority.
 * Clients can download all publish message commands from a particular server index, and all messages in the cache from this time up to the time of download
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
    private final AsynchronousServerSocketChannel asyncServerSocketChannel;
    private final ExecutorService acceptExecutor;
    private final ExecutorService channelExecutor;
    private final ScheduledExecutorService retryExecutor;
    private final List<ClientMachine> clientMachines = new CopyOnWriteArrayList<>();
    private final AtomicReference<ServerIndex> maxMessage = new AtomicReference<>(ServerIndex.createDefaultFromNow());
    private final PublishersAndSubscribers publishersAndSubscribers = new PublishersAndSubscribers();
    private final MostRecentMessages mostRecentMessages;
    private final Cleanable cleanable;
    
    
    /**
     * Class representing a remote machine.
     * Key fields are machineId (a string) and channel (an AsynchronousSocketChannel).
     */
    protected static final class ClientMachine {
        private static ClientMachine unregistered(@Nonnull AsynchronousSocketChannel channel) {
            return new ClientMachine(new ClientMachineId("<unregistered>"), channel);
        }
        
        private final @Nonnull ClientMachineId machineId;
        private final @Nonnull String remoteAddress;
        private final @Nonnull AsynchronousSocketChannel channel;
        private final @Nonnull WriteManager writeManager = new WriteManager();

        private ClientMachine(@Nonnull ClientMachineId machineId, @Nonnull AsynchronousSocketChannel channel) {
            this.machineId = machineId;
            this.remoteAddress = getRemoteAddress(channel);
            this.channel = channel;
        }

        private @Nonnull AsynchronousSocketChannel getChannel() {
            return channel;
        }
        
        protected final @Nonnull ClientMachineId getMachineId() {
            return machineId;
        }
        
        @Override
        public String toString() {
            return machineId.toString() + '@' + remoteAddress;
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(machineId);
        }
        
        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof ClientMachine)) {
                return false;
            }
            ClientMachine that = (ClientMachine) thatObject;
            return this.machineId.equals(that.machineId);
        }

//        @Override
//        public int compareTo(ClientMachine that) {
//            return this.machineId.compareTo(that.machineId);
//        }

        @Nonnull WriteManager getWriteManager() {
            return writeManager;
        }

        /**
         * Class to ensure that only one threads tries to write to a channel at one time,
         * otherwise we will encounter WritePendingException.
         */
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
     * 
     * @implNote The synchronized functions in this class could be synchronized on TopicInfo.notifyClients instead, and topicMap would have to be a ConcurrentHashMap.
     */
    private static class PublishersAndSubscribers {
        private static class TopicInfo {
            private CreatePublisher createPublisher;
            private final List<SubscriberEndpoint> subscriberEndpoints = new ArrayList<>(); // unique by ClientMachine, subscriberName; sorted by ClientMachine, clientTimestamp
            private Set<ClientMachineId> notifyClients; // clients to notify when a publisher is created
            private final Collection<SubscriberEndpoint> inactiveSubscriberEndpoints = new HashSet<>();
            
            private void setNotifyClientsToNullIfEmpty() {
                if (notifyClients != null && notifyClients.isEmpty()) {
                    notifyClients = null;
                }
            }
        }
        
        private final Map<String /*topic*/, TopicInfo> topicMap = new TreeMap<>();
        
        /**
         * Add subscriber to this topic.
         * Note that the publisher may not yet be created.
         * 
         * @return the CreatePublisher if one exists, and true if the client machine is not already subscribed
         */
        @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
        synchronized @Nonnull AddSubscriberResult addSubscriberEndpoint(final String topic, final String subscriberName, long clientTimestamp, ClientMachineId clientMachineId) {
            TopicInfo info = topicMap.computeIfAbsent(topic, unused -> new TopicInfo());
            Long revisedClientTimestamp = null;
            
            // in case client died and a new one is started, get clientTimestamp from the previous subscriber
            // running time O(N) where N is the number of inactive subscribers for this topic
            // O(1) is possible using HashSet or HashMap lookup as inactiveSubscriberEndpoints is a HashSet
            for (var iter = info.inactiveSubscriberEndpoints.iterator(); iter.hasNext(); ) {
                var oldEndpoint = iter.next();
                if (oldEndpoint.getSubscriberName().equals(subscriberName) && oldEndpoint.getClientMachineId().equals(clientMachineId)) {
                    iter.remove();
                    clientTimestamp = oldEndpoint.getClientTimestamp();
                    revisedClientTimestamp = clientTimestamp;
                    break;
                }
            }
            
            // check if client machine is already subscribed
            // this must be done before adding subscriber endpoint
            // running time O(N) where N is the number of active subscribers for this topic
            // O(lg(N)) is possible using binary search
            boolean clientMachineAlreadySubscribedToTopic = isClientMachineAlreadySubscribedToTopic(info, clientMachineId); // checkstyle:VariableDeclarationUsageDistance
            
            // add subscriber in sorted order
            // sort order is clientMachine then client timestamp
            // running time O(N*lg(N)) where N is the number of active subscribers
            // O(lg(N) + N) is possible using binary search followed by insert at the right location
            var newEndpoint = new SubscriberEndpoint(clientMachineId, subscriberName, clientTimestamp);
            if (info.subscriberEndpoints.contains(newEndpoint)) {
                throw new IllegalStateException("Already subscribed to topic: "// COVERAGE: missed
                        + "clientMachine=" + clientMachineId
                        + ", topic=" + topic
                        + ", subscriberName=" + subscriberName);
            }
            info.subscriberEndpoints.add(newEndpoint);
            info.subscriberEndpoints.sort(Comparator.comparing(SubscriberEndpoint::getClientMachineId)
                                                    .thenComparing(SubscriberEndpoint::getClientTimestamp));
            
            // remove client machine from notifyClients as clients who subscribe to a topic are always notified when the publisher is created
            // running time O(N) where N is the number of clients wanting a notification when the publisher is created
            if (info.notifyClients != null) {
                info.notifyClients.removeIf(c -> c.equals(clientMachineId));
                info.setNotifyClientsToNullIfEmpty();
            }
            
            return new AddSubscriberResult(info.createPublisher, clientMachineAlreadySubscribedToTopic, revisedClientTimestamp);
        }
        
        private boolean isClientMachineAlreadySubscribedToTopic(TopicInfo info, ClientMachineId clientMachineId) {
            if (info.createPublisher != null && info.createPublisher.getRelayFields().getSourceMachineId().equals(clientMachineId)) {
                return true;
            }
            return info.subscriberEndpoints.stream().anyMatch(endpoint -> endpoint.getClientMachineId().equals(clientMachineId));
        }

        static class AddSubscriberResult {
            private final @Nullable CreatePublisher createPublisher;
            private final boolean clientMachineAlreadySubscribedToTopic;
            private final Long revisedClientTimestamp;
            
            private AddSubscriberResult(@Nullable CreatePublisher createPublisher, boolean clientMachineAlreadySubscribedToTopic, Long revisedClientTimestamp) {
                this.createPublisher = createPublisher;
                this.clientMachineAlreadySubscribedToTopic = clientMachineAlreadySubscribedToTopic;
                this.revisedClientTimestamp = revisedClientTimestamp;
            }

            @Nullable CreatePublisher getCreatePublisher() {
                return createPublisher;
            }

            boolean isClientMachineAlreadySubscribedToTopic() {
                return clientMachineAlreadySubscribedToTopic;
            }
            
            Long getRevisedClientTimestamp() {
                return revisedClientTimestamp;
            }
        }

        public synchronized void removeSubscriberEndpoint(String topic, ClientMachineId clientMachineId, String subscriberName) {
            TopicInfo info = Objects.requireNonNull(topicMap.get(topic));
            info.subscriberEndpoints.removeIf(subscriberEndpoint -> subscriberEndpoint.getSubscriberName().equals(subscriberName));
            if (info.inactiveSubscriberEndpoints != null) {
                info.inactiveSubscriberEndpoints.removeIf(endpoint -> endpoint.getClientMachineId().equals(clientMachineId) && endpoint.getSubscriberName().equals(subscriberName));
            }
        }
        
        /**
         * Add a client to notify upon the publisher getting created.
         * Does not add if there is already a subscriber and the publisher has not yet been created,
         * as the client will get notified anyway upon the publisher getting created.
         *
         * @return the CreatePublisher if one exists
         */
        synchronized CreatePublisher maybeAddNotifyClient(String topic, ClientMachineId clientMachineId) {
            TopicInfo info = topicMap.computeIfAbsent(topic, unused -> new TopicInfo());
            if (info.createPublisher == null && info.subscriberEndpoints.stream().anyMatch(subscriberEndpoint -> subscriberEndpoint.getClientMachineId().equals(clientMachineId))) {
                return null;
            }
            if (info.createPublisher != null) {
                return info.createPublisher;
            }
            if (info.notifyClients == null) {
                info.notifyClients = new HashSet<>();
            }
            info.notifyClients.add(clientMachineId);
            return null;
        }
        
        /**
         * Add createPublisher command.
         */
        synchronized CreatePublisherResult savePublisher(CreatePublisher createPublisher) {
            var topic = createPublisher.getTopic();
            TopicInfo info = topicMap.computeIfAbsent(topic, unused -> new TopicInfo());
            if (info.createPublisher == null) {
                info.createPublisher = createPublisher;
                return new CreatePublisherResult(null);
            } else {
                return new CreatePublisherResult(info.createPublisher);
            }
        }
        
        static class CreatePublisherResult {
            private final @Nullable CreatePublisher alreadyExistsCreatePublisher;
            
            private CreatePublisherResult(@Nullable CreatePublisher alreadyExistsCreatePublisher) {
                this.alreadyExistsCreatePublisher = alreadyExistsCreatePublisher;
            }
        } 

        /**
         * A channel has been closed.
         * Mark all subscriber endpoints for this channel as inactive.
         * 
         * @return a list of topics that were unsubscribed from (but not the number of subscribers for each topic), or ? if INFO level is not enabled
         */
        synchronized StringBuilder removeClientMachine(ClientMachineId clientMachineId) {
            boolean returnTopicsAffected = LOGGER.isLoggable(Level.INFO);
            StringBuilder topicsAffected = new StringBuilder();
            if (returnTopicsAffected) {
                topicsAffected.append('[');
            }
            for (var entry : topicMap.entrySet()) {
                TopicInfo info = entry.getValue();
                int removeCount = 0;
                for (var iter = info.subscriberEndpoints.iterator(); iter.hasNext(); ) {
                    var endpoint = iter.next();
                    if (endpoint.getClientMachineId().equals(clientMachineId)) {
                        iter.remove();
                        removeCount++;
                        info.inactiveSubscriberEndpoints.add(endpoint);
                    }
                }
                if (info.notifyClients != null) {
                    info.notifyClients.removeIf(notifyClientMachineId -> notifyClientMachineId.equals(clientMachineId));
                    info.setNotifyClientsToNullIfEmpty();
                }
                if (removeCount > 1 && returnTopicsAffected) {
                    topicsAffected.append(entry.getKey()).append('(').append(removeCount).append("),");
                }
            }
            if (returnTopicsAffected) {
                if (topicsAffected.length() > 1) {
                    topicsAffected.deleteCharAt(topicsAffected.length() - 1);
                }
                topicsAffected.append(']');
            }
            return topicsAffected;
        }

        /**
         * This function is used to relay messages from one client to another.
         * Find the list client machines subscribed to this topic, or who want a notification if fetchClientsWantingNotification is true.
         * Side effect of this function is to remove elements from the notifyClients collection if fetchClientsWantingNotification is true.
         * 
         * @param topic retrieve clientMachines subscribed to this topic or who want a notification
         * @param excludeMachineId exclude this machine (used to not relay message to client who sent the message)
         * @param fetchClientsWantingNotification if true fetch client machines that want a notification (used for clients who want to download a publisher but not subscribe to it)
         * @param consumer apply this action to each client machine.
         *        The 2nd argument is the subscriber minimum client timestamp (useful if one client machine has many subscribers),
         *        and is null if we are only notifying a client (i.e. for the fetchPublisher command).  
         */
        synchronized void forClientsSubscribedToPublisher(String topic,
                                                          @Nullable ClientMachineId excludeMachineId,
                                                          boolean fetchClientsWantingNotification,
                                                          BiConsumer<ClientMachineId, Long /*@Nullable minClientTimestamp*/> consumer) {
            TopicInfo info = Objects.requireNonNull(topicMap.get(topic));
            if (info.createPublisher == null) {
                return;
            }
            ClientMachineId prevClientMachineId = null;
            for (SubscriberEndpoint subscriberEndpoint : info.subscriberEndpoints) {
                if (subscriberEndpoint.getClientMachineId().equals(excludeMachineId)) {
                    continue;
                }
                if (!subscriberEndpoint.getClientMachineId().equals(prevClientMachineId)) {
                    consumer.accept(subscriberEndpoint.getClientMachineId(), subscriberEndpoint.getClientTimestamp());
                    prevClientMachineId = subscriberEndpoint.getClientMachineId();
                }
            }

            if (fetchClientsWantingNotification && info.notifyClients != null) {
                Stream<ClientMachineId> notifyClients = info.notifyClients.stream();
                info.notifyClients = null;
                notifyClients.forEach(clientMachineId -> consumer.accept(clientMachineId, null));
            }
        }
    }
    
    /**
     * Class to what subscribers and client machine has.
     * Unique key is clientMachineId + subscriberName.
     * One client machine can have two subscribers for the same topic.
     */
    private static final class SubscriberEndpoint {
        private final ClientMachineId clientMachineId;
        private final String subscriberName;
        private final long clientTimestamp;

        private SubscriberEndpoint(ClientMachineId clientMachineId, String subscriberName, long clientTimestamp) {
            this.clientMachineId = clientMachineId;
            this.subscriberName = subscriberName;
            this.clientTimestamp = clientTimestamp;
        }

        private ClientMachineId getClientMachineId() {
            return clientMachineId;
        }

        private String getSubscriberName() {
            return subscriberName;
        }

        private long getClientTimestamp() {
            return clientTimestamp;
        }
        
        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof SubscriberEndpoint)) {
                return false;
            }
            SubscriberEndpoint that = (SubscriberEndpoint) thatObject;
            return this.clientMachineId.equals(that.clientMachineId) && this.subscriberName.equals(that.subscriberName);
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(clientMachineId, subscriberName);
        }
        
        @Override
        public String toString() {
            return clientMachineId + "/" + subscriberName;
        }
    }

    /**
     * A data structure to store the most recently published messages in memory.
     * We only save the last N messages of each retention priority.
     */
    private static class MostRecentMessages {
        private final int[] numberOfMostRecentMessagesToKeep;
        private final List<LinkedList<PublishMessage>> messages = List.of(new LinkedList<>(), new LinkedList<>()); // MEDIUM priority, HIGH priority
        private final Map<ClientMachineId, Map<String /*topic*/, ServerIndex /*maxIndex*/>> highestIndexMap = new HashMap<>();
        
        MostRecentMessages(Map<RetentionPriority, Integer> numberOfMostRecentMessagesToKeep) {
            this.numberOfMostRecentMessagesToKeep = computeNumberOfMostRecentMessagesToKeep(numberOfMostRecentMessagesToKeep);
        }
        
        private static int[] computeNumberOfMostRecentMessagesToKeep(Map<RetentionPriority, Integer> mostRecentMessagesToKeep) {
            var result = new int[RetentionPriority.values().length];
            for (var entry : mostRecentMessagesToKeep.entrySet()) {
                result[entry.getKey().ordinal()] = entry.getValue();
            }
            return result;
        }

        synchronized void save(PublishMessage publishMessage) {
            int ordinal = publishMessage.getRetentionPriority().ordinal();
            LinkedList<PublishMessage> linkedList = messages.get(ordinal);
            MoreCollections.addLargeElementToSortedList(linkedList, COMPARE_BY_SERVER_INDEX, publishMessage);
            if (linkedList.size() > numberOfMostRecentMessagesToKeep[ordinal]) {
                linkedList.removeFirst();
            }
        }
        
        private static final Comparator<PublishMessage> COMPARE_BY_SERVER_INDEX = (lhs, rhs) -> {
            return lhs.getRelayFields().getServerIndex().compareTo(rhs.getRelayFields().getServerIndex());
        };

        /**
         * This function is used to send saved messages to a client.
         * It is the calling function's responsibility to check if this client is subscribed to the topic.
         * 
         * <p>Messages that originated from clientMachine are not sent to it because save/onMessageRelayed would have been called,
         * setting the maxIndex for this clientMachine and topic,
         * and this functions only retrieves messages larger than maxIndex.
         * 
         * <p>If topic is null it means find all messages (used when user downloads messages).
         * If topic is not null is means only messages published to this topic (used when user calls addSubscriber).
         * 
         * <p>If minClientTimestamp is null it means find all messages (used when user downloads messages).
         * If minClientTimestamp is not null is means only messages published on or after this client date (used when user calls addSubscriber).
         */
        synchronized int forSavedMessages(ClientMachine clientMachine,
                                          @Nullable String topic,
                                          @Nullable Long minClientTimestamp,
                                          @Nullable ServerIndex lowerBoundInclusive,
                                          ServerIndex upperBoundInclusive,
                                          Consumer<PublishMessage> consumer) {
            int count = 0;
            if (lowerBoundInclusive == null) {
                lowerBoundInclusive = getMaxIndex(clientMachine, topic);
                lowerBoundInclusive = lowerBoundInclusive.increment();
            }
            PublishMessage lastMessage = null;
            Comparator<PublishMessage> comparator = (lhs, rhs) -> ServerIndex.compare(lhs.getRelayFields().getServerIndex(), rhs.getRelayFields().getServerIndex()); 
            Iterator<PublishMessage> iter = new ZipMinIterator<>(messages, comparator);

            while (iter.hasNext()) {
                PublishMessage message = iter.next();
                if (message.getRelayFields().getServerIndex().compareTo(upperBoundInclusive) > 0) {
                    break; // COVERAGE: download test also test subset of messages
                }
                if (message.getRelayFields().getServerIndex().compareTo(lowerBoundInclusive) < 0) {
                    continue;
                }
                if (topic == null || topic.equals(message.getTopic())) {
                    lastMessage = message;
                    if (minClientTimestamp == null || message.getClientTimestamp() >= minClientTimestamp) {
                        consumer.accept(message);
                        count++;
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

        /**
         * Function must be called with lock held.
         */
        private @Nonnull ServerIndex getMaxIndex(ClientMachine clientMachine, String topic) {
            Map<String, ServerIndex> map = highestIndexMap.computeIfAbsent(clientMachine.getMachineId(), unused -> new HashMap<>());
            var index = map.get(topic);
            return index != null ? index : ServerIndex.MIN_VALUE; // COVERAGE: test message published with no subscribers, then subscriber added
        }

        /**
         * Function must be called with lock held.
         */
        private void setMaxIndexIfLarger(ClientMachine clientMachine, String topic, ServerIndex newMax) {
            Map<String, ServerIndex> map = highestIndexMap.computeIfAbsent(clientMachine.getMachineId(), unused -> new HashMap<>());
            var index = map.get(topic);
            if (index == null || newMax.compareTo(index) > 0) {
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
        this.acceptExecutor = Executors.newSingleThreadExecutor(createThreadFactory("DistributedMessageServer.accept", true));
        this.channelExecutor = Executors.newFixedThreadPool(NUM_CHANNEL_THREADS, createThreadFactory("DistributedMessageServer.socket", true));
        this.retryExecutor = Executors.newScheduledThreadPool(1, createThreadFactory("DistributedMessageServer.Retry", true));
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
     * Start the message server asynchronously.
     * Returns a future that is resolved when everything starts, or rejected if starting fails.
     * If there was an IOException in starting the future is rejected with this exception.
     * 
     * @throws java.util.concurrent.RejectedExecutionException if server was shutdown
     */
    public CompletableFuture<Void> start() {
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
            LOGGER.log(Level.WARNING, String.format("Failed to start %s", snippet), e);
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
                    LOGGER.log(Level.WARNING, "Exception during serverSocketChannel.accept()", e); 
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
                             .thenApplyAsync(message -> handle(channel, message), channelExecutor)
                             .whenComplete(this::onComplete);
        }
        
        private @Nullable ClientMachine handle(AsynchronousSocketChannel channel, MessageBase message) {
            ClientMachine clientMachine = null;
            boolean unhandled = false;
            if (message instanceof ClientGeneratedMessage) {
                if (message instanceof Identification) {
                    LOGGER.log(Level.TRACE,
                               String.format("Received message from client: clientAddress=%s, %s",
                                             getRemoteAddress(channel),
                                             message.toLoggingString()));
                    onValidMessageReceived(message);
                    Identification identification = (Identification) message;
                    DistributedMessageServer.this.addIfNotPresent(identification, channel);
                } else {
                    clientMachine = findClientMachineByChannel(channel);
                    if (clientMachine == null) {
                        sendRequestIdentification(channel, message);
                    } else {
                        ClientMachine clientMachineAsFinal = clientMachine;
                        Runnable logging = () -> {
                            LOGGER.log(Level.TRACE,
                                       String.format("Received message from client: clientMachine=%s, %s",
                                                     clientMachineAsFinal.getMachineId(),
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
                            RelayMessageBase relay = (RelayMessageBase) message;
                            if (relay.getRelayFields() == null) {
                                ServerIndex nextServerId = maxMessage.updateAndGet(ServerIndex::increment);
                                relay.setRelayFields(new RelayFields(System.currentTimeMillis(), nextServerId, clientMachine.getMachineId()));
                            }
                            logging.run();
                            DistributedMessageServer.this.handleRelayMessage(clientMachine, relay);
                        } else if (message instanceof DownloadPublishedMessages) {
                            logging.run();
                            DistributedMessageServer.this.handleDownload(clientMachine, (DownloadPublishedMessages) message);
                        } else {
                            unhandled = true;
                        }
                    }
                }
            } else {
                clientMachine = findClientMachineByChannel(channel);
                unhandled = true;
            }
            if (unhandled) {
                if (clientMachine == null) {
                    clientMachine = ClientMachine.unregistered(channel);
                }
                LOGGER.log(Level.WARNING,
                           String.format("Unsupported message from client: clientMachine=%s, %s",
                                         clientMachine,
                                         message.toLoggingString()));
                DistributedMessageServer.this.send(new UnsupportedMessage(message.getClass(), extractClientIndex(message)), clientMachine, 0);
            }
            return clientMachine;
        }
        
        private void onComplete(@Nullable ClientMachine clientMachine, Throwable exception) {
            if (exception != null) {
                if (SocketTransformer.isClosed(exception)) {
                    logChannelClosed(exception);
                    removeChannel(channel);
                    return;
                } else {
                    logException(clientMachine, exception);
                }
            }
            DistributedMessageServer.this.submitReadFromChannelJob(channel);
        }
        
        private void logChannelClosed(Throwable e) {
            ClientMachine clientMachine = DistributedMessageServer.this.findClientMachineByChannel(channel);
            LOGGER.log(Level.INFO,
                       "Channel closed: clientMachine={0}, exception={1}",
                       clientMachine != null ? clientMachine.getMachineId() : "<unknown>",
                       e.toString());
        }
        
        private void logException(@Nullable ClientMachine clientMachine, Throwable e) {
            LOGGER.log(Level.WARNING,
                       String.format("Error reading from remote socket: clientMachine=%s",
                                     clientMachine != null ? clientMachine.getMachineId() : getRemoteAddress(channel)),
                       e);
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
    private void addIfNotPresent(Identification identification, AsynchronousSocketChannel channel) {
        if (findClientMachineByChannel(channel) != null) {
            LOGGER.log(Level.ERROR, "Channel channel already present: clientChannel={0}", getRemoteAddress(channel));
            return;
        }
        if (findClientMachineByMachineId(identification.getMachineId()) != null) {
            LOGGER.log(Level.ERROR, "Client machine already present: clientMachine={0}", identification.getMachineId()); // COVERAGE: test adding 2nd machine with same name
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
    private ClientMachine findClientMachineByMachineId(ClientMachineId machineId) {
        for (var clientMachine : clientMachines) {
            if (clientMachine.getMachineId().equals(machineId)) {
                return clientMachine;
            }
        }
        return null;
    }
    
    /**
     * Given a client machine id find the channel.
     * Running time O(N).
     * 
     * @return client machine or null if the client id is not found
     * @implNote function could be made O(1) by using a ConcurrentHashMap of ClientMachineId to ClientMachine 
     */
    private ClientMachine lookupClientMachine(ClientMachineId clientMachineId) {
        return clientMachines.stream().filter(clientMachine -> clientMachine.getMachineId().equals(clientMachineId))
                                      .findFirst()
                                      .orElse(null);
    }
    
    /**
     * Remove the given channel from topology.
     * Running time O(N).
     */
    private void removeChannel(AsynchronousSocketChannel channel) {
        ClientMachine clientMachine = Objects.requireNonNull(findClientMachineByChannel(channel));
        String topicsAffected = publishersAndSubscribers.removeClientMachine(clientMachine.getMachineId()).toString();
        clientMachines.remove(clientMachine);
        LOGGER.log(Level.INFO, "Removed client machine: clientMachine={0}, clientChannel={1}, topicsAffected={2}",
                   clientMachine.getMachineId(),
                   getRemoteAddress(channel),
                   topicsAffected);
    }
    
    private void handleFetchPublisher(ClientMachine clientMachine, String topic) {
        CreatePublisher createPublisher = publishersAndSubscribers.maybeAddNotifyClient(topic, clientMachine.getMachineId());
        if (createPublisher != null) {
            send(createPublisher, clientMachine, 0);
        }
    }
    
    private void handleAddSubscriber(ClientMachine clientMachine, AddSubscriber subscriberInfo) {
        String topic = subscriberInfo.getTopic();
        String subscriberName = subscriberInfo.getSubscriberName();
        long clientTimestamp = subscriberInfo.getClientTimestamp();
        boolean doDownload = false;
        boolean forceLogging = subscriberInfo.isResend();
        PublishersAndSubscribers.AddSubscriberResult addSubscriberResult = publishersAndSubscribers.addSubscriberEndpoint(topic,
                                                                                                                          subscriberName,
                                                                                                                          clientTimestamp,
                                                                                                                          clientMachine.getMachineId());
        if (addSubscriberResult.getRevisedClientTimestamp() != null) {
            clientTimestamp = addSubscriberResult.getRevisedClientTimestamp();
        }
        boolean sendingPublisher = false;
        if (addSubscriberResult.getCreatePublisher() != null && !addSubscriberResult.isClientMachineAlreadySubscribedToTopic()) {
            if (!subscriberInfo.isResend()) {
                sendingPublisher = true;
            }
            if (subscriberInfo.shouldTryDownload()) {
                doDownload = true;
                forceLogging = true;
            }
        }
        LOGGER.log(Level.INFO,
                   "Added subscriber : topic={0}, subscriberName={1}, clientMachine={2}, sendingPublisher={3}, doDownload={4}",
                   topic, subscriberName, clientMachine.getMachineId(), sendingPublisher, doDownload);
        if (sendingPublisher) {
            send(addSubscriberResult.getCreatePublisher(), clientMachine, 0);
        }
        if (doDownload) {
            download("handleAddSubscriber", clientMachine, topic, clientTimestamp, null, ServerIndex.MAX_VALUE, forceLogging);
        }
    }
    
    private void handleRemoveSubscriber(ClientMachine clientMachine, RemoveSubscriber subscriberInfo) {
        String topic = subscriberInfo.getTopic();
        String subscriberName = subscriberInfo.getSubscriberName();
        publishersAndSubscribers.removeSubscriberEndpoint(topic, clientMachine.getMachineId(), subscriberName);
        LOGGER.log(Level.INFO, "Removed subscriber : topic={0} subscriberName={1} clientMachine={2}", topic, subscriberName, clientMachine.getMachineId());
    }

    private void handleRelayMessage(@Nonnull ClientMachine clientMachine, RelayMessageBase relay) {
        if (relay instanceof RelayTopicMessageBase) {
            handleRelayTopicMessage(clientMachine, (RelayTopicMessageBase) relay);
        }
    }
    
    private void handleRelayTopicMessage(ClientMachine clientMachine, RelayTopicMessageBase relay) {
        boolean skipRelayMessage = false;
        boolean isResendPublisher = false;
        boolean fetchClientsWantingNotification = false;
        if (relay instanceof CreatePublisher) {
            CreatePublisher createPublisherPassedInToThisFunction = (CreatePublisher) relay;
            PublishersAndSubscribers.CreatePublisherResult createPublisherResult = publishersAndSubscribers.savePublisher(createPublisherPassedInToThisFunction);
            if (createPublisherResult.alreadyExistsCreatePublisher == null) {
                LOGGER.log(Level.INFO, "Added publisher: topic={0}, topicClass={1}", relay.getTopic(), createPublisherPassedInToThisFunction.getPublisherClass().getSimpleName());
                if (createPublisherPassedInToThisFunction.isResend()) {
                    isResendPublisher = true;
                }
                fetchClientsWantingNotification = true;
            } else {
                CreatePublisher alreadyExistsCreatePublisher = createPublisherResult.alreadyExistsCreatePublisher;
                LOGGER.log(Level.INFO,
                           String.format("Publisher already exists: topic=%s, topicClass=%s, clientTimestamp=%d, serverIndex=%s, discarding newServerIndex=%s",
                                         alreadyExistsCreatePublisher.getTopic(),
                                         alreadyExistsCreatePublisher.getPublisherClass().getSimpleName(),
                                         alreadyExistsCreatePublisher.getClientTimestamp(),
                                         alreadyExistsCreatePublisher.getRelayFields().getServerIndex().toString(),
                                         relay.getRelayFields().getServerIndex()));
                skipRelayMessage = true;
            }
        } else if (relay instanceof PublishMessage) {
            PublishMessage publishMessage = (PublishMessage) relay;
            mostRecentMessages.save(publishMessage);
        }
        if (!skipRelayMessage) {
            relayMessageToOtherClients(clientMachine, relay, isResendPublisher, fetchClientsWantingNotification);
        }
    }
    
    private void relayMessageToOtherClients(ClientMachine clientMachine, RelayTopicMessageBase relay, boolean isResendPublisher, boolean fetchClientsWantingNotification) {
        BiConsumer<ClientMachineId, Long> relayAction;
        if (!isResendPublisher) {
            // relay CreatePublisher or PublishMessage to clients subscribed to this topic
            relayAction = (otherClientMachineId, unused) -> send(relay, lookupClientMachine(otherClientMachineId), 0);
        } else {
            // client is resending publisher after a server restart
            // download all messages in the cache from the subscriber timestamp
            // this handles the case that (a) server dies, (b) clients publish messages, (c) server restarts,
            // and (d) clients send the messages and resend their AddSubscriber and CreatePublisher commands to the server
            // the messages published since the server died (b) need to be sent to all subscribers
            relayAction = (otherClientMachineId, minClientTimestamp) -> {
                if (minClientTimestamp == null) {
                    send(relay, lookupClientMachine(otherClientMachineId), 0); // COVERAGE: missed
                } else {
                    download("handleCreatePublisher",
                             lookupClientMachine(otherClientMachineId),
                             relay.getTopic(), minClientTimestamp,
                             null /*lowerBoundInclusive*/,
                             ServerIndex.MAX_VALUE,
                             true /*forceLogging*/);
                }
            };
        }
        publishersAndSubscribers.forClientsSubscribedToPublisher(relay.getTopic(), clientMachine.getMachineId(), fetchClientsWantingNotification, relayAction);
    }
    
    private void handleDownload(ClientMachine clientMachine, DownloadPublishedMessages download) {
        download("download", clientMachine, null, null, download.getStartServerIndexInclusive(), download.getEndServerIndexInclusive(), /*forceLogging*/ true);
    }
    
    private void download(@Nonnull String trigger,
                          ClientMachine clientMachine,
                          @Nullable String topic,
                          @Nullable Long minClientTimestamp,
                          @Nullable ServerIndex lowerBoundInclusive,
                          ServerIndex upperBoundInclusive,
                          boolean forceLogging) {
        int numMessages = mostRecentMessages.forSavedMessages(
            clientMachine,
            topic,
            minClientTimestamp,
            lowerBoundInclusive, 
            upperBoundInclusive,
            publishMessage -> send(publishMessage, clientMachine, 0));
        if (numMessages != 0 || forceLogging) {
            LOGGER.log(Level.INFO, String.format("Download messages to client: clientMachine=%s, trigger=%s, numMessagesDownloaded=%d",
                                                 clientMachine.getMachineId(),
                                                 trigger,
                                                 numMessages));
        }
    }

    private void sendRequestIdentification(AsynchronousSocketChannel channel, MessageBase message) {
        ClientMachine clientMachine = ClientMachine.unregistered(channel);
        RequestIdentification request = new RequestIdentification(message.getClass(), extractClientIndex(message));
        send(request, clientMachine, 0);
    }

    private void sendInvalidRelayMessage(ClientMachine clientMachine, RelayMessageBase relayMessage, String error) {
        InvalidRelayMessage invalid = new InvalidRelayMessage(relayMessage.getClientIndex(), error);
        send(invalid, clientMachine, 0);
    }
    
    private void send(MessageBase message, ClientMachine clientMachine, int retry) {
        if (clientMachine.getWriteManager().acquireWriteLock(message)) {
            internalSend(message, clientMachine, retry);
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
            // COVERAGE: missed
            LOGGER.log(Level.WARNING,
                       String.format("Send message failed: clientMachine=%s, retry=%d, retryDone=%b",
                                     clientMachine.getMachineId(), retry, true),
                       e);
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
        // COVERAGE: missed
        boolean retryDone = retry >= MAX_RETRIES || SocketTransformer.isClosed(e) || e instanceof RuntimeException || e instanceof Error;
        Level level = retryDone ? Level.WARNING : Level.TRACE;
        LOGGER.log(level, () -> String.format("Send message failed: clientMachine=%s, retry=%d, retryDone=%b",
                                              clientMachine.getMachineId(), retry, retryDone),
                   e);
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
     * <p>This function is only called for valid messages received, even if processing that message results in an error.
     * Basically, it is not called for class types that the server does not support.
     */
    protected void onValidMessageReceived(MessageBase message) {
    }
    
    protected final Stream<ClientMachine> getRemoteClientsStream() {
        return clientMachines.stream();
    }

    private enum ErrorMessageEnum {
        /**
         * Server already saw this message and gave it a serverIndex.
         * Yet client sent this message back to the server.
         */
        MESSAGE_ALREADY_PROCESSED("Message already processed by server: clientIndex=%s");
        
        private final String formatString;

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
            LOGGER.log(Level.INFO, "Shutting down " + DistributedMessageServer.class.getSimpleName() + ": serverAddress=" + getLocalAddress(channel));
            LOGGER.log(Level.TRACE, "Call stack at creation:" + getCallStack());
            closeExecutorQuietly(acceptExecutor);
            closeExecutorQuietly(channelExecutor);
            closeExecutorQuietly(retryExecutor);
            clientMachines.forEach(clientMachine -> closeQuietly(clientMachine.channel));
            closeQuietly(channel);
        }
    }
}

