package org.sn.myutils.pubsub;

import static org.sn.myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static org.sn.myutils.pubsub.PubSubUtils.closeQuietly;
import static org.sn.myutils.pubsub.PubSubUtils.computeExponentialBackoff;
import static org.sn.myutils.pubsub.PubSubUtils.extractClientIndex;
import static org.sn.myutils.pubsub.PubSubUtils.getLocalAddress;
import static org.sn.myutils.pubsub.PubSubUtils.getRemoteAddress;
import static org.sn.myutils.util.concurrent.MoreExecutors.createThreadFactory;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.NetworkChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.Nullable;
import org.sn.myutils.pubsub.MessageClasses.AddOrRemoveSubscriber;
import org.sn.myutils.pubsub.MessageClasses.AddSubscriber;
import org.sn.myutils.pubsub.MessageClasses.AddSubscriberFailed;
import org.sn.myutils.pubsub.MessageClasses.ClientAccepted;
import org.sn.myutils.pubsub.MessageClasses.ClientGeneratedMessage;
import org.sn.myutils.pubsub.MessageClasses.ClientRejected;
import org.sn.myutils.pubsub.MessageClasses.CreatePublisher;
import org.sn.myutils.pubsub.MessageClasses.CreatePublisherFailed;
import org.sn.myutils.pubsub.MessageClasses.DownloadFailed;
import org.sn.myutils.pubsub.MessageClasses.DownloadPublishedMessagesByClientTimestamp;
import org.sn.myutils.pubsub.MessageClasses.DownloadPublishedMessagesByServerId;
import org.sn.myutils.pubsub.MessageClasses.FetchPublisher;
import org.sn.myutils.pubsub.MessageClasses.Identification;
import org.sn.myutils.pubsub.MessageClasses.InternalServerError;
import org.sn.myutils.pubsub.MessageClasses.InvalidRelayMessage;
import org.sn.myutils.pubsub.MessageClasses.MakeServerThrowAnException;
import org.sn.myutils.pubsub.MessageClasses.MessageBase;
import org.sn.myutils.pubsub.MessageClasses.MessageWrapper;
import org.sn.myutils.pubsub.MessageClasses.PublishMessage;
import org.sn.myutils.pubsub.MessageClasses.PublisherCreated;
import org.sn.myutils.pubsub.MessageClasses.RelayFields;
import org.sn.myutils.pubsub.MessageClasses.RelayMessageBase;
import org.sn.myutils.pubsub.MessageClasses.RelayMessageWrapper;
import org.sn.myutils.pubsub.MessageClasses.RelayTopicMessageBase;
import org.sn.myutils.pubsub.MessageClasses.RemoveSubscriber;
import org.sn.myutils.pubsub.MessageClasses.RemoveSubscriberFailed;
import org.sn.myutils.pubsub.MessageClasses.RequestIdentification;
import org.sn.myutils.pubsub.MessageClasses.Resendable;
import org.sn.myutils.pubsub.MessageClasses.SubscriberAdded;
import org.sn.myutils.pubsub.MessageClasses.SubscriberRemoved;
import org.sn.myutils.pubsub.MessageClasses.UnhandledMessage;
import org.sn.myutils.util.ExceptionUtils;
import org.sn.myutils.util.MoreCollections;
import org.sn.myutils.util.ZipMinIterator;


/**
 * Server class that receives messages from a client and relays it to all subscribed to the topic.
 * When a client connects, they send an Identification message identifying their name, and the name must be unique.
 * A client can then send createPublisher and `publisher.publish` commands, and they will be relayed to other clients who are subscribed to the topic.
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
 * <p>Thereafter clients may send createPublisher, `publisher.publisher`, subscribe, unsubscribe, or other commands,
 * and these will be relayed to all other clients.
 * Upon receiving a message to relay, the server generates a monotonically  increasing integer and sets the machineId of the machine which sent the message.
 * These are part of the message sent to each client who is subscribed to the topic.
 * The server id is guaranteed to be unique monotonically increasing even if the server is restarted,
 * so long as all machines have the correct time,
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
 * <p>The server caches the last N messages of each RetentionPriority.
 * Clients can download all publish message commands from a particular server index, and all messages in the cache from this time up to the time of download
 * will be sent to that client.
 * 
 * <p>About messages sent between client and server if using a socket:
 * The first two bytes are the length of the message.
 * The next N bytes is the message, when serialized and converted to a byte stream.
 */
public class DistributedMessageServer extends Shutdowneable {
    private static final System.Logger LOGGER = System.getLogger(DistributedMessageServer.class.getName());
    private static final int NUM_CHANNEL_THREADS = 4;
    private static final int MAX_RETRIES = 3;

    private final SocketTransformer socketTransformer;
    private final SocketAddress messageServer;
    private final AsynchronousServerSocketChannel asyncServerSocketChannel;
    private final ExecutorService acceptExecutor;
    private final ExecutorService channelExecutor;
    private final ScheduledExecutorService retryExecutor;
    private final List<ClientMachine> clientMachines = new CopyOnWriteArrayList<>();
    private final AtomicReference<ServerIndex> maxMessage = new AtomicReference<>(new ServerIndex(CentralServerId.createDefaultFromNow()));
    private final PublishersAndSubscribers publishersAndSubscribers;

    /**
     * Class representing a remote machine.
     * Key fields are machineId (a string) and channel (an AsynchronousSocketChannel).
     */
    protected static final class ClientMachine {
        private static ClientMachine unregistered(@NotNull AsynchronousSocketChannel channel) {
            return new ClientMachine(new ClientMachineId("<unregistered>"), channel);
        }
        
        private final @NotNull ClientMachineId machineId;
        private final @NotNull String remoteAddress;
        private final @NotNull AsynchronousSocketChannel channel;
        private final @NotNull WriteManager writeManager = new WriteManager();

        private ClientMachine(@NotNull ClientMachineId machineId, @NotNull AsynchronousSocketChannel channel) {
            this.machineId = machineId;
            this.remoteAddress = getRemoteAddress(channel);
            this.channel = channel;
        }

        private @NotNull AsynchronousSocketChannel getChannel() {
            return channel;
        }
        
        @NotNull ClientMachineId getMachineId() {
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
            if (!(thatObject instanceof ClientMachine that)) {
                return false;
            }
            return this.machineId.equals(that.machineId);
        }

        @NotNull WriteManager getWriteManager() {
            return writeManager;
        }

        /**
         * Class to ensure that only one threads tries to write to a channel at one time,
         * otherwise we will encounter WritePendingException.
         */
        static class WriteManager {
            private final AtomicBoolean writeLock = new AtomicBoolean();
            private final Queue<MessageWrapper> writeQueue = new LinkedList<>();

            /**
             * Acquire a write lock on this channel.
             * But if it is not available, add the message to send to the write queue.
             */
            synchronized boolean acquireWriteLock(@NotNull MessageWrapper wrapper) {
                boolean acquired = writeLock.compareAndSet(false, true);
                if (!acquired) {
                    writeQueue.add(wrapper);
                }
                return acquired;
            }

            /**
             * This function is called with the write lock held.
             * If the write queue is not empty, return the head of it and keep the write lock held.
             * If it is empty, release the lock.
             */
            @Nullable synchronized MessageWrapper returnHeadOfHeadQueueOrReleaseLock() {
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
            @SuppressWarnings("unused") private final String topic;
            private final ReentrantLock lock = new ReentrantLock();
            private CreatePublisher createPublisher;
            private final List<SubscriberEndpoint> subscriberEndpoints = new ArrayList<>(); // unique by ClientMachine, subscriberName; sorted by ClientMachine, clientTimestamp
            private Set<ClientMachineId> notifyClients; // clients to notify when a publisher is created
            private final Collection<SubscriberEndpoint> inactiveSubscriberEndpoints = new HashSet<>();
            private final MostRecentMessages mostRecentMessages;
            private final Queue<DeferredPublishMessage> deferredPublishMessages = new LinkedList<>();

            /**
             * This code may be hit if server is restarted.
             * Clients send their CreatePublisher and PublishMessage commands to the server.
             * It is possible that a PublishMessage is received before the publisher/topic is created on the server,
             * so we add the Publish to a deferred list, and publish these messages upon the publisher getting created.
             */
            private record DeferredPublishMessage(PublishMessage publishMessage,
                                                  Consumer<SubscriberParamsForCallback> relayAction) {
            }
            
            TopicInfo(String topic, Map<RetentionPriority, Integer> mostRecentMessagesToKeep) {
                this.topic = topic;
                this.mostRecentMessages = new MostRecentMessages(mostRecentMessagesToKeep);
            }
            
            private void setNotifyClientsToNullIfEmpty() {
                if (notifyClients != null && notifyClients.isEmpty()) {
                    notifyClients = null;
                }
            }

            void addDeferredPublishMessage(PublishMessage publishMessage, Consumer<SubscriberParamsForCallback> relayAction) {
                deferredPublishMessages.add(new DeferredPublishMessage(publishMessage, relayAction));
            }
        }
        
        private final Map<RetentionPriority, Integer> mostRecentMessagesToKeep;
        private final Map<String /*topic*/, TopicInfo> topicMap = new ConcurrentHashMap<>();
        
        PublishersAndSubscribers(Map<RetentionPriority, Integer> mostRecentMessagesToKeep) {
            this.mostRecentMessagesToKeep = mostRecentMessagesToKeep;
        }

        /**
         * Acquire a lock and add subscriber to this topic.
         * Note that the publisher may not yet be created.
         * Upon adding the subscriber, invoke the callback.
         */
        @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
        void addSubscriberEndpoint(final String topic,
                                   final String subscriberName,
                                   long clientTimestamp,
                                   ClientMachineId clientMachineId,
                                   Consumer<AddSubscriberResult> afterSubscriberAdded) {
            TopicInfo info = topicMap.computeIfAbsent(topic, ignored -> new TopicInfo(topic, mostRecentMessagesToKeep));
            info.lock.lock();
            try {
                // in case client died and a new one is started, get clientTimestamp from the previous subscriber
                // running time O(N) where N is the number of inactive subscribers for this topic
                // O(1) is possible using HashSet or HashMap lookup as inactiveSubscriberEndpoints is a HashSet
                for (var iter = info.inactiveSubscriberEndpoints.iterator(); iter.hasNext(); ) {
                    var oldEndpoint = iter.next();
                    if (oldEndpoint.subscriberName().equals(subscriberName) && oldEndpoint.clientMachineId().equals(clientMachineId)) {
                        iter.remove();
                        clientTimestamp = oldEndpoint.clientTimestamp();
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
                info.subscriberEndpoints.sort(Comparator.comparing(SubscriberEndpoint::clientMachineId)
                                                        .thenComparing(SubscriberEndpoint::clientTimestamp));
                
                // remove client machine from notifyClients as clients who subscribe to a topic are always notified when the publisher is created
                // running time O(N) where N is the number of clients wanting a notification when the publisher is created
                if (info.notifyClients != null) {
                    info.notifyClients.removeIf(c -> c.equals(clientMachineId));
                    info.setNotifyClientsToNullIfEmpty();
                }
                
                var addSubscriberResult = new AddSubscriberResult(info.createPublisher, clientMachineAlreadySubscribedToTopic, clientTimestamp);
                
                afterSubscriberAdded.accept(addSubscriberResult);
            } finally {
                info.lock.unlock();
            }
        }
        
        private boolean isClientMachineAlreadySubscribedToTopic(TopicInfo info, ClientMachineId clientMachineId) {
            if (info.createPublisher != null && info.createPublisher.getRelayFields().getSourceMachineId().equals(clientMachineId)) {
                return true;
            }
            return info.subscriberEndpoints.stream().anyMatch(endpoint -> endpoint.clientMachineId().equals(clientMachineId));
        }

        private record AddSubscriberResult(@Nullable MessageClasses.CreatePublisher createPublisher,
                                           boolean clientMachineAlreadySubscribedToTopic,
                                           long clientTimestamp) {
        }

        public void removeSubscriberEndpoint(String topic, ClientMachineId clientMachineId, String subscriberName) {
            TopicInfo info = Objects.requireNonNull(topicMap.get(topic));
            info.lock.lock();
            try {
                info.subscriberEndpoints.removeIf(subscriberEndpoint -> subscriberEndpoint.subscriberName().equals(subscriberName));
                info.inactiveSubscriberEndpoints.removeIf(endpoint -> endpoint.clientMachineId().equals(clientMachineId)
                                                              && endpoint.subscriberName().equals(subscriberName));
                info.mostRecentMessages.removeClientMachineState(clientMachineId);
            } finally {
                info.lock.unlock();
            }
        }
        
        /**
         * Add a client to notify upon the publisher getting created.
         * Does not add if there is already a subscriber and the publisher has not yet been created,
         * as the client will get notified anyway upon the publisher getting created.
         *
         * @return the CreatePublisher if one exists
         */
        CreatePublisher maybeAddNotifyClient(String topic, ClientMachineId clientMachineId) {
            TopicInfo info = topicMap.computeIfAbsent(topic, ignored -> new TopicInfo(topic, mostRecentMessagesToKeep));
            info.lock.lock();
            try {
                if (info.createPublisher == null
                        && info.subscriberEndpoints.stream().anyMatch(subscriberEndpoint -> subscriberEndpoint.clientMachineId().equals(clientMachineId))) {
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
            } finally {
                info.lock.unlock();
            }
        }

        private record SavePublisherCallbacks(Consumer<SubscriberParamsForCallback> relayAction,
                                              Runnable finalizerAction) {
        }
        
        /**
         * Add a publisher.
         * Followup actions are to notify clients who subscribed to this topic of the new publisher.
         * Also publish deferred messages.
         * 
         * @param createPublisher the create publisher command sent from the client
         * @param onPublisherCreatedCallback the function to call once the publisher is created.
         *        Not called if there is an exception.
         *        Return relayAction if we should relay this createPublisher command to other clients; this function is called on each client subscriber.
         *        Return finalizerAction if we should confirm the CreatePublisher command to the client who sent it.
         */
        void savePublisher(CreatePublisher createPublisher,
                           Function<CreatePublisherResult, SavePublisherCallbacks> onPublisherCreatedCallback) {
            var topic = createPublisher.getTopic();
            TopicInfo info = topicMap.computeIfAbsent(topic, ignored -> new TopicInfo(topic, mostRecentMessagesToKeep));
            info.lock.lock();
            try {
                CreatePublisherResult result;
                if (info.createPublisher == null) {
                    info.createPublisher = createPublisher;
                    result = new CreatePublisherResult(null);
                } else {
                    result = new CreatePublisherResult(info.createPublisher);
                }
                SavePublisherCallbacks callbacks = onPublisherCreatedCallback.apply(result);
                if (callbacks.relayAction != null) {
                    forClientsSubscribedToPublisher(createPublisher.getTopic(),
                                                    createPublisher.getRelayFields().getSourceMachineId() /*excludeMachineId*/,
                                                    true /*fetchClientsWantingNotification*/,
                                                    callbacks.relayAction);
                }
                if (callbacks.finalizerAction != null) {
                    callbacks.finalizerAction.run();
                }
                sendDeferredPublishMessages(info);
            } finally {
                info.lock.unlock();
            }
        }
        
        private void sendDeferredPublishMessages(TopicInfo info) {
            while (true) {
                var deferred = info.deferredPublishMessages.poll();
                if (deferred == null) {
                    break;
                }
                saveMessage(deferred.publishMessage, deferred.relayAction);
            }
        }

        record CreatePublisherResult(@Nullable MessageClasses.CreatePublisher alreadyExistsCreatePublisher) {
        }

        /**
         * A channel has been closed.
         * Mark all subscriber endpoints for this channel as inactive.
         * 
         * @return a list of topics that were unsubscribed from (but not the number of subscribers for each topic), or ? if INFO level is not enabled
         */
        StringBuilder removeClientMachine(ClientMachineId clientMachineId) {
            boolean returnTopicsAffected = LOGGER.isLoggable(Level.INFO);
            StringBuilder topicsAffected = new StringBuilder();
            if (returnTopicsAffected) {
                topicsAffected.append('[');
            }
            for (var entry : topicMap.entrySet()) {
                TopicInfo info = entry.getValue();
                info.lock.lock();
                try {
                    int removeCount = 0;
                    for (var iter = info.subscriberEndpoints.iterator(); iter.hasNext(); ) {
                        var endpoint = iter.next();
                        if (endpoint.clientMachineId().equals(clientMachineId)) {
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
                } finally {
                    info.lock.unlock();
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
         * Must be called with lock held.
         * 
         * @param topic retrieve clientMachines subscribed to this topic or who want a notification
         * @param excludeMachineId exclude this machine (used to not relay message to client who sent the message)
         * @param fetchClientsWantingNotification if true fetch client machines that want a notification (used for clients who want to download a publisher but not subscribe to it)
         * @param consumer apply this action to each client machine.
         *        The 2nd argument is a struct consisting of
         *          - the subscriber minimum client timestamp (useful if one client machine has many subscribers),
         *            and is null if we are only notifying a client (i.e. for the fetchPublisher command).  
         *          - the current max message id for this subscriber.
         */
        private void forClientsSubscribedToPublisher(String topic,
                                                     @Nullable ClientMachineId excludeMachineId,
                                                     boolean fetchClientsWantingNotification,
                                                     Consumer<SubscriberParamsForCallback> consumer) {
            TopicInfo info = Objects.requireNonNull(topicMap.get(topic));
            info.lock.lock();
            try {
                if (info.createPublisher == null) {
                    return;
                }
                ClientMachineId prevClientMachineId = null;
                for (SubscriberEndpoint subscriberEndpoint : info.subscriberEndpoints) {
                    if (subscriberEndpoint.clientMachineId().equals(excludeMachineId)) {
                        continue;
                    }
                    if (!subscriberEndpoint.clientMachineId().equals(prevClientMachineId)) {
                        var params = new SubscriberParamsForCallback(subscriberEndpoint.clientMachineId(), subscriberEndpoint.clientTimestamp());
                        consumer.accept(params);
                        prevClientMachineId = subscriberEndpoint.clientMachineId();
                    }
                }
    
                if (fetchClientsWantingNotification && info.notifyClients != null) {
                    Stream<ClientMachineId> notifyClients = info.notifyClients.stream();
                    info.notifyClients = null;
                    notifyClients.forEach(clientMachineId -> consumer.accept(new SubscriberParamsForCallback(clientMachineId, null)));
                }
            } finally {
                info.lock.unlock();
            }
        }

        public void saveMessage(PublishMessage publishMessage, Consumer<SubscriberParamsForCallback> relayAction) {
            String topic = publishMessage.getTopic();
            TopicInfo info = topicMap.computeIfAbsent(topic, ignored -> new TopicInfo(topic, mostRecentMessagesToKeep));
            info.lock.lock();
            try {
                if (info.createPublisher != null) {
                    info.mostRecentMessages.save(publishMessage);
                    relayAction = relayAction.andThen(
                        subscriberParamsForCallback -> info.mostRecentMessages.onMessageRelayed(subscriberParamsForCallback.clientMachineId(), publishMessage));
                    forClientsSubscribedToPublisher(topic,
                                                    publishMessage.getRelayFields().getSourceMachineId(),
                                                    false,
                                                    relayAction);
                } else {
                    info.addDeferredPublishMessage(publishMessage, relayAction);
                }
            }  finally {
                info.lock.unlock();
            }
        }

        /**
         * This function is used to send saved messages to a client.
         *
         * <p>Messages are saved sorted by server index.
         * To find messages between the given server indices, the current implementation searches all messages from start to end
         * till it finds the one whose index is equal to or just less than lowerBoundInclusive.
         * So the running time to find the first record is O(N).
         * Future implementations may use binary search to reduce the time to O(lg(N)).
         *
         * <p>The implementation then scans each record from here till and returns it if it falls within the given client timestamp range.
         * It stops scanning when the record's server index is larger than upperBoundInclusive.
         * So the running time to return all records is O(N).
         * 
         * <p>To find all records between two client timestamps, the client will usually pass in lowerBoundInclusive as null and upperBoundInclusive as MAX_VALUE,
         * so the time to find the first record is still O(N) even if binary search were used.
         * Since messages with increasing client timestamp typically have an increasing server index,
         * a future implementation may use binary search to find the message whose client timestamp is equal to or just greater than minClientTimestamp.
         *
         * <p>Because some messages may be received out of order, a message with a higher server index may have a smaller client timestamp,
         * and so this approach risks not finding all messages.
         * But maybe that's a fact of life, though the implementation could scan a few records to the left or right to try to find more records that match.
         *
         * @param clientMachine the client to send messages to
         * @param topics null means send messages for all topics, otherwise send messages only for these topics
         * @param minClientTimestamp find messages on or after this client timestamp (can be 0)
         * @param maxClientTimestamp find messages on or before this client timestamp (can be Long.MAX_VALUE)
         * @param lowerBoundInclusive send messages from this point. If null, calculate lowerBoundInclusive as the current server index the client is on plus one.
         * @param upperBoundInclusive send messages till this point
         * @param callback function that sends messages
         * @param errorCallback function that sends a message if the download request is invalid
         */
        int forSavedMessages(ClientMachine clientMachine,
                             Collection<String> topics,
                             long minClientTimestamp,
                             long maxClientTimestamp,
                             @Nullable ServerIndex lowerBoundInclusive,
                             @NotNull ServerIndex upperBoundInclusive,
                             Consumer<PublishMessage> callback,
                             @Nullable Consumer<PubSubException> errorCallback) {
            BiFunction<String, TopicInfo, TopicInfo> checkClientSubscribedToTopic = (topic, info) -> {
                if (info == null || !isClientMachineAlreadySubscribedToTopic(info, clientMachine.getMachineId())) {
                    throw new PubSubException(ErrorMessageEnum.CLIENT_NOT_SUBSCRIBED_TO_TOPIC.format(clientMachine.getMachineId(), topic));
                }
                return info;
            };
            
            int count = 0;

            List<Lock> locks = new ArrayList<>(topics.size());
            try {
                List<TopicInfo> infos = topics.stream()
                                              .map(topic -> checkClientSubscribedToTopic.apply(topic, topicMap.get(topic)))
                                              .toList();
                
                // lock all topics
                infos.forEach(info -> {
                    Lock lock = info.lock;
                    lock.lock();
                    locks.add(lock);
                });

                // construct an iterator across all topics and retention priorities
                // the items will be returned in the order of serverIndex
                List<List<PublishMessage>> allMessagesAcrossTopics = new ArrayList<>();
                for (var info : infos) {
                    allMessagesAcrossTopics.addAll(info.mostRecentMessages.getMessagesOfAllRetentionPriorities());
                }
                Comparator<PublishMessage> comparator = (lhs, rhs) -> ServerIndex.compare(lhs.getRelayFields().getServerIndex(), rhs.getRelayFields().getServerIndex()); 
                Iterator<PublishMessage> iter = new ZipMinIterator<>(allMessagesAcrossTopics, comparator);

                // calculate lowerBoundInclusive if not explicitly passed in
                if (lowerBoundInclusive == null) {
                    if (topics.size() != 1) {
                        throw new IllegalArgumentException("lowerBoundInclusive can only be null if downloading messages for one topic");
                    }
                    lowerBoundInclusive = infos.getFirst().mostRecentMessages.getMaxIndex(clientMachine.getMachineId());
                    lowerBoundInclusive = lowerBoundInclusive.increment();
                }
                
                // iterate over all messages and if in range invoke the callback to relay
                while (iter.hasNext()) {
                    PublishMessage message = iter.next();
                    if (message.getRelayFields().getServerIndex().compareTo(upperBoundInclusive) > 0) {
                        break;
                    }
                    if (message.getRelayFields().getServerIndex().compareTo(lowerBoundInclusive) < 0) {
                        continue;
                    }
                    if (minClientTimestamp <= message.getClientTimestamp() && message.getClientTimestamp() <= maxClientTimestamp) {
                        callback.accept(message);
                        count++;
                    }
                }
            } catch (PubSubException e) {
                if (errorCallback != null) {
                    errorCallback.accept(e);
                } else {
                    throw e;
                }
            } finally {
                // unlock all topics
                locks.forEach(PubSubUtils::unlockSafely);
            }
            
            // return the number of messages relayed
            return count;
        }
    }

    /**
     * A struct describing parameters about a subscriber.
     * Used when we fetch subscribers to a topic and want to invoke an action of each them,
     * such as relaying a published message to these subscribers.
     */
    private record SubscriberParamsForCallback(ClientMachineId clientMachineId,
                                               Long minClientTimestamp) {
    }

    /**
     * Class to store what subscribers and client machine has.
     * Unique key is clientMachineId + subscriberName.
     * Cannot make this class a record, because in a record the unique key would be all 3 member variables.
     * One client machine can have two subscribers for the same topic.
     */
    private record SubscriberEndpoint(ClientMachineId clientMachineId, String subscriberName, long clientTimestamp) {
        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof SubscriberEndpoint that)) {
                return false;
            }
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
     * Each topic has one instance of MostRecentMessages.
     * We only save the last N messages of each retention priority.
     */
    private static class MostRecentMessages {
        private final int[] numberOfMostRecentMessagesToKeep;
        private final List<LinkedList<PublishMessage>> allMessages = List.of(new LinkedList<>(), new LinkedList<>()); // MEDIUM priority, HIGH priority
        private final Map<ClientMachineId, ServerIndex /*maxIndex*/> highestIndexMap = new HashMap<>();
        
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

        /**
         * Save a message and call a callback function atomically.
         */
        void save(PublishMessage publishMessage) {
            int ordinal = publishMessage.getRetentionPriority().ordinal();
            LinkedList<PublishMessage> linkedList = allMessages.get(ordinal);
            MoreCollections.addLargeElementToSortedList(linkedList, COMPARE_BY_SERVER_INDEX, publishMessage);
            if (linkedList.size() > numberOfMostRecentMessagesToKeep[ordinal]) {
                linkedList.removeFirst();
            }
        }

        private static final Comparator<PublishMessage> COMPARE_BY_SERVER_INDEX = Comparator.comparing(message -> message.getRelayFields().getServerIndex());
        //private static final Comparator<PublishMessage> COMPARE_BY_CLIENT_TIMESTAMP = Comparator.comparingLong(ClientGeneratedMessage::getClientTimestamp);

        List<LinkedList<PublishMessage>> getMessagesOfAllRetentionPriorities() {
            return allMessages;
        }
        
        void onMessageRelayed(ClientMachineId clientMachineId, PublishMessage publishMessage) {
            setMaxIndexIfLarger(clientMachineId, publishMessage.getRelayFields().getServerIndex());
        }

        /**
         * Function must be called with lock held.
         */
        private @NotNull ServerIndex getMaxIndex(ClientMachineId clientMachineId) {
            ServerIndex index = highestIndexMap.get(clientMachineId);
            return index != null ? index : ServerIndex.MIN_VALUE;
        }

        /**
         * Function must be called with lock held.
         */
        private void setMaxIndexIfLarger(ClientMachineId clientMachineId, ServerIndex newMax) {
            ServerIndex index = highestIndexMap.get(clientMachineId);
            if (index == null || newMax.compareTo(index) > 0) {
                highestIndexMap.put(clientMachineId, newMax);
            }
        }

        /**
         * Function must be called with lock held.
         */
        void removeClientMachineState(ClientMachineId clientMachineId) {
            highestIndexMap.remove(clientMachineId);
        }
    }
    
    /**
     * Create a message server.
     * 
     * @param messageServer the host/port of this server
     * @param mostRecentMessagesToKeep the number of most recent messages of the given priority to keep (and zero if message not in this list)
     * @throws IOException if there is an error opening a socket (but no error if the host:port is already in use)
     */
    public static DistributedMessageServer create(@NotNull SocketAddress messageServer, Map<RetentionPriority, Integer> mostRecentMessagesToKeep) throws IOException {
        return create(messageServer, mostRecentMessagesToKeep, new SocketTransformer());
    }

    static DistributedMessageServer create(@NotNull SocketAddress messageServer,
                                           Map<RetentionPriority, Integer> mostRecentMessagesToKeep,
                                           SocketTransformer socketTransformer) throws IOException {
        var server = new DistributedMessageServer(messageServer, mostRecentMessagesToKeep, socketTransformer);
        server.registerCleanable();
        return server;
    }

    // package private for tests
    DistributedMessageServer(@NotNull SocketAddress messageServer,
                             Map<RetentionPriority, Integer> mostRecentMessagesToKeep,
                             SocketTransformer socketTransformer) throws IOException {
        this.socketTransformer = socketTransformer;
        this.messageServer = messageServer;
        this.asyncServerSocketChannel = AsynchronousServerSocketChannel.open();
        this.acceptExecutor = Executors.newSingleThreadExecutor(createThreadFactory("DistributedMessageServer.accept", true));
        this.channelExecutor = Executors.newFixedThreadPool(NUM_CHANNEL_THREADS, createThreadFactory("DistributedMessageServer.socket", true));
        this.retryExecutor = Executors.newScheduledThreadPool(1, createThreadFactory("DistributedMessageServer.Retry", true));
        this.publishersAndSubscribers = new PublishersAndSubscribers(mostRecentMessagesToKeep);
    }

    @Override
    protected Runnable shutdownAction() {
        return () -> {
            LOGGER.log(Level.INFO, "Details: serverAddress={0}",
                       getLocalAddress(asyncServerSocketChannel));
            closeExecutorQuietly(acceptExecutor);
            closeExecutorQuietly(channelExecutor);
            closeExecutorQuietly(retryExecutor);
            clientMachines.forEach(clientMachine -> closeQuietly(clientMachine.channel));
            closeQuietly(asyncServerSocketChannel);
        };
    }

    public SocketAddress getMessageServerAddress() {
        return messageServer;
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
        try {
            openServerSocket();
            acceptExecutor.submit(new AcceptThread());
            future.complete(null);
        } catch (IOException | RuntimeException | Error e) {
            LOGGER.log(Level.WARNING,
                       String.format("Failed to start DistributedMessageServer centralServer=%s", messageServer.toString()),
                       e);
            future.completeExceptionally(e);
        }
    }

    private void openServerSocket() throws IOException {
        onBeforeSocketBound(asyncServerSocketChannel);
        asyncServerSocketChannel.bind(messageServer);
        LOGGER.log(Level.INFO, String.format("Started DistributedMessageServer: messageServer=%s, messageServerAddress=%s",
                                             messageServer.toString(), getLocalAddress(asyncServerSocketChannel)));
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
            socketTransformer.readMessageFromSocketAsync(channel)
                             .thenApplyAsync(message -> handle(channel, message), channelExecutor)
                             .whenComplete(this::onComplete);
        }

        private @Nullable ClientMachine handle(AsynchronousSocketChannel channel, MessageBase message) {
            ClientMachine clientMachine = null;
            try {
                boolean unhandled = false;
                if (message instanceof ClientGeneratedMessage) {
                    if (message instanceof Identification identification) {
                        LOGGER.log(Level.TRACE,
                                   () -> String.format("Received message from client: clientAddress=%s, %s",
                                                       getRemoteAddress(channel),
                                                       message.toLoggingString()));
                        onValidMessageReceived(message);
                        DistributedMessageServer.this.addIfNotPresent(identification, channel);
                    } else {
                        clientMachine = findClientMachineByChannel(channel);
                        if (clientMachine == null) {
                            LOGGER.log(Level.TRACE,
                                       String.format("Received message from unknown client: clientAddress=%s, %s",
                                                     getRemoteAddress(channel),
                                                     message.toLoggingString()));
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

                            if (message instanceof RelayMessageBase relayMessage
                                    && relayMessage.getRelayFields() != null
                                    && (!(message instanceof Resendable) || !((Resendable) message).isResend())) {
                                DistributedMessageServer.this.sendInvalidRelayMessage(clientMachine,
                                                                                      relayMessage,
                                                                                      ErrorMessageEnum.MESSAGE_ALREADY_PROCESSED.format(relayMessage.getClientIndex()));
                            } else if (message instanceof FetchPublisher fetchPublisher) {
                                logging.run();
                                DistributedMessageServer.this.handleFetchPublisher(clientMachine, fetchPublisher.getTopic());
                            } else if (message instanceof AddSubscriber addSubscriber) {
                                logging.run();
                                DistributedMessageServer.this.handleAddSubscriber(clientMachine, addSubscriber);
                            } else if (message instanceof RemoveSubscriber removeSubscriber) {
                                logging.run();
                                DistributedMessageServer.this.handleRemoveSubscriber(clientMachine, removeSubscriber);
                            } else if (message instanceof RelayMessageBase relay) {
                                if (relay.getRelayFields() == null) {
                                    ServerIndex nextServerId = maxMessage.updateAndGet(ServerIndex::increment);
                                    relay.setRelayFields(new RelayFields(System.currentTimeMillis(), nextServerId, clientMachine.getMachineId()));
                                }
                                logging.run();
                                DistributedMessageServer.this.handleRelayMessage(clientMachine, relay);
                            } else if (message instanceof DownloadPublishedMessagesByServerId downloadPublishedMessagesByServerId) {
                                logging.run();
                                DistributedMessageServer.this.handleDownload(clientMachine, downloadPublishedMessagesByServerId);
                            } else if (message instanceof DownloadPublishedMessagesByClientTimestamp downloadPublishedMessagesByClientTimestamp) {
                                logging.run();
                                DistributedMessageServer.this.handleDownload(clientMachine, downloadPublishedMessagesByClientTimestamp);
                            } else {
                                unhandled = true;
                            }
                        }
                    }
                } else {
                    clientMachine = findClientMachineByChannel(channel);
                    unhandled = true;
                }
                if (message instanceof MakeServerThrowAnException) {
                    LOGGER.log(Level.TRACE,
                               String.format("Received message from client: clientAddress=%s, clientMachine=%s, %s",
                                             getRemoteAddress(channel),
                                             clientMachine != null ? clientMachine.getMachineId() : "<unknown>",
                                             message.toLoggingString()));
                    onValidMessageReceived(message);
                    throw new RuntimeException("Random error");
                }
                if (unhandled) {
                    if (clientMachine == null) {
                        clientMachine = ClientMachine.unregistered(channel);
                    }
                    LOGGER.log(Level.WARNING,
                               String.format("Unsupported message from client: clientMachine=%s, %s",
                                             clientMachine,
                                             message.toLoggingString()));
                    DistributedMessageServer.this.wrapAndSend(new UnhandledMessage(message.getClass(), extractClientIndex(message)), clientMachine);
                }
            } catch (RuntimeException e) {
                DistributedMessageServer.this.sendInternalServerError(channel, clientMachine, e);
                throw e;
            }
            return clientMachine;
        }
        
        private void onComplete(@Nullable ClientMachine clientMachine, Throwable exception) {
            if (exception != null) {
                if (SocketTransformer.isClosed(exception)) {
                    logChannelClosed(exception);
                    removeChannel(channel);
                    return;
                } else if (exception instanceof SocketTransformer.ReadSocketException) {
                    logException("Error reading from remote socket", clientMachine, exception);
                    DistributedMessageServer.this.wrapAndSend(new InternalServerError(exception), clientMachine);
                } else {
                    logException("Error processing message", clientMachine, exception);
                    DistributedMessageServer.this.wrapAndSend(new InternalServerError(exception), clientMachine);
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
        
        private void logException(String topLevelMessage, @Nullable ClientMachine clientMachine, Throwable e) {
            LOGGER.log(Level.WARNING,
                       String.format(topLevelMessage + ": clientMachine=%s",
                                     clientMachine != null ? clientMachine.getMachineId() : getRemoteAddress(channel)),
                       e);
        }
    }
    
    /**
     * Add a client machine to the topology and send a ClientAccepted message.
     * If a machine with this id is already present, we send a ClientRejected and log an error.
     * If a machine with this channel is already present, we send a ClientRejected and log an error.
     * 
     * @param identification the identification sent by the client
     * @param channel the channel the message was sent on
     */
    private void addIfNotPresent(Identification identification, AsynchronousSocketChannel channel) {
        if (findClientMachineByChannel(channel) != null) {
            var clientRejected = new ClientRejected(maxMessage.get().extractCentralServerId(), ErrorMessageEnum.CHANNEL_ALREADY_REGISTERED.format(getRemoteAddress(channel)));
            LOGGER.log(Level.ERROR, clientRejected.toLoggingString());
            wrapAndSend(clientRejected, ClientMachine.unregistered(channel));
            return;
        }
        ClientMachine existingClientMachine = findClientMachineByMachineId(identification.getMachineId());
        if (existingClientMachine != null) {
            var clientRejected = new ClientRejected(maxMessage.get().extractCentralServerId(),
                                                    ErrorMessageEnum.DUPLICATE_CLIENT_MACHINE_ID.format(identification.getMachineId(),
                                                                                                        getRemoteAddress(existingClientMachine.getChannel())));
            LOGGER.log(Level.ERROR, clientRejected.toLoggingString());
            wrapAndSend(clientRejected, ClientMachine.unregistered(channel));
            return;
        }
        
        setChannelOptions(channel);
        var clientMachine = new ClientMachine(identification.getMachineId(), channel);
        clientMachines.add(clientMachine);
        LOGGER.log(Level.INFO, "Added client machine: clientMachine={0}, clientAddress={1}", clientMachine.getMachineId(), getRemoteAddress(channel));
        var clientAccepted = new ClientAccepted(maxMessage.get().extractCentralServerId());
        wrapAndSend(clientAccepted, clientMachine);
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
            wrapAndSend(createPublisher, clientMachine);
        }
    }
    
    private void handleAddSubscriber(ClientMachine clientMachine, AddSubscriber subscriberInfo) {
        String topic = subscriberInfo.getTopic();
        String subscriberName = subscriberInfo.getSubscriberName();        
        
        String reason = canSubscribe(subscriberInfo);
        if (reason != null) {
            wrapAndSend(new AddSubscriberFailed(ErrorMessageEnum.CANNOT_SUBSCRIBE.format(subscriberInfo.getTopic(), subscriberInfo.getSubscriberName(), reason),
                                                topic,
                                                subscriberName),
                        clientMachine
            );
            return;
        }

        Consumer<PublishersAndSubscribers.AddSubscriberResult> afterSubscriberAdded = addSubscriberResult -> {
            boolean doDownload = false;
            boolean forceLogging = subscriberInfo.isResend();
            long clientTimestamp = addSubscriberResult.clientTimestamp();
            boolean sendingPublisher = false;
            if (addSubscriberResult.createPublisher() != null && !addSubscriberResult.clientMachineAlreadySubscribedToTopic()) {
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
            if (!subscriberInfo.isResend()) {
                wrapAndSend(new SubscriberAdded(topic, subscriberName), clientMachine);
            }
            if (sendingPublisher) {
                wrapAndSend(addSubscriberResult.createPublisher(), clientMachine);
            }
            if (doDownload) {
                download("handleAddSubscriber",
                         /*downloadClientCommandIndex*/ null,
                         clientMachine,
                         Collections.singletonList(topic),
                         clientTimestamp,
                         Long.MAX_VALUE,
                         null,
                         ServerIndex.MAX_VALUE,
                         null,
                         forceLogging);
            }
        };
        
        publishersAndSubscribers.addSubscriberEndpoint(topic,
                                                       subscriberName,
                                                       subscriberInfo.getClientTimestamp(),
                                                       clientMachine.getMachineId(),
                                                       afterSubscriberAdded);
    }
    
    private void handleRemoveSubscriber(ClientMachine clientMachine, RemoveSubscriber subscriberInfo) {
        String topic = subscriberInfo.getTopic();
        String subscriberName = subscriberInfo.getSubscriberName();

        String reason = canSubscribe(subscriberInfo);
        if (reason != null) {
            wrapAndSend(new RemoveSubscriberFailed(ErrorMessageEnum.CANNOT_SUBSCRIBE.format(subscriberInfo.getTopic(), subscriberInfo.getSubscriberName(), reason),
                                                   topic,
                                                   subscriberName),
                        clientMachine
            );
            return;
        }

        publishersAndSubscribers.removeSubscriberEndpoint(topic, clientMachine.getMachineId(), subscriberName);
        wrapAndSend(new SubscriberRemoved(topic, subscriberName), clientMachine);
        LOGGER.log(Level.INFO, "Removed subscriber : topic={0} subscriberName={1} clientMachine={2}", topic, subscriberName, clientMachine.getMachineId());
    }

    private void handleRelayMessage(@NotNull ClientMachine clientMachine, RelayMessageBase relay) {
        if (relay instanceof RelayTopicMessageBase) {
            if (relay instanceof CreatePublisher createPublisher) {
                handleCreatePublisher(clientMachine, createPublisher);
            } else if (relay instanceof PublishMessage publishMessage) {
                handlePublishMessage(publishMessage);
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }
    
    private void handleCreatePublisher(ClientMachine clientMachine, CreatePublisher createPublisher) {
        String topic = createPublisher.getTopic();
        
        String reason = canCreatePublisher(createPublisher);
        if (reason != null) {
            wrapAndSend(new CreatePublisherFailed(ErrorMessageEnum.CANNOT_CREATE_PUBLISHER.format(topic, reason),
                                                  createPublisher.getClientIndex(),
                                                  topic),
                 clientMachine
            );
            return;
        }
        
        Function<PublishersAndSubscribers.CreatePublisherResult, PublishersAndSubscribers.SavePublisherCallbacks> onPublisherCreatedCallback = createPublisherResult -> {
            boolean skipRelayMessage = false;
            boolean isResendPublisher = false;
            if (createPublisherResult.alreadyExistsCreatePublisher == null) {
                LOGGER.log(Level.INFO, "Added publisher: topic={0}, topicClass={1}", topic, createPublisher.getPublisherClass().getSimpleName());
                if (createPublisher.isResend()) {
                    isResendPublisher = true;
                }
            } else {
                CreatePublisher alreadyExistsCreatePublisher = createPublisherResult.alreadyExistsCreatePublisher;
                LOGGER.log(Level.INFO,
                           String.format("Publisher already exists: topic=%s, topicClass=%s, clientTimestamp=%d, serverIndex=%s, discarding newServerIndex=%s",
                                         alreadyExistsCreatePublisher.getTopic(),
                                         alreadyExistsCreatePublisher.getPublisherClass().getSimpleName(),
                                         alreadyExistsCreatePublisher.getClientTimestamp(),
                                         alreadyExistsCreatePublisher.getRelayFields().getServerIndex().toString(),
                                         createPublisher.getRelayFields().getServerIndex()));
                skipRelayMessage = true;
            }
            Consumer<SubscriberParamsForCallback> actionForEachOtherClient;
            if (!skipRelayMessage) {
                actionForEachOtherClient = relayCreatePublisherToOtherClientsAction(createPublisher, isResendPublisher);
            } else {
                actionForEachOtherClient = null;
            }
            Runnable finalizerAction;
            if (!createPublisher.isResend()) {
                finalizerAction = () -> wrapAndSend(new PublisherCreated(createPublisher.getTopic(), createPublisher.getRelayFields()), clientMachine);
            } else {
                finalizerAction = null;
            }
            return new PublishersAndSubscribers.SavePublisherCallbacks(actionForEachOtherClient, finalizerAction);
        };
        publishersAndSubscribers.savePublisher(createPublisher, onPublisherCreatedCallback);
    }
    
    private Consumer<SubscriberParamsForCallback> relayCreatePublisherToOtherClientsAction(CreatePublisher relay, boolean isResendPublisher) {
        if (!isResendPublisher) {
            // relay CreatePublisher to clients subscribed to this topic
            return params -> {
                ClientMachine clientMachine = lookupClientMachine(params.clientMachineId());
                wrapAndSend(relay, clientMachine);
            };
        } else {
            // client is resending publisher after a server restart
            // download all messages in the cache from the subscriber timestamp
            // this handles the case that (a) server dies, (b) clients publish messages, (c) server restarts,
            // and (d) clients send the messages and resend their AddSubscriber and CreatePublisher commands to the server
            // the messages published since the server died (b) need to be sent to all subscribers
            return params -> {
                if (params.minClientTimestamp() == null) {
                    // this block called for clients just wanting notification of a publisher
                    wrapAndSend(relay, lookupClientMachine(params.clientMachineId()));
                } else {
                    download("handleCreatePublisher",
                             null,
                             lookupClientMachine(params.clientMachineId()),
                             Collections.singletonList(relay.getTopic()),
                             params.minClientTimestamp(),
                             Long.MAX_VALUE,
                             null /*lowerBoundInclusive*/,
                             ServerIndex.MAX_VALUE,
                             null,
                             true /*forceLogging*/);
                }
            };
        }
    }
    
    private void handlePublishMessage(PublishMessage relay) {
        Consumer<SubscriberParamsForCallback> relayAction = params -> {
            var otherClientMachine = lookupClientMachine(params.clientMachineId());
            wrapAndSend(relay, otherClientMachine);
        };
        publishersAndSubscribers.saveMessage(relay, relayAction);
    }

    private void handleDownload(ClientMachine clientMachine, DownloadPublishedMessagesByServerId download) {
        download(
            "downloadByServerId",
            download.getCommandIndex(),
            clientMachine,
            download.getTopics(),
            Long.MIN_VALUE /*minClientTimestamp*/,
            Long.MAX_VALUE /*maxClientTimestamp*/,
            download.getStartServerIndexInclusive(),
            download.getEndServerIndexInclusive(),
            exception -> wrapAndSend(new DownloadFailed(download.getCommandIndex(),
                                                        download.toLoggingString(),
                                                        exception.getMessage()),
                                     clientMachine),
            /*forceLogging*/ true);
    }

    private void handleDownload(ClientMachine clientMachine, DownloadPublishedMessagesByClientTimestamp download) {
        download(
            "downloadByClientTimestamp",
            download.getCommandIndex(),
            clientMachine,
            download.getTopics(),
            download.getStartInclusive() /*minClientTimestamp*/,
            download.getEndInclusive() /*maxClientTimestamp*/,
            ServerIndex.MIN_VALUE,
            ServerIndex.MAX_VALUE,
            exception -> wrapAndSend(new DownloadFailed(download.getCommandIndex(),
                                                        download.toLoggingString(),
                                                        exception.getMessage()),
                                     clientMachine),
            /*forceLogging*/ true);
    }

    private void download(@NotNull String trigger,
                          @Nullable Integer downloadCommandIndex,
                          ClientMachine clientMachine,
                          Collection<String> topics,
                          long minClientTimestamp,
                          long maxClientTimestamp,
                          @Nullable ServerIndex lowerBoundInclusive,
                          @NotNull ServerIndex upperBoundInclusive,
                          @Nullable Consumer<PubSubException> errorCallback,
                          boolean forceLogging) {
        int numMessages = publishersAndSubscribers.forSavedMessages(
            clientMachine,
            topics,
            minClientTimestamp,
            maxClientTimestamp,
            lowerBoundInclusive, 
            upperBoundInclusive,
            publishMessage -> send(new RelayMessageWrapper(publishMessage, downloadCommandIndex), clientMachine, 0),
            errorCallback);
        if (numMessages != 0 || forceLogging) {
            LOGGER.log(Level.INFO, String.format("Download messages to client: clientMachine=%s, trigger=%s,  numMessagesDownloaded=%d",
                                                 clientMachine.getMachineId(),
                                                 trigger,
                                                 numMessages));
        }
    }

    private void sendInternalServerError(AsynchronousSocketChannel channel, ClientMachine clientMachine, RuntimeException exception) {
        if (clientMachine == null) {
            clientMachine = ClientMachine.unregistered(channel);
        }
        InternalServerError request = new InternalServerError(exception);
        wrapAndSend(request, clientMachine);
    }

    private void sendRequestIdentification(AsynchronousSocketChannel channel, MessageBase message) {
        ClientMachine clientMachine = ClientMachine.unregistered(channel);
        RequestIdentification request = new RequestIdentification(message.getClass(), extractClientIndex(message));
        wrapAndSend(request, clientMachine);
    }

    private void sendInvalidRelayMessage(ClientMachine clientMachine, RelayMessageBase relayMessage, String error) {
        InvalidRelayMessage invalid = new InvalidRelayMessage(error, relayMessage.getClientIndex());
        wrapAndSend(invalid, clientMachine);
    }
    
    private void wrapAndSend(MessageBase message, ClientMachine clientMachine) {
        send(new MessageWrapper(message), clientMachine, 0);
    }
    
    private void send(MessageWrapper wrapper, ClientMachine clientMachine, int retry) {
        if (clientMachine.getWriteManager().acquireWriteLock(wrapper)) {
            internalSend(wrapper, clientMachine, retry);
        }
    }
    
    /**
     * Send a message to client asynchronously.
     * If we are already writing another message to the client machine, the message to added to a queue.
     * Upon sending a message, this function sends the first of any queued messages by calling itself.
     */
    private void internalSend(MessageWrapper wrapper, ClientMachine clientMachine, int retry) {
        try {
            socketTransformer.writeMessageToSocketAsync(wrapper, Short.MAX_VALUE, clientMachine.getChannel())
                             .thenAcceptAsync(ignored -> afterMessageSent(clientMachine, wrapper), channelExecutor)
                             .exceptionally(e -> retrySend(wrapper, clientMachine, retry, e))
                             .thenRun(() -> sendQueuedMessageOrReleaseLock(clientMachine));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,
                       String.format("Send message failed: clientMachine=%s, messageClass=%s, retry=%d, retryDone=%b",
                                     clientMachine.getMachineId(), wrapper.getMessage().getClass().getSimpleName(), retry, true),
                       e);
        }
    }
    
    private void afterMessageSent(ClientMachine clientMachine, MessageWrapper wrapper) {
        LOGGER.log(
            Level.TRACE,
            () -> String.format("Sent message to client: clientMachine=%s, %s",
                                clientMachine.getMachineId(),
                                wrapper.toLoggingString()));
        onMessageSent(wrapper);
    }
    
    private Void retrySend(MessageWrapper wrapper, ClientMachine clientMachine, int retry, Throwable throwable) {
        Throwable e = ExceptionUtils.unwrapCompletionException(throwable);
        if (e instanceof SocketTransformer.WriteSocketException) {
            e = e.getCause();
        }
        boolean retryDone = retry >= MAX_RETRIES || SocketTransformer.isClosed(e) || e instanceof RuntimeException || e instanceof Error;
        Level level = retryDone ? Level.WARNING : Level.TRACE;
        LOGGER.log(level, () -> String.format("Send message failed: clientMachine=%s, messageClass=%s, retry=%d, retryDone=%b",
                                              clientMachine.getMachineId(), wrapper.getMessage().getClass().getSimpleName(), retry, retryDone),
                   e);
        if (!retryDone) {
            int nextRetry = retry + 1;
            long delayMillis = computeExponentialBackoff(1000, nextRetry, MAX_RETRIES);
            retryExecutor.schedule(() -> send(wrapper, clientMachine, nextRetry), delayMillis, TimeUnit.MILLISECONDS);
        } else {
            onSendMessageFailed(wrapper, e);
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
     * Override this function to set socket options.
     * For example, the unit tests set SO_REUSEADDR to true.
     */
    protected void onBeforeSocketBound(NetworkChannel channel) throws IOException {
    }

    /**
     * Return null if publisher can be created, and a non-null reason if it cannot.
     * Derived classes may override this function to check if createPublisher passes in a valid secret key,
     * or that the publisher name follows particular naming standards.
     */
    protected @Nullable String canCreatePublisher(CreatePublisher createPublisher) {
        return null;
    }

    /**
     * Return null if client can subscribe, and a non-null reason if it cannot.
     * Derived classes may override this function to check if addOrRemoveSubscriber passes in a valid secret key.
     */
    protected @Nullable String canSubscribe(AddOrRemoveSubscriber addOrRemoveSubscriber) {
        return null;
    }

    /**
     * Override this function to do something before sending a message.
     * For example, the unit tests override this to record the number of messages sent.
     */
    protected void onMessageSent(MessageWrapper wrapper) {
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
    
    /**
     * Override this function to do something when sending a message failed.
     * For example, the unit tests override this to record the failures.
     */
    protected void onSendMessageFailed(MessageWrapper message, Throwable e) {
    }
    
    protected final Stream<ClientMachine> getRemoteClientsStream() {
        return clientMachines.stream();
    }

    private enum ErrorMessageEnum {
        /**
         * A client sent an Identification, but the server already has a machine with this channel.
         */
        CHANNEL_ALREADY_REGISTERED("Channel already registered: channel=%s"),

        /**
         * A client sent an Identification, but the server already has a machine with this client id.
         */
        DUPLICATE_CLIENT_MACHINE_ID("Duplicate channel: clientMachine=%s, otherClientChannel=%s"),
        
        /**
         * Server already saw this message and gave it a serverIndex.
         * Yet client sent this message back to the server.
         */
        MESSAGE_ALREADY_PROCESSED("Message already processed by server: clientIndex=%s"),
        
        /**
         * Client is downloading messages for a topic which does not exist or for which it is not subscribed.
         */
        CLIENT_NOT_SUBSCRIBED_TO_TOPIC("Client is not subscribed to topic: clientMachine=%s, topic=%s"),
        
        /**
         * Cannot create publisher. Possible cause is that secret is not correct.
         */
        CANNOT_CREATE_PUBLISHER("Cannot create publisher: topic=%s, reason='%s'"),
        
        /**
         * Cannot subscribe or unsubscribe. Possible cause is that secret is not correct.
         */
        CANNOT_SUBSCRIBE("Cannot subscribe: topic=%s, subscriberName=%s, reason='%s'");
        
        
        
        private final String formatString;

        ErrorMessageEnum(String formatString) {
            this.formatString = formatString;
        }

        String format(Object... args) {
            return String.format(formatString, args);
        }
    }
}
