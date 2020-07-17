package myutils.pubsub;

import static myutils.pubsub.PubSubUtils.addShutdownHook;
import static myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static myutils.pubsub.PubSubUtils.closeQuietly;
import static myutils.pubsub.PubSubUtils.computeExponentialBackoffMillis;
import static myutils.pubsub.PubSubUtils.extractIndex;
import static myutils.pubsub.PubSubUtils.getLocalAddress;
import static myutils.pubsub.PubSubUtils.getRemoteAddress;
import static myutils.pubsub.PubSubUtils.isClosed;
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
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import myutils.pubsub.MessageClasses.AddSubscriber;
import myutils.pubsub.MessageClasses.ClientGeneratedMessage;
import myutils.pubsub.MessageClasses.CreatePublisher;
import myutils.pubsub.MessageClasses.DownloadPublishedMessages;
import myutils.pubsub.MessageClasses.Identification;
import myutils.pubsub.MessageClasses.InvalidRelayMessage;
import myutils.pubsub.MessageClasses.MessageBase;
import myutils.pubsub.MessageClasses.PublishMessage;
import myutils.pubsub.MessageClasses.RelayMessageBase;
import myutils.pubsub.MessageClasses.RelayTopicMessageBase;
import myutils.pubsub.MessageClasses.RemoveSubscriber;
import myutils.pubsub.MessageClasses.RequestIdentification;
import myutils.pubsub.PubSubUtils.CallStackCapturing;
import myutils.util.MultimapUtils;
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
 * <p>The server caches the last N messages of each MessagePriority.
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
    private final Map<String /*topic*/, Collection<SubscriberEndpoint>> subscriberEndpointMap = new HashMap<>();
    private final AtomicLong maxMessage = new AtomicLong();
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
        private final List<CreatePublisher> publishers = new ArrayList<>();
        private final int[] mostRecentMessagesToKeep;
        private final List<Deque<PublishMessage>> messages = List.of(new LinkedList<>(), new LinkedList<>());
        private final Map<ClientMachine, Map<String /*topic*/, Long /*maxIndex*/>> highestIndexMap = new HashMap<>();
        
        MostRecentMessages(Map<MessagePriority, Integer> mostRecentMessagesToKeep) {
            this.mostRecentMessagesToKeep = computeMostRecentMessagesToKeep(mostRecentMessagesToKeep);
        }
        
        private static int[] computeMostRecentMessagesToKeep(Map<MessagePriority, Integer> mostRecentMessagesToKeep) {
            var result = new int[MessagePriority.values().length];
            for (var entry : mostRecentMessagesToKeep.entrySet()) {
                result[entry.getKey().ordinal()] = entry.getValue();
            }
            return result;
        }

        synchronized void save(ClientMachine clientMachine, RelayTopicMessageBase relayMessage) {
            if (relayMessage instanceof CreatePublisher) {
                CreatePublisher createPublisher = (CreatePublisher) relayMessage;
                publishers.add(createPublisher);
            } else if (relayMessage instanceof PublishMessage) {
                PublishMessage publishMessage = (PublishMessage) relayMessage;
                int ordinal = publishMessage.getPriority().ordinal();
                Deque<PublishMessage> deque = messages.get(ordinal);
                deque.add(publishMessage);
                if (deque.size() > mostRecentMessagesToKeep[ordinal]) {
                    deque.removeFirst();
                }
            }
            onMessageRelayed(clientMachine, relayMessage);
        }
        
        synchronized void onMessageRelayed(ClientMachine clientMachine, RelayTopicMessageBase relayMessage) {
            setMaxIndexIfLarger(clientMachine, relayMessage.getTopic(), relayMessage.getServerIndex());
        }

        /**
         * Retrieve publishers to send to clientMachine.
         * 
         * <p>Publishers that originated from clientMachine are not sent to it because save/onMessageRelayed would have been called,
         * setting the maxIndex for this clientMachine and topic,
         * and this functions only retrieves messages larger than maxIndex.
         */
        synchronized int retrievePublishers(ClientMachine clientMachine,
                                            @Nonnull String topic,
                                            long upperBoundInclusive,
                                            Consumer<CreatePublisher> consumer) {
            int count = 0;
            long lowerBoundInclusive = getMaxIndex(clientMachine, topic) + 1;
            CreatePublisher lastMessage = null;
            Iterator<CreatePublisher> iter = publishers.iterator();
            while (iter.hasNext()) {
                CreatePublisher message = iter.next();
                if (message.getServerIndex() > upperBoundInclusive) {
                    break;
                }
                if (message.getServerIndex() < lowerBoundInclusive) {
                    continue;
                }
                consumer.accept(message);
                count++;
                lastMessage = message;
            }
            if (lastMessage != null) {
                setMaxIndexIfLarger(clientMachine, topic, lastMessage.getServerIndex());
            }
            return count;
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
            Iterator<PublishMessage> iter = new ZipMinIterator<PublishMessage>(messages, (lhs, rhs) -> Long.compare(lhs.getServerIndex(), rhs.getServerIndex()));
            while (iter.hasNext()) {
                PublishMessage message = iter.next();
                if (message.getServerIndex() > upperBoundInclusive) {
                    break;
                }
                if (message.getServerIndex() < lowerBoundInclusive) {
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
                setMaxIndexIfLarger(clientMachine, topic, lastMessage.getServerIndex());
            }
            return count;
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
    public DistributedMessageServer(@Nonnull String host, int port, Map<MessagePriority, Integer> mostRecentMessagesToKeep) throws IOException {
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
     * 
     * @throws IOException if the host:port is already in use or another IOException occurs
     */
    public void start() throws IOException {
        openServerSocket();
        acceptExecutor.submit(new AcceptThread());
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
                    Identification identification = (Identification) message;
                    DistributedMessageServer.this.addIfNotPresentAndSendPublishers(identification, channel);
                } else {
                    ClientMachine clientMachine = findClientMachineByChannel(channel);
                    if (clientMachine == null) {
                        sendRequestIdentification(channel, message);
                    } else if (message instanceof RelayMessageBase && ((RelayMessageBase) message).getServerIndex() != 0) {
                        RelayMessageBase relayMessage = (RelayMessageBase) message;
                        DistributedMessageServer.this.sendInvalidRelayMessage(clientMachine,
                                                                              relayMessage,
                                                                              ErrorMessageEnum.MESSAGE_ALREADY_PROCESSED.format(relayMessage.getClientIndex()));
                    } else {
                        Runnable logging = () -> {
                            LOGGER.log(Level.TRACE,
                                       String.format("Received message from client: clientMachine=%s, %s",
                                                     clientMachine.getMachineId(),
                                                     message.toLoggingString()));
                            onValidMessageReceived(message);
                        };
                        if (message instanceof AddSubscriber) {
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
                if (isClosed(exception)) {
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
        LOGGER.log(Level.INFO, "Added client machine: clientMachine={0} clientChannel={1}", identification.getMachineId(), getRemoteAddress(channel));
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
        removeSubscriber(channel);
    }
    
    private synchronized void removeSubscriber(AsynchronousSocketChannel channel) {
        for (var iter = subscriberEndpointMap.entrySet().iterator(); iter.hasNext(); ) {
            var entry = iter.next();
            Collection<SubscriberEndpoint> collection = entry.getValue();
            collection.removeIf(endpoint -> endpoint.getClientMachine().getChannel() == channel);
            if (collection.isEmpty()) {
                iter.remove();
            }
        }
    }
    
    private synchronized void handleAddSubscriber(ClientMachine clientMachine, AddSubscriber subscriberInfo) {
        String topic = subscriberInfo.getTopic();
        String subscriberName = subscriberInfo.getSubscriberName();
        MultimapUtils<String, SubscriberEndpoint> multimap = new MultimapUtils<>(subscriberEndpointMap, ArrayList::new);
        var endpoints = multimap.getOrCreate(topic);
        boolean clientMachineAlreadySubscribedToTopic = endpoints.stream().anyMatch(endpoint -> endpoint.getClientMachine().equals(clientMachine));
        endpoints.add(new SubscriberEndpoint(subscriberName, clientMachine));
        int numPublishersSent;
        if (!clientMachineAlreadySubscribedToTopic) {
            numPublishersSent = mostRecentMessages.retrievePublishers(clientMachine,
                                                                      topic,
                                                                      Long.MAX_VALUE,
                createPublisher -> DistributedMessageServer.this.send(createPublisher, clientMachine, 0));
        } else {
            numPublishersSent = 0;
        }
        LOGGER.log(Level.INFO,
                   "Added subscriber : topic={0} subscriberName={1} clientMachine={2} numPublishersSent={3}",
                  topic, subscriberName, clientMachine.getMachineId(), numPublishersSent);
        if (subscriberInfo.shouldTryDownload()) {
            download(clientMachine, topic, subscriberInfo.getClientTimestamp(), null, Long.MAX_VALUE);
        }
    }
    
    private synchronized void handleRemoveSubscriber(ClientMachine clientMachine, RemoveSubscriber subscriberInfo) {
        String topic = subscriberInfo.getTopic();
        String subscriberName = subscriberInfo.getSubscriberName();
        MultimapUtils<String, SubscriberEndpoint> multimap = new MultimapUtils<>(subscriberEndpointMap, ArrayList::new);
        multimap.removeIf(topic, endpoint -> endpoint.getSubscriberName().equals(subscriberName));
        LOGGER.log(Level.INFO, "Removed subscriber : topic={0} subscriberName={1} clientMachine={2}", topic, subscriberName, clientMachine.getMachineId());
    }

    private void handleRelayMessage(@Nonnull ClientMachine clientMachine, RelayMessageBase relay) {
        relay.setServerTimestampAndSourceMachineIdAndIndex(clientMachine.getMachineId(), maxMessage.incrementAndGet());
        if (relay instanceof RelayTopicMessageBase) {
            handleRelayTopicMessage(clientMachine, (RelayTopicMessageBase) relay);
        }
    }
    
    private void handleRelayTopicMessage(ClientMachine clientMachine, RelayTopicMessageBase relay) {
        mostRecentMessages.save(clientMachine, relay);
        Collection<SubscriberEndpoint> endpoints = subscriberEndpointMap.get(relay.getTopic());
        if (endpoints != null) {
            endpoints.stream()
                     .filter(endpoint -> !endpoint.getClientMachine().equals(clientMachine))
                     .map(SubscriberEndpoint::getClientMachine)
                     .distinct()
                     .forEach(otherMachine -> send(relay, otherMachine, 0));
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
        int numMessages = mostRecentMessages.retrieveMessages(clientMachine,
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
        try {
            SocketTransformer.writeMessageToSocketAsync(message, Short.MAX_VALUE, clientMachine.getChannel())
                             .thenAcceptAsync(unused -> afterMessageSent(clientMachine, message), channelExecutor)
                             .exceptionally(e -> retrySend(message, clientMachine, retry, e));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,
                       String.format("Send message failed: clientMachine=%s, retry=%d, retryDone=%b, exception=%s",
                                     clientMachine.getMachineId(), retry, true, e.toString()));
        }
    }
    
    private void afterMessageSent(ClientMachine clientMachine, MessageBase message) {
        LOGGER.log(Level.TRACE,
                   String.format("Sent message to client: clientMachine=%s, messageClass=%s, messageIndex=%d",
                                 clientMachine.getMachineId(),
                                 message.getClass().getSimpleName(),
                                 extractIndex(message)));
        onMessageSent(message);
        if (message instanceof RelayTopicMessageBase) {
            RelayTopicMessageBase relayMessage = (RelayTopicMessageBase) message;
            mostRecentMessages.onMessageRelayed(clientMachine, relayMessage);
        }
    }
    
    private Void retrySend(MessageBase message, ClientMachine clientMachine, int retry, Throwable e) {
        boolean retryDone = retry >= MAX_RETRIES || isClosed(e);
        Level level = retryDone ? Level.WARNING : Level.TRACE;
        LOGGER.log(level, () -> String.format("Send message failed: clientMachine=%s, retry=%d, retryDone=%b, exception=%s",
                                              clientMachine.getMachineId(), retry, retryDone, e.toString()));
        if (!retryDone) {
            int nextRetry = retry + 1;
            long delayMillis = computeExponentialBackoffMillis(1000, nextRetry, MAX_RETRIES);
            retryExecutor.schedule(() -> send(message, clientMachine, nextRetry), delayMillis, TimeUnit.MILLISECONDS);
        }
        return null;
    }

    private void submitReadFromChannelJob(AsynchronousSocketChannel channel) {
        channelExecutor.submit(new ChannelThread(channel));
    }
    
    /**
     * Shutdown this object.
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

