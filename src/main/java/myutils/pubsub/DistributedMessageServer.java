package myutils.pubsub;

import static myutils.pubsub.PubSubUtils.addShutdownHook;
import static myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static myutils.pubsub.PubSubUtils.closeQuietly;
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
import java.nio.channels.Channel;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
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
import myutils.pubsub.MessageClasses.ActionMessageBase;
import myutils.pubsub.MessageClasses.ClientGeneratedMessage;
import myutils.pubsub.MessageClasses.CreatePublisher;
import myutils.pubsub.MessageClasses.DownloadPublishedMessages;
import myutils.pubsub.MessageClasses.Identification;
import myutils.pubsub.MessageClasses.MessageBase;
import myutils.pubsub.MessageClasses.PublishMessage;
import myutils.pubsub.MessageClasses.RequestIdentification;
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
 * Clients must avoid infinite recursion: if server relays a message to client2, that client2 must not send that message back to the server
 * as that would send the message back to client1.
 * 
 * <p>The server caches the last N messages of each MessagePriority.
 * Clients can download all publish message commands from a particular index, and all messages in the cache from this time up to the time of download
 * will be sent to that client.
 * 
 * <p>About messages sent between client and server:
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
    private final MostRecentMessages mostRecentMessages;
    private final Cleanable cleanable;
    
    
    /**
     * Class representing a remote machine.
     * Key fields are machineId (a string) and channel (an AsynchronousSocketChannel).
     */
    private static final class ClientMachine {
        private final String machineId;
        private final AsynchronousSocketChannel channel;
        private final String remoteAddress;
        
        private ClientMachine(String machineId, AsynchronousSocketChannel channel) {
            this.machineId = machineId;
            this.channel = channel;
            this.remoteAddress = getRemoteAddress(channel);
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

    private static class MostRecentMessages {
        private final List<CreatePublisher> publishers = new ArrayList<>();
        private final int[] mostRecentMessagesToKeep;
        private final List<Deque<PublishMessage>> messages = List.of(new LinkedList<>(), new LinkedList<>()); 
        
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

        synchronized void save(ActionMessageBase action) {
            if (action instanceof CreatePublisher) {
                CreatePublisher createPublisher = (CreatePublisher) action;
                publishers.add(createPublisher);
            } else if (action instanceof PublishMessage) {
                PublishMessage publishMessage = (PublishMessage) action;
                int ordinal = publishMessage.getPriority().ordinal();
                Deque<PublishMessage> deque = messages.get(ordinal);
                deque.add(publishMessage);
                if (deque.size() > mostRecentMessagesToKeep[ordinal]) {
                    deque.removeFirst();
                }
            }
        }
        
        synchronized int retrievePublishers(long upperBoundInclusive, Consumer<CreatePublisher> consumer) {
            int count = 0;
            Iterator<CreatePublisher> iter = publishers.iterator();
            while (iter.hasNext()) {
                var message = iter.next();
                if (message.getIndex() > upperBoundInclusive) {
                    break;
                }
                consumer.accept(message);
                count++;
            }
            return count;
        }
        
        synchronized int retrieveMessages(long lowerBoundInclusive, long upperBoundInclusive, Consumer<PublishMessage> consumer) {
            int count = 0;
            Iterator<PublishMessage> iter = new ZipMinIterator<PublishMessage>(messages, (lhs, rhs) -> Long.compare(lhs.getIndex(), rhs.getIndex()));
            while (iter.hasNext()) {
                var message = iter.next();
                if (message.getIndex() < lowerBoundInclusive) {
                    continue;
                }
                if (message.getIndex() > upperBoundInclusive) {
                    break;
                }
                consumer.accept(message);
                count++;
            }
            return count;
        }
    }
    
    /**r
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
        this.cleanable = addShutdownHook(this, new Cleanup(asyncServerSocketChannel, acceptExecutor, channelExecutor, retryExecutor, clientMachines), DistributedMessageServer.class);
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
        asyncServerSocketChannel.bind(new InetSocketAddress(host, port));
        LOGGER.log(Level.INFO, String.format("Started DistributedMessageServer: localHostAndPort=%s:%d, localMachine=%s", host, port, getLocalAddress(asyncServerSocketChannel)));
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
            LOGGER.log(Level.TRACE,
                       String.format("Received message from client: clientMachine=%s, messageClass=%s, messageIndex=%d",
                                     getRemoteAddress(channel),
                                     message.getClass().getSimpleName(),
                                     extractIndex(message)));
            if (message instanceof ClientGeneratedMessage) {
                ClientGeneratedMessage clientGeneratedMessage = (ClientGeneratedMessage) message;
                clientGeneratedMessage.setServerTimestampToNow();
                if (message instanceof Identification) {
                    Identification identification = (Identification) message;
                    DistributedMessageServer.this.addIfNotPresentAndSendPublishers(identification, channel);
                } else if (message instanceof ActionMessageBase) {
                    DistributedMessageServer.this.handleAction(channel, (ActionMessageBase) message);
                } else if (message instanceof DownloadPublishedMessages) {
                    DistributedMessageServer.this.handleDowload(channel, (DownloadPublishedMessages) message);
                } else {
                    LOGGER.log(Level.DEBUG, "Unhandled action {0}", message.getClass().getSimpleName());
                }
            } else {
                LOGGER.log(Level.DEBUG, "Unhandled action {0}", message.getClass().getSimpleName());
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
            LOGGER.log(Level.DEBUG, "Channel closed: clientMachine={0}", clientMachine != null ? clientMachine.getMachineId() : "<unknown>");
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
        
        long upperBoundInclusive = maxMessage.get();
        int numPublishers = mostRecentMessages.retrievePublishers(upperBoundInclusive, createPublisher -> DistributedMessageServer.this.send(createPublisher, clientMachine, 0));
        if (numPublishers > 0) {
            LOGGER.log(Level.INFO, "Sent publishers to newly added client: clientMachine={0} numPublishers={1}", identification.getMachineId(), numPublishers);
        }
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
    }
    
    private void handleAction(AsynchronousSocketChannel channel, ActionMessageBase action) {
        ClientMachine clientMachine = findClientMachineByChannel(channel);
        if (clientMachine != null) {
            action.setSourceMachineIdAndResetIndex(clientMachine.getMachineId(), maxMessage.incrementAndGet());
            mostRecentMessages.save(action);
            sendToEveryoneExcept(action, channel);
        } else {
            sendRequestIdentification(channel, action.getIndex());
        }
    }
    
    private void sendRequestIdentification(AsynchronousSocketChannel channel, long failedIndex) {
        ClientMachine clientMachine = new ClientMachine("<unknown>", channel);
        RequestIdentification request = new RequestIdentification(failedIndex);
        send(request, clientMachine, 0);
    }

    private void handleDowload(AsynchronousSocketChannel channel, DownloadPublishedMessages download) {
        long lowerBoundInclusive = download.getStartIndex();
        long upperBoundInclusive = maxMessage.get();
        ClientMachine clientMachine = findClientMachineByChannel(channel);
        int numMessages = mostRecentMessages.retrieveMessages(lowerBoundInclusive, upperBoundInclusive, publishMessage -> send(publishMessage, clientMachine, 0));
        LOGGER.log(Level.INFO, String.format("Download messages to client: clientMachine=%s numMessages=%d", clientMachine.getMachineId(), numMessages));
    }

    private void sendToEveryoneExcept(MessageBase message, AsynchronousSocketChannel excludeChannel) {
        for (var remoteMachine : clientMachines) {
            if (remoteMachine.getChannel() == excludeChannel) {
                continue;
            }
            send(message, remoteMachine, 0);
        }
    }

    private void send(MessageBase message, ClientMachine clientMachine, int retry) {
        try {
            SocketTransformer.writeMessageToSocketAsync(message, Short.MAX_VALUE, clientMachine.getChannel())
                             .thenAcceptAsync(unused -> LOGGER.log(Level.TRACE,
                                                                   String.format("Sent message: clientMachine=%s, messageClass=%s, messageIndex=%d",
                                                                                 clientMachine.getMachineId(),
                                                                                 message.getClass().getSimpleName(),
                                                                                 extractIndex(message))),
                                              channelExecutor)
                             .exceptionally(e -> retrySend(message, clientMachine, retry, e));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,
                       String.format("Send message failed: clientMachine=%s, retry=%d, retryDone=%b, exception=%s",
                                     clientMachine.getMachineId(), retry, true, e.toString()));
        }
    }
    
    private Void retrySend(MessageBase message, ClientMachine clientMachine, int retry, Throwable e) {
        boolean retryDone = retry >= MAX_RETRIES || isClosed(e);
        Level level = retryDone ? Level.WARNING : Level.TRACE;
        LOGGER.log(level, () -> String.format("Send message failed: clientMachine=%s, retry=%d, retryDone=%b, exception=%s",
                                              clientMachine.getMachineId(), retry, retryDone, e.toString()));
        if (!retryDone) {
            int delay = 1 << retry;
            retryExecutor.schedule(() -> send(message, clientMachine, retry + 1), delay, TimeUnit.SECONDS);
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
     * Cleanup this class.
     * Close all connections, close the socket server channel, and shutdown all executors.
     */
    private static class Cleanup extends CallStackCapturing implements Runnable {
        private final Channel channel;
        private final ExecutorService acceptExecutor;
        private final ExecutorService channelExecutor;
        private final ExecutorService retryExecutor;
        private final List<ClientMachine> clientMachines;

        private Cleanup(Channel channel, ExecutorService acceptExecutor, ExecutorService channelExecutor, ExecutorService retryExecutor, List<ClientMachine> clientMachines) {
            this.channel = channel;
            this.acceptExecutor = acceptExecutor;
            this.channelExecutor = channelExecutor;
            this.retryExecutor = retryExecutor;
            this.clientMachines = clientMachines;
        }

        @Override
        public void run() {
            LOGGER.log(Level.DEBUG, "Shutting down " + DistributedMessageServer.class.getSimpleName() + getCallStack());
            closeExecutorQuietly(acceptExecutor);
            closeExecutorQuietly(channelExecutor);
            closeExecutorQuietly(retryExecutor);
            clientMachines.forEach(clientMachine -> closeQuietly(clientMachine.channel));
            closeQuietly(channel);
        }
    }
}

