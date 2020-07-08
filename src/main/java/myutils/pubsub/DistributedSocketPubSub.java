package myutils.pubsub;

import static myutils.pubsub.PubSubUtils.addShutdownHook;
import static myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static myutils.pubsub.PubSubUtils.closeQuietly;
import static myutils.pubsub.PubSubUtils.extractIndex;
import static myutils.pubsub.PubSubUtils.extractSourceMachine;
import static myutils.pubsub.PubSubUtils.getLocalAddress;
import static myutils.pubsub.PubSubUtils.getRemoteAddress;
import static myutils.pubsub.PubSubUtils.isClosed;
import static myutils.util.concurrent.MoreExecutors.createThreadFactory;

import java.io.IOException;
import java.lang.System.Logger.Level;
import java.lang.ref.Cleaner.Cleanable;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import myutils.pubsub.MessageClasses.CreatePublisher;
import myutils.pubsub.MessageClasses.DownloadPublishedMessages;
import myutils.pubsub.MessageClasses.Identification;
import myutils.pubsub.MessageClasses.MessageBase;
import myutils.pubsub.MessageClasses.PublishMessage;
import myutils.pubsub.MessageClasses.RequestIdentification;
import myutils.pubsub.PubSubUtils.CallStackCapturing;

/**
 * Client class that acts as an in-memory publish/subscribe system, as well as talks to a server to send and receive publish commands.
 * 
 * <p>When a DistributedPubSub is started it connects to the DistributedMessageServer, and identifies itself to the server.
 * The identification includes the client's name, and must be unique across all machines in the system.
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
 * <p>About messages sent between client and server:
 * The first two bytes are the length of the message.
 * The next N bytes is the message, when serialized and converted to a byte stream.
 */
public class DistributedSocketPubSub extends PubSub {
    private static final System.Logger LOGGER = System.getLogger(DistributedSocketPubSub.class.getName());
    private static final ThreadLocal<Boolean> isRemoteThread = new ThreadLocal<>();
    private static final int MAX_RETRIES = 3;

    private String messageServerHost;
    private int messageServerPort;
    private final String machineId;
    private final SocketChannel channel;
    private final ExecutorService channelExecutor = Executors.newFixedThreadPool(2, createThreadFactory("DistributedPubSub"));
    private final ScheduledExecutorService retryExecutor = Executors.newScheduledThreadPool(1, createThreadFactory("DistributedPubSub.Retry"));
    private MessageWriter messageWriter;
    private final AtomicLong localMaxMessage = new AtomicLong();
    private final Cleanable cleanable;

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
        this.channel = createNewSocket(localServer, localPort);
        this.messageWriter = createMessageWriter(this.machineId, messageServerHost, messageServerPort);
        this.cleanable = addShutdownHook(this, new Cleanup(channel, channelExecutor, retryExecutor), DistributedSocketPubSub.class);
    }
    
    private static SocketChannel createNewSocket(String localServer, int localPort) throws IOException {
        var channel = SocketChannel.open();        
        channel.bind(new InetSocketAddress(localServer, localPort));
        return channel;
    }

    private MessageWriter createMessageWriter(@Nullable String machineId,
                                              String messageServerHost,
                                              int messageServerPort) throws IOException {
        MessageWriter messageWriter = new MessageWriter(machineId, channel, localMaxMessage, retryExecutor);
        return messageWriter;
    }

    /**
     * Start the message client.
     * 
     * @throws IOException if there was an error connecting to the messageServerHost:messageServerPost or any other IOException
     */
    public void start() throws IOException {
        channel.connect(new InetSocketAddress(messageServerHost, messageServerPort));
        LOGGER.log(Level.INFO,
                   String.format("Started DistributedPubSub machine=%s with local address %s connected to %s:%d remoteMachine=%s",
                                 messageWriter.getMachineId(),
                                 getLocalAddress(channel),
                                 messageServerHost,
                                 messageServerPort,
                                 getRemoteAddress(channel)));
        channelExecutor.submit(messageWriter);
        channelExecutor.submit(new MessageReader());
        messageWriter.sendIdentification();
    }
    
    /**
     * Download as many messages starting with startIndex from the server.
     * The server does not retain messages forever, so it may not find the oldest messages.
     * 
     * @param startIndex the start index
     * @see DistributedMessageServer#DistributedMessageServer(String, int, java.util.Map) for the number of messages of each MessagePriority to remember
     * @see MessagePriority
     */
    public void download(long startIndex) {
        messageWriter.download(startIndex);
    }

    /**
     * Thread that writes messages to the message server.
     */
    private static class MessageWriter implements Runnable {
        private final String machineId;
        private final SocketChannel channel;
        private final AtomicLong localMaxMessage;
        private final BlockingQueue<MessageBase> queue = new LinkedBlockingQueue<>();
        private final ScheduledExecutorService retryExecutor;

        private MessageWriter(String machineId, SocketChannel chanel, AtomicLong localMaxMessage, ScheduledExecutorService retryExecutor) {
            this.machineId = machineId;
            this.channel = chanel;
            this.localMaxMessage = localMaxMessage;
            this.retryExecutor = retryExecutor;
        }

        String getMachineId() {
            return machineId;
        }

        @Override
        public void run() {
            try {
                while (channel.isConnected()) {
                    MessageBase message = queue.take();
                    LOGGER.log(Level.TRACE,
                               String.format("Sending message to server: machine=%s, messageClass=%s, index=%d",
                                             machineId,
                                             message.getClass().getSimpleName(),
                                             extractIndex(message)));
                    send(message, 0);
                }
            } catch (InterruptedException ignored) {
                LOGGER.log(Level.INFO, "MessageWriter interrupted: machine={0}", machineId);
            } catch (RuntimeException | Error e) {
                LOGGER.log(Level.ERROR, "Unexpected exception: machine=" + machineId, e);
            }
        }

        private void send(MessageBase message, int retry) {
            try {
                SocketTransformer.writeMessageToSocket(message, Short.MAX_VALUE, channel);
            } catch (IOException e) {
                boolean retryDone = retry >= MAX_RETRIES || isClosed(e);
                Level level = retryDone ? Level.WARNING : Level.DEBUG;
                LOGGER.log(level,
                    () -> String.format("Send message failed: machine=%s, retry=%d, retryDone=%b, exception=%s",
                                        machineId, retry, retryDone, e.toString()));
                if (!retryDone) {
                    int delay = 1 << retry;
                    retryExecutor.schedule(() -> send(message, retry + 1), delay, TimeUnit.SECONDS);
                }
            }
        }

        private void sendIdentification() {
            internalPutMessage(new Identification(machineId));
        }

        private void createPublisher(@Nonnull String topic, @Nonnull Class<?> publisherClass) {
            if (isRemoteThread.get() == Boolean.TRUE) {
                return;
            }
            internalPutMessage(new CreatePublisher(localMaxMessage.incrementAndGet(), topic, publisherClass));
        }

        private void publishMessage(@Nonnull String topic, @Nonnull CloneableObject<?> message, MessagePriority priority) {
            if (isRemoteThread.get() == Boolean.TRUE) {
                return;
            }
            internalPutMessage(new PublishMessage(localMaxMessage.incrementAndGet(), topic, message, priority));
        }

        public void download(long startIndex) {
            internalPutMessage(new DownloadPublishedMessages(startIndex));
        }

        private void internalPutMessage(MessageBase message) {
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
        private MessageReader() {
        }

        @Override
        public void run() {
            isRemoteThread.set(Boolean.TRUE);
            while (channel.isConnected()) {
                try {
                    MessageBase message = SocketTransformer.readMessageFromSocket(channel);
                    LOGGER.log(Level.TRACE,
                               String.format("Received message from server: machine=%s, messageClass=%s, index=%d, sourceMachine=%s",
                                             DistributedSocketPubSub.this.machineId,
                                             message.getClass().getSimpleName(),
                                             extractIndex(message),
                                             extractSourceMachine(message)));
                    if (message instanceof RequestIdentification) {
                        messageWriter.sendIdentification();
                    } else if (message instanceof CreatePublisher) {
                        CreatePublisher createPublisher = (CreatePublisher) message;
                        DistributedSocketPubSub.this.localMaxMessage.set(createPublisher.getIndex());
                        DistributedSocketPubSub.this.createPublisher(createPublisher.getTopic(),
                                                               createPublisher.getPublisherClass());
                    } else if (message instanceof PublishMessage) {
                        PublishMessage publishMessage = (PublishMessage) message;
                        DistributedSocketPubSub.this.localMaxMessage.set(publishMessage.getIndex());
                        Optional<Publisher> publisher = DistributedSocketPubSub.this.getPublisher(publishMessage.getTopic());
                        if (publisher.isPresent()) {
                            publisher.get().publish(publishMessage.getMessage());
                        }
                    } else {
                        LOGGER.log(Level.WARNING, "Unrecognized object type received: machine={0}, messageClass={2}",
                                   DistributedSocketPubSub.this.machineId, message.getClass().getSimpleName());
                    }
                } catch (IOException e) {
                    if (isClosed(e)) {
                        LOGGER.log(Level.INFO, "Socket closed, ending reader: {0}", e.toString());
                    }
                    LOGGER.log(Level.WARNING, "Socket exception: machine={0}, exception={1}",
                               DistributedSocketPubSub.this.machineId, e.toString());
                } catch (RuntimeException | Error e) {
                    LOGGER.log(Level.ERROR, "Unexpected exception: machine=" + DistributedSocketPubSub.this.machineId, e);
                }
            }
        }
    }

    /**
     * Publisher that forwards publish commands to the message server.
     */
    public final class DistributedPublisher extends Publisher {
        private DistributedPublisher(@Nonnull String topic, @Nonnull Class<?> publisherClass) {
            super(topic, publisherClass);
        }

        @Override
        public <T extends CloneableObject<?>> void publish(@Nonnull T message, MessagePriority priority) {
            super.publish(message, priority);
            DistributedSocketPubSub.this.messageWriter.publishMessage(getTopic(), message, priority);
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
        var publisher = new DistributedPublisher(topic, publisherClass);
        messageWriter.createPublisher(topic, publisherClass);
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
     * Shutdown this object.
     */
    @Override
    public void shutdown() {
        super.shutdown();
        cleanable.clean();
    }

    /**
     * Cleanup this class. Close the socket channel and shutdown the executor.
     */
    private static class Cleanup extends CallStackCapturing implements Runnable {
        private final SocketChannel channel;
        private final ExecutorService channelExecutor;
        private final ExecutorService retryExecutor;

        private Cleanup(SocketChannel channel, ExecutorService channelExecutor, ExecutorService retryExecutor) {
            this.channel = channel;
            this.channelExecutor = channelExecutor;
            this.retryExecutor = retryExecutor;
        }

        @Override
        public void run() {
            LOGGER.log(Level.DEBUG, "Cleaning up " + DistributedSubscriber.class.getSimpleName() + getCallStack());
            closeExecutorQuietly(channelExecutor);
            closeExecutorQuietly(retryExecutor);
            closeQuietly(channel);
        }
    }
}
