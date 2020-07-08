package myutils.pubsub;

import static myutils.pubsub.PubSubUtils.addShutdownHook;
import static myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static myutils.util.concurrent.MoreExecutors.createThreadFactory;

import java.lang.System.Logger.Level;
import java.lang.ref.Cleaner.Cleanable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import myutils.pubsub.PubSubUtils.CallStackCapturing;
import myutils.util.MultimapUtils;


/**
 * A class to implement publish-subscribe in memory in one JVM, with the ability to add distributed features.
 * 
 * <p>There is a publish function to create a publisher for a topic, along with the type of class the publisher publishes.
 * There is a subscribe function to create a subscriber for a topic, along with the type of the class that the subscriber handles and a callback function.
 * The subscriber class must be the same as or inherit from the publisher class.
 * A publisher will only invoke subscribers that handle the type of the event.
 * Subscribers will only see messages published since the time of subscription.
 * 
 * <p>Prior to invoking the subscription handler, this class makes a copy of the message.
 * The publisher class must implement the CloneableObject interface, which has a public clone function that does not throw CloneNotSupportedException.
 *
 * <p>Each PubSub instance has a general exception handler to handle exceptions invoking any subscription handler.
 * One may log the error with callstack (the default behavior), or
 * or provide custom behavior such as logging a shorter message or resubmitting the message to the subscription handler.
 * The handler is responsible for taking care of the backoff strategy.
 */
public abstract class PubSub implements Shutdowneable {
    private static final System.Logger LOGGER = System.getLogger(PubSub.class.getName());

    /**
     * The default way in which messages to process in this PubSub are stored, which is just a standard queue.
     */
    public static Supplier<Queue<Subscriber>> defaultQueueCreator() {
        return () -> new ArrayDeque<>();
    }

    /**
     * A general exception handler to handle all exceptions arising in this PubSub.
     */
    public interface SubscriptionMessageExceptionHandler {
        /**
         * Handle exception thrown when processing a subscription message.
         *
         * @param e the exception, which is either a RuntimeException or Error
         */
        void handleException(Subscriber subscriber, CloneableObject<?> message, Throwable e);
    }

    /**
     * The default exception handler which logs the exception and callstack at WARNING level.
     */
    public static SubscriptionMessageExceptionHandler defaultSubscriptionMessageExceptionHandler() {
        return defaultSubscriptionMessageExceptionHandler;
    }

    private static final SubscriptionMessageExceptionHandler defaultSubscriptionMessageExceptionHandler = new DefaultSubscriptionMessageExceptionHandler();

    private static class DefaultSubscriptionMessageExceptionHandler implements SubscriptionMessageExceptionHandler {
        private static final System.Logger LOGGER = System.getLogger(DefaultSubscriptionMessageExceptionHandler.class.getName());

        @Override
        public void handleException(Subscriber subcriber, CloneableObject<?> message, Throwable e) {
            LOGGER.log(System.Logger.Level.WARNING, "Exception invoking subscriber", e);
        }
    }

    private final Map<String /*topic*/, Publisher> topicMap = new HashMap<>();
    private final Map<String /*topic*/, Collection<DeferredSubscriber>> deferredSubscribersMap = new HashMap<>();
    private final ThreadPoolExecutor executorService;
    private final Queue<Subscriber> masterList;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler;
    private final Cleanable cleanable;

    /**
     * Create a PubSub system.
     * 
     * @param numInMemoryHandlers the number of threads handling messages that are published by all publishers.
     * @param queueCreator the queue to store all message across all subscribers.
     * @param subscriptionMessageExceptionHandler the general subscription handler for exceptions arising from all subscribers.
     */
    public PubSub(int numInMemoryHandlers,
                  Supplier<Queue<Subscriber>> queueCreator,
                  SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler) {
        this.executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(numInMemoryHandlers, createThreadFactory("PubSubListener"));
        this.masterList = queueCreator.get();
        this.subscriptionMessageExceptionHandler = subscriptionMessageExceptionHandler;
        this.cleanable = addShutdownHook(this, new Cleanup(executorService, lock, notEmpty), PubSub.class);
        startThreads();
    }
    
    /**
     * Class representing the publisher.
     * 
     * <p>In implementation it has a topic, publisher class, list of subscribers, and pointer to the lock in the outer PubSub class. 
     */
    public abstract class Publisher {
        private final @Nonnull String topic;
        private final @Nonnull Class<?> publisherClass;
        private final @Nonnull Collection<Subscriber> subscribers = new ArrayList<>();

        protected Publisher(@Nonnull String topic, @Nonnull Class<?> publisherClass) {
            this.topic = topic;
            this.publisherClass = publisherClass;
        }

        @Nonnull
        public String getTopic() {
            return topic;
        }
        
        public final <T extends CloneableObject<?>> void publish(@Nonnull T message) {
            publish(message, MessagePriority.MEDIUM);
        }
        
        public <T extends CloneableObject<?>> void publish(@Nonnull T message, MessagePriority priority) {
            PubSub.this.lock.lock();
            try {
                for (var subscriber : subscribers) {
                    if (subscriber.subscriberClass.isInstance(message)) {
                        CloneableObject<?> copy = (CloneableObject<?>) message.clone();
                        subscriber.addMessage(copy);
                    }
                }
            } finally {
                PubSub.this.lock.unlock();
            }
        }
    }

    /**
     * Class representing a subscriber.
     * 
     * <p>In implementation it has a topic, name, subscriber class (which must be the same as or inherit from the publisher class), callback function,
     * list of messages to process, and pointer to the lock and condition and master list in the outer PubSub class. 
     */
    public abstract class Subscriber {
        private final @Nonnull String topic;
        private final @Nonnull String subsriberName;
        private final @Nonnull Class<? extends CloneableObject<?>> subscriberClass; // same as or inherits from publisherClass
        private final @Nonnull Consumer<CloneableObject<?>> callback;
        private final @Nonnull Queue<CloneableObject<?>> messages = new ArrayDeque<>();

        protected Subscriber(@Nonnull String topic,
                             @Nonnull String subscriberName,
                             @Nonnull Class<? extends CloneableObject<?>> subscriberClass,
                             @Nonnull Consumer<CloneableObject<?>> callback) {
            this.topic = topic;
            this.subsriberName = subscriberName;
            this.subscriberClass = subscriberClass;
            this.callback = callback;
        }

        @Nonnull
        public String getTopic() {
            return topic;
        }

        @Nonnull
        public String getSubscriberName() {
            return subsriberName;
        }

        public void addMessage(CloneableObject<?> message) {
            if (!(subscriberClass.isInstance(message))) {
                throw new IllegalArgumentException("message must be or inherit from " + subscriberClass.getName());
            }
            PubSub.this.lock.lock();
            try {
                messages.offer(message);
                PubSub.this.masterList.add(this);
                PubSub.this.notEmpty.signal();
            } finally {
                PubSub.this.lock.unlock();
            }
        }
    }

    /**
     * Class used when subscriber created before publisher.
     * When publisher created, this class PubSub makes calls to addSubscriber.
     */
    private static class DeferredSubscriber {
        private final @Nonnull String subsriberName;
        private final @Nonnull Class<? extends CloneableObject<?>> subscriberClass;
        private final @Nonnull Consumer<CloneableObject<?>> callback;

        private DeferredSubscriber(@Nonnull String subscriberName, @Nonnull Class<? extends CloneableObject<?>> subscriberClass, @Nonnull Consumer<CloneableObject<?>> callback) {
            this.subsriberName = subscriberName;
            this.subscriberClass = subscriberClass;
            this.callback = callback;
        }
    }

    /**
     * Create a publisher.
     * 
     * @param <T> the type of object this publisher publishes. Need not implement CloneableObject as the derived classes may implement it.
     * @param topic the topic
     * @param publisherClass same as T.class
     * @return a new publisher
     * @throws IllegalArgumentException if the publisher already exists.
     * @throws IllegalArgumentException if subscriber class of deferred subscribers is not the same or inherits from the publisher class
     */
    public final synchronized <T> Publisher createPublisher(String topic, Class<T> publisherClass) {
        var publisher = topicMap.get(topic);
        if (publisher != null) {
            throw new IllegalArgumentException("publisher already exists: " + topic);
        }
        publisher = newPublisher(topic, publisherClass);
        topicMap.put(topic, publisher);
        addDeferredSubscribers(topic);
        return publisher;
    }
    
    protected abstract <T> Publisher newPublisher(String topic, Class<T> publisherClass);

    /**
     * Get an existing publisher.
     */
    public final synchronized Optional<Publisher> getPublisher(String topic) {
        var publisher = topicMap.get(topic);
        return Optional.ofNullable(publisher);
    }

    private void addDeferredSubscribers(String topic) {
        var deferredSubscribers = deferredSubscribersMap.remove(topic);
        if (deferredSubscribers != null) {
            for (var deferredSubscriber : deferredSubscribers) {
                doSubscribe(topic, deferredSubscriber.subsriberName, deferredSubscriber.subscriberClass, deferredSubscriber.callback);
            }
        }
    }

    /**
     * Create a subscriber.
     * 
     * @param <T> the type of object this subscriber receives. Must be the same as or inherit from the publisher class, and must implement CloneableObject.
     * @param topic the topic
     * @param subscriberName the name of this subscriber, useful for debugging
     * @param subscriberClass same as T.class
     * @param callback the callback function
     * @return a new subscriber
     * @throws IllegalArgumentException if subscriber class is not the same or inherits from the publisher class
-     */
    @SuppressWarnings("unchecked")
    public final synchronized <T extends CloneableObject<?>> Subscriber subscribe(@Nonnull String topic,
                                                                                  @Nonnull String subscriberName,
                                                                                  @Nonnull Class<T> subscriberClass,
                                                                                  @Nonnull Consumer<T> callback) {
        Consumer<CloneableObject<?>> callbackCasted = (Consumer<CloneableObject<?>>) callback;
        return doSubscribe(topic, subscriberName, subscriberClass, callbackCasted);
    }

    private Subscriber doSubscribe(String topic, String subscriberName, Class<? extends CloneableObject<?>> subscriberClass, Consumer<CloneableObject<?>> callback) {
        var subscriber = newSubscriber(topic, subscriberName, subscriberClass, callback);
        var publisher = topicMap.get(topic);
        if (publisher != null) {
            checkClassType(publisher.publisherClass, subscriberClass);
            publisher.subscribers.add(subscriber);
        } else {
            MultimapUtils<String, DeferredSubscriber> multimap = new MultimapUtils<>(deferredSubscribersMap, ArrayList::new);
            multimap.put(topic, new DeferredSubscriber(subscriberName, subscriberClass, callback));
        }
        return subscriber;
    }
    
    protected abstract Subscriber newSubscriber(@Nonnull String topic,
                                                @Nonnull String subscriberName,
                                                @Nonnull Class<? extends CloneableObject<?>> subscriberClass,
                                                @Nonnull Consumer<CloneableObject<?>> callback);

    private static void checkClassType(Class<?> publisherClass, Class<?> subscriberClass) {
        if (!(publisherClass.isAssignableFrom(subscriberClass))) {
            throw new IllegalArgumentException("subscriber class must be the same as or inherit from the publisher class:"
                    + " publisherClass=" + publisherClass.getName()
                    + " subscriberClass=" + subscriberClass.getName());
        }
    }

    /**
     * Unsubscribe a subscriber.
     * Messages in the subscriber queue will be purged.
     * 
     * <p>Running time O(N) where N is the total number of messages in all subscribers.
     */
    public synchronized void unsubscribe(Subscriber subscriber) {
        var publisher = topicMap.get(subscriber.topic);
        if (publisher == null) {
            return;
        }
        masterList.removeIf(iterSubscriber -> iterSubscriber == subscriber);
        publisher.subscribers.remove(subscriber);
    }

    private void startThreads() {
        for (int i = 0; i < executorService.getCorePoolSize(); i++) {
            executorService.submit(new Listener(masterList, lock, notEmpty, subscriptionMessageExceptionHandler));
        }
    }

    /**
     * Thread that listens for a new message in any subscriber queue and process it by cloning it and passing it to all of the relevant subscribers. 
     */
    private static class Listener implements Runnable {
        private final Queue<Subscriber> masterList;
        private final Lock lock;
        private final Condition notEmpty;
        private final SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler;

        private Listener(Queue<Subscriber> masterList, Lock lock, Condition notEmpty, SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler) {
            this.masterList = masterList;
            this.lock = lock;
            this.notEmpty = notEmpty;
            this.subscriptionMessageExceptionHandler = subscriptionMessageExceptionHandler;
        }

        @Override
        public void run() {
            while (true) {
                boolean locked = true;
                lock.lock();
                try {
                    while (masterList.isEmpty()) {
                        try {
                            notEmpty.await();
                        } catch (InterruptedException ignored) {
                            return;
                        }
                    }
                    Subscriber subscriber = masterList.poll();
                    CloneableObject<?> message = subscriber.messages.poll();
                    lock.unlock();
                    locked = false;
                    handleMessage(subscriber, message);
                } finally {
                    if (locked) {
                        lock.unlock();
                    }
                }
            }
        }

        private void handleMessage(Subscriber subscriber, CloneableObject<?> message) {
            try {
                subscriber.callback.accept(message);
            } catch (RuntimeException | Error e) {
                subscriptionMessageExceptionHandler.handleException(subscriber, message, e);
            }
        }
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
     * Shutdown the executor and wake up all threads so that they can end gracefully.
     */
    private static class Cleanup extends CallStackCapturing implements Runnable {
        private final ExecutorService executorService;
        private final ReentrantLock lock;
        private final Condition notEmpty;

        private Cleanup(ExecutorService executorService, ReentrantLock lock, Condition notEmpty) {
            this.executorService = executorService;
            this.lock = lock;
            this.notEmpty = notEmpty;
        }

        @Override
        public void run() {
            LOGGER.log(Level.DEBUG, "Cleaning up " + PubSub.class.getSimpleName() + getCallStack());
            closeExecutorQuietly(executorService);
            try {
                boolean locked = lock.tryLock(100, TimeUnit.MILLISECONDS);
                if (locked) {
                    try {
                        notEmpty.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }        
    }
}
