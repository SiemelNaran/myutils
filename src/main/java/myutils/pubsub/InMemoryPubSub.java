package myutils.pubsub;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import myutils.util.MultimapUtils;


/**
 * A class to implement publish-subscribe in memory in one JVM.
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
public class InMemoryPubSub {
    /**
     * The default way in which messages published in this PubSub are stored, which is just a standard queue.
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

    /**
     * Create a PubSub system.
     * 
     * @param corePoolSize the number of threads handling messages that are published by all publishers.
     * @param queueCreator the queue to store all message across all subscribers.
     * @param subscriptionMessageExceptionHandler the general subscription handler for exceptions arising from all subscribers.
     */
    public InMemoryPubSub(int corePoolSize, Supplier<Queue<Subscriber>> queueCreator, SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler) {
        executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(corePoolSize, createThreadFactory());
        masterList = queueCreator.get();
        this.subscriptionMessageExceptionHandler = subscriptionMessageExceptionHandler;
        startThreads();
        Runtime.getRuntime().addShutdownHook(generateShutdownThread());
    }

    private static ThreadFactory createThreadFactory() {
        final AtomicInteger count = new AtomicInteger();
        return runnable -> {
            ThreadGroup threadGroup = new ThreadGroup("PubSubListener");
            return new Thread(threadGroup, runnable, "PubSubListener" + "-" + count.incrementAndGet());
        };
    }

    /**
     * Class representing the publisher.
     * 
     * <p>In implementation it has a topic, publisher class, list of subscribers, and pointer to the lock in the outer PubSub class. 
     */
    public class Publisher {
        private final @Nonnull String topic;
        private final @Nonnull Class<?> publisherClass;
        private final @Nonnull Collection<Subscriber> subscribers = new ArrayList<>();

        private Publisher(@Nonnull String topic, @Nonnull Class<?> publisherClass) {
            this.topic = topic;
            this.publisherClass = publisherClass;
        }

        public <T extends CloneableObject<?>> void publish(@Nonnull T message) {
            InMemoryPubSub.this.lock.lock();
            try {
                for (var subscriber : subscribers) {
                    if (subscriber.subscriberClass.isInstance(message)) {
                        CloneableObject<?> copy = (CloneableObject<?>) message.clone();
                        subscriber.addMessage(copy);
                    }
                }
            } finally {
                InMemoryPubSub.this.lock.unlock();
            }
        }

        @Nonnull
        public String getTopic() {
            return topic;
        }
    }

    /**
     * Class representing a subscriber.
     * 
     * <p>In implementation it has a topic, name, subscriber class (which must be the same as or inherit from the publisher class), callback function,
     * list of messages to process, and pointer to the lock and condition and master list in the outer PubSub class. 
     */
    public class Subscriber {
        private final @Nonnull String topic;
        private final @Nonnull String subsriberName;
        private final @Nonnull Class<?> subscriberClass; // same as or inherits from publisherClass
        private final @Nonnull Consumer<? super CloneableObject<?>> callback;
        private final @Nonnull Queue<CloneableObject<?>> messages = new ArrayDeque<>();

        private Subscriber(@Nonnull String topic, @Nonnull String subscriberName, @Nonnull Class<?> subscriberClass, @Nonnull Consumer<? super CloneableObject<?>> callback) {
            this.topic = topic;
            this.subsriberName = subscriberName;
            this.subscriberClass = subscriberClass;
            this.callback = callback;
        }

        public void addMessage(CloneableObject<?> message) {
            if (!(subscriberClass.isInstance(message))) {
                throw new IllegalArgumentException("message must be or inherit from " + subscriberClass.getName());
            }
            InMemoryPubSub.this.lock.lock();
            try {
                messages.offer(message);
                InMemoryPubSub.this.masterList.add(this);
                InMemoryPubSub.this.notEmpty.signal();
            } finally {
                InMemoryPubSub.this.lock.unlock();
            }
        }

        @Nonnull
        public String getTopic() {
            return topic;
        }

        @Nonnull
        public String getSubscriberName() {
            return subsriberName;
        }
    }

    /**
     * Class used when subscriber created before publisher.
     * When publisher created, this class PubSub makes calls to addSubscriber.
     */
    private static class DeferredSubscriber {
        private final @Nonnull Class<?> subscriberClass;
        private final @Nonnull String subsriberName;
        private final @Nonnull Consumer<CloneableObject<?>> callback;

        private DeferredSubscriber(@Nonnull String subscriberName, @Nonnull Class<?> subscriberClass, @Nonnull Consumer<CloneableObject<?>> callback) {
            this.subsriberName = subscriberName;
            this.subscriberClass = subscriberClass;
            this.callback = callback;
        }
    }

    /**
     * Create a publisher.
     * 
     * @param <T> the type of object this publisher publishes. Must implement CloneableObject.
     * @param topic the topic
     * @param publisherClass same as T.class
     * @return a new publisher
     * @throws IllegalArgumentException if the publisher already exists.
     * @throws IllegalArgumentException if subscriber class of deferred subscribers is not the same or inherits from the publisher class
     */
    public synchronized <T> Publisher createPublisher(String topic, Class<T> publisherClass) {
        var publisher = topicMap.get(topic);
        if (publisher != null) {
            throw new IllegalArgumentException("publisher already exists: " + topic);
        }
        publisher = new Publisher(topic, publisherClass);
        topicMap.put(topic, publisher);
        addDeferredSubscribers(topic);
        return publisher;
    }

    /**
     * Get an existing publisher.
     */
    public synchronized Optional<Publisher> getPublisher(String topic) {
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
     * @param <T> the type of object this subscriber receives. Must be the same as or inherit from the publisher class.
     * @param topic the topic
     * @param subscriberName the name of this subscriber, useful for debugging
     * @param subscriberClass same as T.class
     * @param callback the callback function
     * @return a new subscriber
     * @throws IllegalArgumentException if subscriber class is not the same or inherits from the publisher class
-     */
    @SuppressWarnings("unchecked")
    public synchronized <T extends CloneableObject<?>> Subscriber subscribe(@Nonnull String topic,
                                                                            @Nonnull String subscriberName,
                                                                            @Nonnull Class<T> subscriberClass,
                                                                            @Nonnull Consumer<T> callback) {
        Consumer<CloneableObject<?>> callbackCasted = (Consumer<CloneableObject<?>>) callback;
        return doSubscribe(topic, subscriberName, subscriberClass, callbackCasted);
    }

    private Subscriber doSubscribe(String topic, String subscriberName, Class<?> subscriberClass, Consumer<CloneableObject<?>> callback) {
        var subscriber = new Subscriber(topic, subscriberName, subscriberClass, callback);
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
    
    private Thread generateShutdownThread() {
        return new Thread(() -> {
            try {
                executorService.shutdownNow();
                executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
                boolean locked = lock.tryLock(1, TimeUnit.SECONDS);
                if (locked) {
                    try {
                        notEmpty.signalAll();
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException ignored) {
            }
        }, "PubSub.shutdown");
    }
}
