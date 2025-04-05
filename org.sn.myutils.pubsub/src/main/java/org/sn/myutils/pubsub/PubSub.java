package org.sn.myutils.pubsub;

import static org.sn.myutils.pubsub.PubSubUtils.closeExecutorQuietly;
import static org.sn.myutils.util.concurrent.MoreExecutors.createThreadFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.Nullable;
import org.sn.myutils.util.MultimapUtils;


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
 * provide custom behavior such as logging a shorter message or resubmitting the message to the subscription handler.
 * The handler is responsible for taking care of the backoff strategy.
 */
public abstract class PubSub extends Shutdowneable {
    /**
     * The default way in which messages to process in this PubSub are stored, which is just a standard queue.
     */
    public static Supplier<Queue<Subscriber>> defaultQueueCreator() {
        return ArrayDeque::new;
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
        public void handleException(Subscriber subscriber, CloneableObject<?> message, Throwable e) {
            LOGGER.log(System.Logger.Level.WARNING, "Exception invoking subscriber", e);
        }
    }

    private final Map<String /*topic*/, Publisher> topicMap = new HashMap<>();
    private final Map<String /*topic*/, Collection<Subscriber>> deferredSubscribersMap = new HashMap<>();
    private final ThreadPoolExecutor executorService;
    private final Queue<Subscriber> masterList;
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler;


    /**
     * Arguments necessary to create a PubSub system.
     *
     * @param numInMemoryHandlers                 the number of threads handling messages that are published by all publishers.
     * @param queueCreator                        the queue to store all message across all subscribers.
     * @param subscriptionMessageExceptionHandler the general subscription handler for exceptions arising from all subscribers.
     */
    public record PubSubConstructorArgs(int numInMemoryHandlers,
                                        Supplier<Queue<Subscriber>> queueCreator,
                                        SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler) {
    }

    /**
     * Create a PubSub system.
     */
    public PubSub(PubSubConstructorArgs args) {
        this.executorService = (ThreadPoolExecutor) Executors.newFixedThreadPool(args.numInMemoryHandlers, createThreadFactory("PubSubListener", true));
        this.masterList = args.queueCreator.get();
        this.subscriptionMessageExceptionHandler = args.subscriptionMessageExceptionHandler;
        startThreads();
    }

    @Override
    protected Runnable shutdownAction() {
        return () -> closeExecutorQuietly(executorService);
    }

    /**
     * Class representing the publisher.
     * 
     * <p>In implementation, it has a topic, publisher class, list of subscribers, and pointer to the lock in the outer PubSub class.
     */
    public abstract class Publisher {
        private final long createdAtTimestamp;
        private final @NotNull String topic;
        private final @NotNull Class<?> publisherClass;
        private final @NotNull Collection<Subscriber> subscribers = new ArrayList<>();

        protected Publisher(@NotNull String topic, @NotNull Class<?> publisherClass) {
            this.createdAtTimestamp = System.currentTimeMillis();
            this.topic = topic;
            this.publisherClass = publisherClass;
        }

        protected long getCreatedAtTimestamp() {
            return createdAtTimestamp;
        }
        
        public @NotNull String getTopic() {
            return topic;
        }
        
        protected @NotNull Class<?> getPublisherClass() {
            return publisherClass;
        }
        
        protected final void addSubscriber(Subscriber subscriber) {
            subscribers.add(subscriber);
        }

        /**
         * Return subscribers in the order they were created.
         */
        protected @NotNull List<Subscriber> getSubscribers() {
            return new ArrayList<>(subscribers);
        }
        
        public final <T extends CloneableObject<?>> void publish(@NotNull T message) {
            publish(message, RetentionPriority.MEDIUM);
        }
        
        public <T extends CloneableObject<?>> void publish(@NotNull T message, RetentionPriority priority) {
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
     * <p>In implementation, it has a topic, name, subscriber class (which must be the same as or inherit from the publisher class), callback function,
     * list of messages to process, and pointer to the lock and condition and master list in the outer PubSub class. 
     */
    public abstract class Subscriber {
        private final long createdAtTimestamp;
        private final @NotNull String topic;
        private final @NotNull String subscriberName;
        private final @NotNull Class<? extends CloneableObject<?>> subscriberClass; // same as or inherits from publisherClass
        private final @NotNull Consumer<CloneableObject<?>> callback;
        private final @NotNull Queue<CloneableObject<?>> messages = new ArrayDeque<>();

        protected Subscriber(@NotNull String topic,
                             @NotNull String subscriberName,
                             @NotNull Class<? extends CloneableObject<?>> subscriberClass,
                             @NotNull Consumer<CloneableObject<?>> callback) {
            this.createdAtTimestamp = System.currentTimeMillis();
            this.topic = topic;
            this.subscriberName = subscriberName;
            this.subscriberClass = subscriberClass;
            this.callback = callback;
        }
        
        protected long getCreatedAtTimestamp() {
            return createdAtTimestamp;
        }

        @NotNull
        public String getTopic() {
            return topic;
        }

        @NotNull
        public String getSubscriberName() {
            return subscriberName;
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
     * Create a publisher.
     * 
     * @param <T> the type of object this publisher publishes. Need not implement CloneableObject as the derived classes may implement it.
     * @param topic the topic
     * @param publisherClass same as T.class
     * @return a new publisher
     * @throws IllegalArgumentException if the publisher already exists.
     * @throws IllegalArgumentException if subscriber class of deferred subscribers is not the same or inherits from the publisher class
     * @throws IllegalStateException if publisher already exists
     */
    public final synchronized <T> Publisher createPublisher(String topic, Class<T> publisherClass) {
        var publisher = topicMap.get(topic);
        if (publisher != null) {
            throw new IllegalStateException("publisher already exists: topic=" + topic);
        }
        onBeforeAddPublisher(topic, publisherClass);
        publisher = newPublisher(topic, publisherClass);
        registerPublisher(publisher);
        addDeferredSubscribers(publisher);
        onPublisherAdded(publisher);
        return publisher;
    }
    
    protected abstract <T> Publisher newPublisher(String topic, Class<T> publisherClass);
    
    /**
     * Register a new publisher.
     * The in-memory pubsub just calls addPublisher, but the distributed pubsub does something more complicated.
     */
    protected abstract void registerPublisher(Publisher publisher);
    
    /**
     * Register a new subscriber.
     * 
     * @param publisher the publisher this subscriber belongs too, null if the publisher does not yet exist
     * @param subscriber the subscriber
     * @param deferred true if subscriber is being added as a deferred subscriber (important as registerSubscriber was called earlier with deferred=false)
     */
    protected abstract void registerSubscriber(@Nullable Publisher publisher, Subscriber subscriber, boolean deferred);

    /**
     * Unregister a subscriber.
     * 
     * @param publisher the publisher this subscriber belongs too, null if the publisher does not yet exist
     * @param subscriber the subscriber
     */
    protected abstract void unregisterSubscriber(@Nullable Publisher publisher, Subscriber subscriber, boolean isDeferred);

    protected final void addPublisher(Publisher publisher) {
        topicMap.put(publisher.getTopic(), publisher);
    }

    /**
     * Get an existing publisher.
     */
    public synchronized Publisher getPublisher(@NotNull String topic) {
        return topicMap.get(topic);
    }

    /**
     * Apply an action to all publishers.
     */
    protected final synchronized void forEachPublisher(Consumer<Publisher> action) {
        topicMap.values().forEach(action);
    }
    
    private void addDeferredSubscribers(Publisher publisher) {
        var subscribers = deferredSubscribersMap.remove(publisher.getTopic());
        if (subscribers != null) {
            for (var subscriber : subscribers) {
                registerSubscriber(publisher, subscriber, true);
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
     * @throws IllegalStateException if the PubSub is already subscribed to the topic with the same subscriberName
-     */
    @SuppressWarnings("unchecked")
    public final synchronized <T extends CloneableObject<?>> Subscriber subscribe(@NotNull String topic,
                                                                                  @NotNull String subscriberName,
                                                                                  @NotNull Class<T> subscriberClass,
                                                                                  @NotNull Consumer<T> callback) {
        Consumer<CloneableObject<?>> callbackCasted = (Consumer<CloneableObject<?>>) callback;
        Supplier<Subscriber> subscriberCreator = () -> {
            onBeforeAddSubscriber(topic, subscriberName, subscriberClass);
            return newSubscriber(topic, subscriberName, subscriberClass, callbackCasted);
        };
        final Subscriber subscriber;
        var publisher = topicMap.get(topic);
        if (publisher != null) {
            if (publisher.subscribers.stream().anyMatch(s -> s.getSubscriberName().equals(subscriberName))) {
                throw new IllegalStateException("already subscribed: topic=" + topic + ", subscriberName=" + subscriberName);
            }
            checkClassType(publisher.publisherClass, subscriberClass);
            subscriber = subscriberCreator.get();
        } else {
            MultimapUtils<String, Subscriber> multimap = new MultimapUtils<>(deferredSubscribersMap, ArrayList::new);
            var deferredSubscribers = multimap.getOrCreate(topic);
            if (deferredSubscribers.stream().anyMatch(s -> s.getSubscriberName().equals(subscriberName))) {
                throw new IllegalStateException("already subscribed: topic=" + topic + ", subscriberName=" + subscriberName);
            }
            subscriber = subscriberCreator.get();
            deferredSubscribers.add(subscriber);
        }
        registerSubscriber(publisher, subscriber, false);
        return subscriber;
    }
    
    protected abstract Subscriber newSubscriber(@NotNull String topic,
                                                @NotNull String subscriberName,
                                                @NotNull Class<? extends CloneableObject<?>> subscriberClass,
                                                @NotNull Consumer<CloneableObject<?>> callback);

    private static void checkClassType(Class<?> publisherClass, Class<?> subscriberClass) {
        if (!(publisherClass.isAssignableFrom(subscriberClass))) {
            throw new IllegalArgumentException("subscriber class must be the same as or inherit from the publisher class:"
                    + " publisherClass=" + publisherClass.getName()
                    + " subscriberClass=" + subscriberClass.getName());
        }
    }

    /**
     * Attempt to unsubscribe a subscriber.
     * Running time O(k) where k is the number of subscribers.
     *
     * @throws IllegalStateException if not subscribed
     * @see PubSub#basicUnsubscribe(String, String, boolean) for more notes on running time.
     */
    public synchronized void unsubscribe(Subscriber findSubscriber) {
        String topic = findSubscriber.getTopic();
        var publisher = topicMap.get(topic);
        var deferredSubscriberList = deferredSubscribersMap.get(topic);
        boolean foundDeferredSubscriber = deferredSubscriberList != null && deferredSubscriberList.stream().anyMatch(subscriber -> subscriber == findSubscriber);
        if (!foundDeferredSubscriber) {
            boolean foundSubscriber = publisher != null && publisher.subscribers.stream().anyMatch(subscriber -> subscriber == findSubscriber);
            if (!foundSubscriber) {
                throw new IllegalStateException("not subscribed: topic=" + topic + ", subscriberName=" + findSubscriber.getSubscriberName());
            }
        }
        unregisterSubscriber(publisher, findSubscriber, foundDeferredSubscriber);
    }

    /**
     * Really unsubscribe a subscriber from this PubSub.
     * Running time O(N) where N is the total number of messages in all subscribers.
     * 
     * @param topic the topic to unsubscribe
     * @param findSubscriberName the subscriber name to unsubscribe
     * @param isDeferred true if this is a deferred subscriber
     */
    protected synchronized void basicUnsubscribe(String topic, String findSubscriberName, boolean isDeferred) {
        if (isDeferred) {
            MultimapUtils<String, Subscriber> multimap = new MultimapUtils<>(deferredSubscribersMap, ArrayList::new);
            multimap.removeIf(topic, subscriber -> subscriber.getSubscriberName().equals(findSubscriberName));
        } else {
            var publisher = topicMap.get(topic);
            if (publisher != null) {
                masterList.removeIf(subscriber -> subscriber.getSubscriberName().equals(findSubscriberName));
                publisher.subscribers.removeIf(subscriber -> subscriber.getSubscriberName().equals(findSubscriberName));
            }
        }
    }
    
    protected <T> void onBeforeAddPublisher(String topic, Class<T> publisherClass) {
    }
    
    protected <T> void onBeforeAddSubscriber(String topic, String subscriberName, Class<T> subscriberClass) {
    }

    protected void onPublisherAdded(Publisher publisher) {
    }
    
    private void startThreads() {
        for (int i = 0; i < executorService.getCorePoolSize(); i++) {
            executorService.submit(new Listener(masterList, lock, notEmpty, subscriptionMessageExceptionHandler));
        }
    }

    /**
     * Thread that listens for a new message in any subscriber queue and process it by cloning it
     * and passing it to all the relevant subscribers.
     */
    private record Listener(Queue<Subscriber> masterList, Lock lock, Condition notEmpty,
                            SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler) implements Runnable {
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
}
