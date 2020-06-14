package myutils.util.concurrent;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import javax.annotation.Nonnull;
import myutils.util.MultimapUtils;


public class InMemoryPubSub {
    private final Map<String /*topic*/, Publisher> topicMap = new HashMap<>();
    private final Map<String /*topic*/, Collection<DeferredSubscriber>> deferredSubscribersMap = new HashMap<>();
    private final ExecutorService executorService;
    private final Queue<Subscriber> masterList = new ArrayDeque<>();
    private final ReentrantLock lock = new ReentrantLock();
    private final Condition notEmpty = lock.newCondition();
    private final SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler;

    public InMemoryPubSub(int corePoolSize, SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler) {
        executorService = Executors.newFixedThreadPool(corePoolSize, createThreadFactory());
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

    public class Publisher {
        private final @Nonnull String topic;
        private final @Nonnull Class<?> publisherClass;
        private final @Nonnull Collection<Subscriber> subscribers = new ArrayList<>();

        private Publisher(@Nonnull String topic, @Nonnull Class<?> publisherClass) {
            this.topic = topic;
            this.publisherClass = publisherClass;
        }

        public <T extends CloneableObject<?>> void publish(@Nonnull T message) {
            lock.lock();
            try {
                for (var subscriber : subscribers) {
                    if (subscriber.subscriberClass.isInstance(message)) {
                        CloneableObject<?> copy = (CloneableObject<?>) message.clone();
                        subscriber.addMessage(copy);
                    }
                }
            } finally {
                lock.unlock();
            }
        }

        @Nonnull
        public String getTopic() {
            return topic;
        }
    }

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
            lock.lock();
            try {
                messages.offer(message);
                masterList.add(this);
                notEmpty.signal();
            } finally {
                lock.unlock();
            }
        }

        @Nonnull
        public String getTopic() {
            return topic;
        }

        public String getSubscriberName() {
            return subsriberName;
        }
    }

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

    public synchronized void unsubscribe(Subscriber subscriber) {
        var publisher = topicMap.get(subscriber.topic);
        if (publisher == null) {
            return;
        }
        publisher.subscribers.remove(subscriber);
    }

    private void startThreads() {
        executorService.submit(new Listener(masterList, lock, notEmpty, subscriptionMessageExceptionHandler));
    }

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
            while (!Thread.currentThread().isInterrupted()) {
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

    public interface SubscriptionMessageExceptionHandler {
        /**
         * Handle exception thrown when processing a subscription message.
         *
         * @param e the exception, which is either a RuntimeException or Error
         */
        void handleException(Subscriber subscriber, CloneableObject<?> message, Throwable e);
    }

    public static SubscriptionMessageExceptionHandler defaultSubscriptionMessageExceptionHandler() {
        return defaultSubscriptionMessageExceptionHandler;
    }

    private static final SubscriptionMessageExceptionHandler defaultSubscriptionMessageExceptionHandler = new DefaultSubscriptionMessageExceptionHandler();

    public static class DefaultSubscriptionMessageExceptionHandler implements SubscriptionMessageExceptionHandler {
        private static final System.Logger LOGGER = System.getLogger(DefaultSubscriptionMessageExceptionHandler.class.getName());

        @Override
        public void handleException(Subscriber subcriber, CloneableObject<?> message, Throwable e) {
            LOGGER.log(System.Logger.Level.WARNING, "Exception invoking subscriber", e);
        }
    }

    private Thread generateShutdownThread() {
        return new Thread(() -> {
            try {
                executorService.shutdownNow();
                executorService.awaitTermination(100, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ignored) {
            }
        }, "PubSub.shutdown");
    }


    public interface CloneableObject<T> extends Cloneable {
        T clone();
    }
}
