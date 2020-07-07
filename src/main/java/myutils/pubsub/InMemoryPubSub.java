package myutils.pubsub;

import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * {@inheritDoc}
 * 
 * <p>This class implements publish-subscribe in memory in one JVM.
 */
public class InMemoryPubSub extends PubSub {
    /**
     * Create a PubSub system.
     * 
     * @param numInMemoryHandlers the number of threads handling messages that are published by all publishers.
     * @param queueCreator the queue to store all message across all subscribers.
     * @param subscriptionMessageExceptionHandler the general subscription handler for exceptions arising from all subscribers.
     */
    public InMemoryPubSub(int numInMemoryHandlers,
                          Supplier<Queue<Subscriber>> queueCreator,
                          SubscriptionMessageExceptionHandler subscriptionMessageExceptionHandler) {
        super(numInMemoryHandlers, queueCreator, subscriptionMessageExceptionHandler);
    }

    public final class InMemoryPublisher extends Publisher {
        private InMemoryPublisher(@Nonnull String topic, @Nonnull Class<?> publisherClass) {
            super(topic, publisherClass);
        }
    }

    public final class InMemorySubscriber extends Subscriber {
        private InMemorySubscriber(@Nonnull String topic,
                                   @Nonnull String subscriberName,
                                   @Nonnull Class<? extends CloneableObject<?>> subscriberClass,
                                   @Nonnull Consumer<CloneableObject<?>> callback) {
            super(topic, subscriberName, subscriberClass, callback);
        }
    }

    @Override
    protected <T> Publisher newPublisher(String topic, Class<T> publisherClass) {
        return new InMemoryPublisher(topic, publisherClass);
    }
    
    @Override
    protected Subscriber newSubscriber(@Nonnull String topic,
                                       @Nonnull String subscriberName,
                                       @Nonnull Class<? extends CloneableObject<?>> subscriberClass,
                                       @Nonnull Consumer<CloneableObject<?>> callback) {
        return new InMemorySubscriber(topic, subscriberName, subscriberClass, callback);
    }
}
