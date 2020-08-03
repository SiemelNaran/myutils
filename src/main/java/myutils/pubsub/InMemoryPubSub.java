package myutils.pubsub;

import java.util.function.Consumer;
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
     * @param baseArgs the arguments for the in-memory pubsub system
     * @see PubSubConstructorArgs for the arguments to the super class
     */
    public InMemoryPubSub(PubSubConstructorArgs baseArgs) {
        super(baseArgs);
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
