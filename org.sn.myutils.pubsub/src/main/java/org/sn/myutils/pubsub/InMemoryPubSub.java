package org.sn.myutils.pubsub;

import java.util.function.Consumer;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.Nullable;


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
        private InMemoryPublisher(@NotNull String topic, @NotNull Class<?> publisherClass) {
            super(topic, publisherClass);
        }
    }

    public final class InMemorySubscriber extends Subscriber {
        private InMemorySubscriber(@NotNull String topic,
                                   @NotNull String subscriberName,
                                   @NotNull Class<? extends CloneableObject<?>> subscriberClass,
                                   @NotNull Consumer<CloneableObject<?>> callback) {
            super(topic, subscriberName, subscriberClass, callback);
        }
    }

    @Override
    protected <T> Publisher newPublisher(String topic, Class<T> publisherClass) {
        return new InMemoryPublisher(topic, publisherClass);
    }
    
    @Override
    protected Subscriber newSubscriber(@NotNull String topic,
                                       @NotNull String subscriberName,
                                       @NotNull Class<? extends CloneableObject<?>> subscriberClass,
                                       @NotNull Consumer<CloneableObject<?>> callback) {
        return new InMemorySubscriber(topic, subscriberName, subscriberClass, callback);
    }

    @Override
    protected void registerPublisher(Publisher publisher) {
        addPublisher(publisher);
    }
    
    @Override
    protected void registerSubscriber(@Nullable Publisher publisher, Subscriber subscriber, boolean deferred) {
        if (publisher != null) {
            publisher.addSubscriber(subscriber);
        }
    }
    
    @Override
    protected void unregisterSubscriber(@Nullable Publisher publisher, Subscriber subscriber, boolean isDeferred) {
        basicUnsubscribe(subscriber.getTopic(), subscriber.getSubscriberName(), isDeferred);
    }
}
