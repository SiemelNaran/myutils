package org.sn.myutils.pubsub;

import static org.sn.myutils.pubsub.PubSubUtils.addShutdownHook;

import java.lang.ref.Cleaner;

/**
 * Represents that a class has a shutdown method.
 * This is so that we can add many Shutdowneable to an array and call shutdown on each of them.
 */
public abstract class Shutdowneable {
    private final ImmutableValue<Cleaner.Cleanable> cleanable = new ImmutableValue<>();

    public Shutdowneable() {
    }

    /**
     * After constructing an object that inherits from Shutdowneable, users
     * should call registerCleanable to have the cleaner close this object
     * properly at JVM exit.
     * <p>
     * If you explicitly call shutdown(), then the cleaner will do nothing.
     */
    public final void registerCleanable() {
        cleanable.set(addShutdownHook(this));
    }

    /**
     * Override this function to return the action to shut down the pubSub.
     * Derived implementations should return a runnable that does something and calls the base class runnable.
     */
    protected abstract Runnable shutdownAction();

    public final void shutdown() {
        cleanable.get().clean();
    }
}
