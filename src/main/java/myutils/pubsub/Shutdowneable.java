package myutils.pubsub;

/**
 * Represents that a class has a shutdown method.
 * This is so that we can add many Shutdowneable to an array and call shutdown on each of them.
 */
public interface Shutdowneable {
    void shutdown();
}
