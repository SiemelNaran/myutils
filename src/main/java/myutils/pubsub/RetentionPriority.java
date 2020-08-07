package myutils.pubsub;


/**
 * Retention priority of messages published.
 * This only applies to a distributed publish/subscribe, as it controls how many messages of each priority are saved.
 */
public enum RetentionPriority implements Comparable<RetentionPriority> {
    MEDIUM,
    HIGH;
}