package myutils.pubsub;


/**
 * Retention priority of messages published.
 * This only applies to a distributed publish/subscribe, as it controls how many messages of each priority are saved.
 */
public enum RetentionPriority {
    HIGH(10),
    MEDIUM(5);
    
    private final int level;
    
    RetentionPriority(int level) {
        this.level = level;
    }

    int getLevel() {
        return level;
    }
}