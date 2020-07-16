package myutils.pubsub;


/**
 * Priority of messages published.
 * This only applies to a distributed publish/subscribe, as it controls how many messages of each priority are maintained.
 */
public enum MessagePriority {
    HIGH(10),
    MEDIUM(5);
    
    private final int level;
    
    MessagePriority(int level) {
        this.level = level;
    }
    
    int getLevel() {
        return level;
    }
}