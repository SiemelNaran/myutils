package myutils.util.concurrent;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Class to lock based on a string/object. This class locks based on the hash code of the string/object.
 * 
 * <p>You cannot do something like synchronized(inputString) { ... }
 * because inputString in
 *     <code>inputString1 = "hello"></code><br>
 *     <code>inputString2 = "hello"></code><br>
 * are different objects, so locking on inputString would allow both threads to continue.
 * 
 * <p>Also <code>string.intern()</code> is not guaranteed to produce unique strings, and besides may be too slow.
 */
public class HashLocks {
    private final TimedReentrantLock[] locks;
    private final List<Map<String, Void>> collisionTrackingList;
    
    /**
     * Create a HashLocks object without collision tracking.
     * 
     * @param hashLocksSize the number of locks
     * @param fair should the locks be fair
     */
    public HashLocks(int hashLocksSize, boolean fair) {
        this(hashLocksSize, fair, null);
    }
    
    /**
     * Create a HashLocks object.
     * 
     * @param hashLocksSize the number of locks
     * @param fair should the locks be fair
     * @param collisionTracking if not null then perform tracking to see how many keys, as defined by key.toString(), map to the same hash code.
     */
    public HashLocks(int hashLocksSize, boolean fair, CollisionTracking collisionTracking) {
        locks = new TimedReentrantLock[hashLocksSize];
        for (int i = 0; i < hashLocksSize; i++) {
            locks[i] = new TimedReentrantLock(fair);
        }
        if (collisionTracking != null) {
            collisionTrackingList = new ArrayList<Map<String, Void>>(hashLocksSize);
            for (int i = 0; i < hashLocksSize; i++) {
                collisionTrackingList.add(Collections.synchronizedMap(new HashLocksKeyMap(collisionTracking)));
            }
        } else {
            collisionTrackingList = null;
        }
    }
    
    /**
     * Return the ReentrantLock for 'key' based on the hash code of 'key'.
     */
    public ReentrantLock getLock(Object key) {
        int index = Math.floorMod(key.hashCode(), locks.length);
        if (collisionTrackingList != null) {
            collisionTrackingList.get(index).put(key.toString(), null);
        }
        return locks[index];
    }
    
    /**
     * Return the statistics of each lock.
     */
    public Stream<Statistics> statistics() {
        return IntStream.range(0, locks.length)
                        .mapToObj(index -> new Statistics(locks[index], collisionTrackingList != null ? collisionTrackingList.get(index) : null));
    }
    

    /**
     * Class to store the N most recently strings that map to each reentrant lock.
     * Only used when doCollisionTracking is true.
     */
    private static final class HashLocksKeyMap extends LinkedHashMap<String, Void> {
        private static final long serialVersionUID = 1L;
        
        private final CollisionTracking collisionTracking;
        
        private HashLocksKeyMap(@Nonnull CollisionTracking collisionTracking) {
            this.collisionTracking = collisionTracking;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Void> eldest) {
            return size() > collisionTracking.numStringsPerHashCode;
        }
    }
    
    
    /**
     * The statistics of each lock.
     * Use to fine-tune hashLocksSize.
     */
    public static final class Statistics {
        private final boolean locked;
        private final int queueLength;
        private final Duration totalWaitTime;
        private final Duration totalLockRunningTime;
        private final Duration approximateTotalIdleTime;
        private final int usage;
        
        private Statistics(TimedReentrantLock lock, @Nullable Map<String, Void> collisionTracking) {
            this.locked = lock.isLocked();
            this.queueLength = lock.getQueueLength();
            this.totalWaitTime = lock.getTotalWaitTime();
            this.totalLockRunningTime = lock.getTotalLockRunningTime();
            this.approximateTotalIdleTime = lock.getApproximateTotalIdleTime();
            this.usage = collisionTracking != null ? collisionTracking.size() : -1;
        }
        
        /**
         * Tells whether the lock is locked right now.
         */
        public boolean isLocked() {
            return locked;
        }

        /**
         * An estimate of the number of threads waiting on this lock right now.
         */
        public int getQueueLength() {
            return queueLength;
        }

        /**
         * The total time to lock the lock.
         */
        public Duration getTotalWaitTime() {
            return totalWaitTime;
        }

        /**
         * The time from the time the lock was acquired to unlock.
         */
        public Duration getTotalLockRunningTime() {
            return totalLockRunningTime;
        }
        
        /**
         * The approximate time the lock has not bee in use.
         * This is simply the difference between the lock creation time and now, and the total lock running time.
         * 
         * <p>It is approximate because if the lock is in use at the time this function called,
         * totalLockRunningTime has not been updated (it is only updated upon unlock).
         */
        public Duration getApproximateTotalIdleTimes() {
            return approximateTotalIdleTime;
        }

        /**
         * The number of distinct strings using this lock.
         * 0 indicates that hashLocksSize is too large.
         * >1 indicates that hashLocksSize is too small.
         */
        public int getUsage() {
            return usage;
        }
    }
    
    
    public static class CollisionTracking {
        private final int numStringsPerHashCode;

        public static CollisionTrackingBuilder newBuilder() {
            return new CollisionTrackingBuilder();
        }
        
        private CollisionTracking(int numStringsPerHashCode) {
            this.numStringsPerHashCode = numStringsPerHashCode;
        }
        
        int getNumStringsPerHashCode() {
            return numStringsPerHashCode;
        }
    }
    
    public static class CollisionTrackingBuilder {
        private static final int DEFAULT_NUM_STRINGS_PER_HASHCODE = 5;
        
        private int numStringsPerHashCode = DEFAULT_NUM_STRINGS_PER_HASHCODE;
        
        public CollisionTrackingBuilder() {
        }
        
        public CollisionTrackingBuilder setNumStringsPerHashCode(int numStringsPerHashCode) {
            this.numStringsPerHashCode = numStringsPerHashCode;
            return this;
        }
        
        public CollisionTracking build() {
            return new CollisionTracking(numStringsPerHashCode);
        }
    }
}
