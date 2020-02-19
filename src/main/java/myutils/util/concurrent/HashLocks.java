package myutils.util.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import javax.annotation.Nonnull;


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
 * 
 * @param <LockType> The type of lock, such as ReentrantLock or TimedReentrantLock
 * @param <LockStatisticsType> Statistics of the lock
 */
public class HashLocks<LockType, LockStatisticsType> {
    private final List<LockType> locks;
    private final BiFunction<LockType, Set<String>, LockStatisticsType> toLockStatistics;
    private final List<Set<String>> collisionTrackingList;
    
    /**
     * Create a HashLocks object without collision tracking.
     * 
     * @param hashLocksSize the number of locks
     * @param lockCreator function that creates a lock, for example <code>() -> new ReentrantLock(true)</code>
     * @param toLockStatistics function that creates statistics out of a lock
     */
    public static <LockType, LockStatisticsType> HashLocks<LockType, LockStatisticsType> create(int hashLocksSize,
                                                                                                Supplier<LockType> lockCreator,
                                                                                                BiFunction<LockType, Set<String>, LockStatisticsType> toLockStatistics) {
        return new HashLocks<>(hashLocksSize, lockCreator, toLockStatistics, null);
    }

    /**
     * Create a HashLocks object.
     * 
     * @param hashLocksSize the number of locks
     * @param lockCreator function that creates a lock, for example <code>() -> new ReentrantLock(true)</code>
     * @param toLockStatistics function that creates statistics out of a lock
     * @param collisionTracking if not null then perform tracking to see how many keys, as defined by key.toString(), map to the same hash code.
     */
    public static <LockType, LockStatisticsType> HashLocks<LockType, LockStatisticsType> create(int hashLocksSize,
                                                                                                Supplier<LockType> lockCreator,
                                                                                                BiFunction<LockType, Set<String>, LockStatisticsType> toLockStatistics,
                                                                                                CollisionTracking collisionTracking) {
        return new HashLocks<>(hashLocksSize, lockCreator, toLockStatistics, collisionTracking);
    }

    private HashLocks(int hashLocksSize,
                      Supplier<LockType> lockCreator,
                      BiFunction<LockType, Set<String>, LockStatisticsType> toLockStatistics,
                      CollisionTracking collisionTracking) {
        this.locks = new ArrayList<LockType>(hashLocksSize);
        this.toLockStatistics = toLockStatistics;
        for (int i = 0; i < hashLocksSize; i++) {
            locks.add(lockCreator.get());
        }
        if (collisionTracking != null) {
            this.collisionTrackingList = new ArrayList<Set<String>>(hashLocksSize);
            for (int i = 0; i < hashLocksSize; i++) {
                collisionTrackingList.add(Collections.synchronizedSet(Collections.newSetFromMap(new HashLocksKeyMap(collisionTracking))));
            }
        } else {
            this.collisionTrackingList = null;
        }
    }
    
    /**
     * Return the ReentrantLock for 'key' based on the hash code of 'key'.
     */
    public LockType getLock(Object key) {
        int index = Math.floorMod(key.hashCode(), locks.size());
        if (collisionTrackingList != null) {
            collisionTrackingList.get(index).add(key.toString());
        }
        return locks.get(index);
    }
    
    /**
     * Return the statistics of each lock.
     */
    public Stream<LockStatisticsType> statistics() {
        return IntStream.range(0, locks.size())
                        .mapToObj(index -> toLockStatistics.apply(locks.get(index), collisionTrackingList != null ? collisionTrackingList.get(index) : null));
    }
    

    /**
     * Class to store the N most recently strings that map to each reentrant lock.
     * Only used when doCollisionTracking is true.
     */
    private static final class HashLocksKeyMap extends LinkedHashMap<String, Boolean> {
        private static final long serialVersionUID = 1L;
        
        private final CollisionTracking collisionTracking;
        
        private HashLocksKeyMap(@Nonnull CollisionTracking collisionTracking) {
            this.collisionTracking = collisionTracking;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return size() > collisionTracking.numStringsPerHashCode;
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
