package myutils.util.concurrent;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.locks.Lock;
import java.util.function.Supplier;


/**
 * Helper class to lock based on a string. This class locks based on the hash code of the string/object.
 * 
 * <p>You cannot do something like synchronized(inputString) { ... }
 * because inputString in
 *     <code>inputString1 = "hello"></code><br>
 *     <code>inputString2 = "hello"></code><br>
 * are different objects, so locking on inputString would allow both threads to continue.
 * 
 * <p>Also <code>string.intern()</code> is not guaranteed to produce unique strings.
 * 
 * @param <T> the type of lock such as ReentrantLock or ReadWriteLock.
 */
public class HashLocks<T extends Lock> {
    private final Lock[] locks;
    
    /**
     * @param clazz the type of the lock, which must have a default constructor.
     * @param size the number of locks
     * @throws RuntimeException if there was an error instantiating an instance of the class.
     */
    HashLocks(Class<T> clazz, int size) {
        try {
            locks = new Lock[size];
            for (int i = 0; i < size; i++) {
                locks[i] = clazz.getDeclaredConstructor().newInstance();
            }
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * @param supplier function to create an instance of the lock
     * @param size the number of locks
     */
    HashLocks(Supplier<T> supplier, int size) {
        locks = new Lock[size];
        for (int i = 0; i < size; i++) {
            locks[i] = supplier.get();
        }

    }
    
    Lock getLock(Object key) {
        int index = Math.floorMod(key.hashCode(), locks.length);
        return locks[index];
    }
}
