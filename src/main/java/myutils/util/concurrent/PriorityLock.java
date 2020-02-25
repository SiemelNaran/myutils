package myutils.util.concurrent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;


/**
 * A lock which builds upon reentrant lock by giving preference to the queue with the highest priority.
 * 
 * <ul>Here is how this class implements it:
 *   <li>This class has an internal ReentrantLock.
 *   <li>In general, threads have priorities between 1 and 10 inclusive, with 5 being the default priority.
 *           Thus a PriorityLock has 10 counters and 10 conditions based on the internal ReentrantLock.</li>
 *   <li>When a thread of priority N requests a lock,
 *           the lock makes a note that a thread of priority N is waiting for a lock by incrementing the counter for this priority.</li>
 *   <li>When a thread gets the internal lock, we check if there are any threads with higher priority in the queue (simply by checking if the count > 0</li>
 *   <li>If there is, then we await on that priority's condition</li>
 *   <li>When unlock is called we decrement the count for the thread's priority,
 *           and signal the condition object so that any threads waiting on this condition wake up and start running.</li>
 * </ul>
 * 
 * @see Thread#MIN_PRIORITY
 * @see Thread#NORM_PRIORITY
 * @see Thread#MAX_PRIORITY
 */
public class PriorityLock implements Lock {
    private static class Level {
        private final AtomicInteger count = new AtomicInteger();
        private final Condition condition;

        private Level(Lock lock) {
            condition = lock.newCondition();
        }
    }

    private final Lock lock;
    private final Level[] levels;
    private int originalPriority;

    /**
     * Create a priority lock backed by a non-fair ReentrantLock.
     */
    public PriorityLock() {
        this(false);
    }
    
    /**
     * Create a priority lock backed by a ReentrantLock.
     */
    public PriorityLock(boolean fair) {
        this(new ReentrantLock(fair));
    }
    
    /**
     * Create a priority lock backed by the given lock.
     * Implementations must be sure that no two PriorityLock's share the same lock.
     * 
     * @param the internal lock
     */
    protected PriorityLock(Lock lock) {
        this.lock = lock;
        this.levels = new Level[Thread.MAX_PRIORITY];
        for (int i = 0; i < Thread.MAX_PRIORITY; i++) {
            this.levels[i] = new Level(lock);
        }
    }

    @Override
    public void lock() {
        Thread currentThread = Thread.currentThread();
        int priority = addThread(currentThread);
        try {
            lock.lock();
            try {
                waitForHigherPriorityTasksToFinish(currentThread);
            } catch (InterruptedException e) {
                throw new CancellationException(e.getMessage());
            }
        } catch (RuntimeException | Error e) {
            cleanup(priority, e);
            throw e;
        }
        originalPriority = priority;
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        Thread currentThread = Thread.currentThread();
        int priority = addThread(currentThread);
        try {
            lock.lockInterruptibly();
            waitForHigherPriorityTasksToFinish(currentThread);
        } catch (InterruptedException e) {
            removeThread(priority);
            throw e;
        } catch (RuntimeException | Error e) {
            cleanup(priority, e);
            throw e;
        }
        originalPriority = priority;
    }

    @Override
    public boolean tryLock() {
        Thread currentThread = Thread.currentThread();
        int priority = addThread(currentThread);
        try {
            boolean acquired = lock.tryLock();
            if (acquired) {
                Integer nextHigherPriority = computeNextHigherPriorityIndex(priority);
                if (nextHigherPriority != null) {
                    lock.unlock();
                    acquired = false;
                    removeThread(priority);
                } else {
                    originalPriority = priority;
                }
            } else {
                removeThread(priority);
            }
            return acquired;
        } catch (RuntimeException | Error e) {
            cleanup(priority, e);
            throw e;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        Thread currentThread = Thread.currentThread();
        time = unit.toMillis(time);
        unit = TimeUnit.MILLISECONDS;
        int priority = addThread(currentThread);
        try {
            do {
                long now = System.currentTimeMillis();
                try {
                    if (lock.tryLock(time, unit)) {
                        Integer nextHigherPriority = computeNextHigherPriorityIndex(priority);
                        if (nextHigherPriority != null) {
                            lock.unlock();
                        } else {
                            originalPriority = priority;
                            return true;
                        }
                    }
                } catch (InterruptedException e) {
                    removeThread(priority);
                    throw e;
                }
                long delta = System.currentTimeMillis() - now;
                time -= delta;
            } while (time > 0);
            removeThread(priority);
            return false;
        } catch (RuntimeException | Error e) {
            cleanup(priority, e);
            throw e;
        }
    }

    @Override
    public void unlock() {
        Condition condition = removeThread(originalPriority);
        // signal all threads waiting on this originalPriority to wake up so that each of them calls computeNextHigherPriority 
        // if we called condition.signal() it would only wake up one thread, and the others would be waiting for the higher priority thread to finish,
        // which will never happen if this thread is the last of the higher priority ones.
        condition.signalAll();
        lock.unlock();
    }

    @Override
    public @Nonnull Condition newCondition() {
        return lock.newCondition();
    }
    
    @Override
    public String toString() {
        return lock.toString();
    }

    private int addThread(Thread currentThread) {
        int index = currentThread.getPriority() - 1;
        levels[index].count.incrementAndGet();
        return currentThread.getPriority();
    }

    private void waitForHigherPriorityTasksToFinish(Thread currentThread) throws InterruptedException {
        int priority = currentThread.getPriority();
        while (true) {
            Integer nextHigherPriorityIndex = computeNextHigherPriorityIndex(priority);
            if (nextHigherPriorityIndex == null) {
                break;
            }
            levels[nextHigherPriorityIndex].condition.await();
        }
    }

    private Integer computeNextHigherPriorityIndex(int priority) {
        for (int i = priority; i < Thread.MAX_PRIORITY; i++) {
            if (levels[i].count.get() > 0) {
                return i;
            }
        }
        return null;
    }

    private void cleanup(int priority, Throwable e) {
        Condition condition = removeThread(priority);
        try {
            condition.signalAll();
        } catch (RuntimeException | Error e2) {
            e.addSuppressed(e2);
        }
    }
    
    private @Nonnull Condition removeThread(int priority) {
        int index = priority - 1;
        levels[index].count.decrementAndGet();
        return levels[index].condition;
    }
}
