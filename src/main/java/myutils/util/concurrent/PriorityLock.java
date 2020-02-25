package myutils.util.concurrent;

import java.util.Date;
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

    private final Lock internalLock;
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
        this.internalLock = lock;
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
            internalLock.lock();
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
            internalLock.lockInterruptibly();
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
            boolean acquired = internalLock.tryLock();
            if (acquired) {
                Integer nextHigherPriority = computeNextHigherPriorityIndex(priority);
                if (nextHigherPriority != null) {
                    internalLock.unlock();
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
                    if (internalLock.tryLock(time, unit)) {
                        Integer nextHigherPriority = computeNextHigherPriorityIndex(priority);
                        if (nextHigherPriority != null) {
                            internalLock.unlock();
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
        internalLock.unlock();
    }

    @Override
    public @Nonnull Condition newCondition() {
        return new PriorityLockCondition();
    }
    
    @Override
    public String toString() {
        return internalLock.toString();
    }

    private int addThread(Thread currentThread) {
        int index = currentThread.getPriority() - 1;
        levels[index].count.incrementAndGet();
        return currentThread.getPriority();
    }

    /**
     * Wait for tasks with priority higher that currentThread to finish.
     * 
     * <p>This functions finds the smallest priority higher than this one and awaits on that priority's condition. 
     * 
     * <p>Precondition: when this function is called the internal lock is locked by this thread.
     * Postcondition:
     *   (1) upon successful exit from this function this thread still holds the internal lock,
     *   (2) upon exceptional exit from this function this thread does not hold the internal lock.
     * 
     * @return the current thread's priority
     * @throws InterruptedException
     */
    private void waitForHigherPriorityTasksToFinish(Thread currentThread) throws InterruptedException {
        int priority = currentThread.getPriority();
        while (true) {
            Integer nextHigherPriorityIndex = computeNextHigherPriorityIndex(priority);
            if (nextHigherPriorityIndex == null) {
                break;
            }
            try {
                levels[nextHigherPriorityIndex].condition.await();
            } catch (InterruptedException | RuntimeException | Error e) {
                internalLock.unlock();
                throw e;
            }
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
            internalLock.lock();
            try {
                condition.signalAll();
            } catch (RuntimeException | Error e2) {
                e.addSuppressed(e2);
            } finally {
                internalLock.unlock();
            }
        } catch (RuntimeException | Error e2) { 
            e.addSuppressed(e2);
        }
    }
    
    private @Nonnull Condition removeThread(int priority) {
        int index = priority - 1;
        levels[index].count.decrementAndGet();
        return levels[index].condition;
    }
    
    
    private class PriorityLockCondition implements Condition {
        private final Condition internalCondition;
        
        private PriorityLockCondition() {
            this.internalCondition = PriorityLock.this.internalLock.newCondition();
        }
        
        @Override
        public void await() throws InterruptedException {
            Condition condition = removeThread(originalPriority);
            condition.signalAll();
            internalCondition.await();
            PriorityLock.this.internalLock.unlock();
            PriorityLock.this.lockInterruptibly();
        }

        @Override
        public void awaitUninterruptibly() {
            // TODO Auto-generated method stub
        }

        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public void signal() {
            internalCondition.signalAll();
        }

        @Override
        public void signalAll() {
            internalCondition.signalAll();
        }
    }
}
