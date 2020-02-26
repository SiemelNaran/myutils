package myutils.util.concurrent;

import java.util.Date;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


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
    
    /**
     * Helper class to remove waiting thread's priority from the tree so that threads that depend on this priority can proceed.
     * Reason for using a helper class along with a BlockingQueue is because removing a priority may be a long running operation
     * as we have to acquire a lock on the internalLock in order to signal a condition object based on the internalLock.
     */
    private static class RemovePriorityAction {
        private static final ExecutorService EXECUTOR = Executors.newSingleThreadExecutor(runnable -> new Thread(runnable, "RemovePriorityActionThread"));
        private static final LinkedBlockingQueue<RemovePriorityAction> REMOVE_THREAD_QUEUE = new LinkedBlockingQueue<RemovePriorityAction>();
        
        private final PriorityLock priorityLock;
        private final Level level;
        
        private RemovePriorityAction(PriorityLock priorityLock, Level level) {
            this.priorityLock = priorityLock;
            this.level = level;
        }
        
        static {
            RemovePriorityAction.EXECUTOR.submit(() -> {
                while (true) {
                    RemovePriorityAction action = REMOVE_THREAD_QUEUE.take();
                    action.priorityLock.cleanupNow(action.level); 
                }
             });
        }
    }

    /**
     * Helper POJO class that stores the number of threads waiting in each priority
     * along with a Condition object for each priority.
     */
    private static class Level {
        private final AtomicInteger count = new AtomicInteger();
        private final Condition condition;

        private Level(Lock internalLock) {
            condition = internalLock.newCondition();
        }
    }
    
    
    private final Lock internalLock;
    private final Level[] levels;
    private int originalPriority; // to save the original priority of the thread in case user changes it while running the thread
    
    
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
     * @param internalLock the internal lock
     */
    protected PriorityLock(Lock internalLock) {
        this.internalLock = internalLock;
        this.levels = new Level[Thread.MAX_PRIORITY];
        for (int i = 0; i < Thread.MAX_PRIORITY; i++) {
            this.levels[i] = new Level(internalLock);
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
        } catch (InterruptedException | RuntimeException | Error e) {
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
                    cleanup(priority, null);
                } else {
                    originalPriority = priority;
                }
            } else {
                cleanup(priority, null);
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
                    cleanup(priority, null);
                    throw e;
                }
                long delta = System.currentTimeMillis() - now;
                time -= delta;
            } while (time > 0);
            cleanup(priority, null);
            return false;
        } catch (RuntimeException | Error e) {
            cleanup(priority, e);
            throw e;
        }
    }

    @Override
    public void unlock() {
        cleanupFast(originalPriority);
        internalLock.unlock();
    }
    
    /**
     * Signal all threads waiting on this originalPriority to wake up so that each of them calls computeNextHigherPriority 
     * if we called condition.signal() it would only wake up one thread, and the others would be waiting for the higher priority thread to finish,
     * which will never happen if this thread is the last of the higher priority ones.
     * 
     * <p>Precondition: The internalLock is held by the current thread.
     */
    private void cleanupFast(int priority) {
        int index = priority - 1;
        levels[index].count.decrementAndGet();
        Condition condition = levels[index].condition;
        condition.signalAll();
    }
    
    /**
     * A variation of cleanupFast that does not assume that the current thread holds the internal lock.
     * This implementation calls tryLock to acquire the internal lock quickly, followed by decrement and signalAll.
     * If acquring the lock fails then add the cleanup operation to a queue so that the RemovePriorityActionThread can perform the cleanup.
     */
    private void cleanup(int priority, @Nullable Throwable e) {
        int index = priority - 1;
        Level level = levels[index]; 
        Condition condition = level.condition;
        try {
            if (internalLock.tryLock()) {
                try {
                    level.count.decrementAndGet();
                    condition.signalAll();
                } finally {
                    internalLock.unlock();
                }
            } else {
                RemovePriorityAction.REMOVE_THREAD_QUEUE.add(new RemovePriorityAction(this, level));
            }
        } catch (RuntimeException | Error e2) { 
            if (e != null) {
                e.addSuppressed(e2);
            }
        }
    }
    
    /**
     * A variation of cleanupFast that is intended to be called by the queue.
     * This implementation calls lock to acquire the internal lock, followed by decrement and signalAll.
     */
    private void cleanupNow(Level level) {
        level.count.decrementAndGet();
        internalLock.lock();
        try {
            level.count.decrementAndGet();
            level.condition.signalAll();
        } catch (RuntimeException | Error e2) {
            e2.printStackTrace();
        } finally {
            internalLock.unlock();
        }
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

    
    private class PriorityLockCondition implements Condition {
        private final Condition internalCondition;
        
        private PriorityLockCondition() {
            this.internalCondition = PriorityLock.this.internalLock.newCondition();
        }
        
        @Override
        public void await() throws InterruptedException {
            PriorityLock.this.signal();
            internalCondition.await();
            PriorityLock.this.internalLock.unlock();
            PriorityLock.this.lockInterruptibly();
        }

        @Override
        public void awaitUninterruptibly() {
            PriorityLock.this.signal();
            internalCondition.awaitUninterruptibly();
            PriorityLock.this.internalLock.unlock();
            PriorityLock.this.lock();
        }

        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            Thread currentThread = Thread.currentThread();
            time = unit.toMillis(time);
            unit = TimeUnit.MILLISECONDS;
            PriorityLock.this.signal();
            int priority = currentThread.getPriority();
            do {
                long now = System.currentTimeMillis();
                try {
                    if (internalCondition.await(time, unit)) {
                        return true;
                    } else {
                        Integer nextHigherPriority = computeNextHigherPriorityIndex(priority);
                        if (nextHigherPriority == null) {
                            addThread(currentThread);
                            PriorityLock.this.originalPriority = priority;
                            return false;
                        }
                    }
                } catch (InterruptedException e) {
                    throw e;
                }
                long delta = System.currentTimeMillis() - now;
                time -= delta;
            } while (time > 0);
            removeThread(priority);
            return false;
        }

        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public void signal() {
            internalCondition.signalAll();
        }

        @Override
        public void signalAll() {
            internalCondition.signalAll();
        }
        
        @Override
        public String toString() {
            return internalCondition.toString();
        }
    }
}
