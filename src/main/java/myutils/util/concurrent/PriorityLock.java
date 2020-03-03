package myutils.util.concurrent;

import java.util.Date;
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
        private static final LinkedBlockingQueue<RemovePriorityAction> REMOVE_THREAD_QUEUE = new LinkedBlockingQueue<>();
        private static volatile boolean SHUTDOWN = false;
        
        private final PriorityLock priorityLock;
        private final int priority;
        
        private RemovePriorityAction(PriorityLock priorityLock, int priority) {
            this.priorityLock = priorityLock;
            this.priority = priority;
        }
        
        static {
            RemovePriorityAction.EXECUTOR.submit(() -> {
                while (!SHUTDOWN) {
                    try {
                        RemovePriorityAction action = REMOVE_THREAD_QUEUE.take();
                        action.priorityLock.cleanupNow(action.priority);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
             });

            Runtime.getRuntime().addShutdownHook(new Thread(() -> SHUTDOWN = true));
        }
    }

    
    /**
     * Helper class to manage a list of levels.  Used by both the PriorityLock and its Condition objects.
     * 
     * <p>The key functions in this class are
     * waitForHigherPriorityTasksToFinish (to wait for tasks with a higher priority to finish), and
     * cleanupImmediately (to remove this thread from the execution tree and signal threads waiting on this priority).
     * 
     * <p>The actual conditions are owned by the PriorityLock.
     */
    private static class LevelManager {
        private final AtomicInteger[] counts;
        private final Condition[] conditions;

        private LevelManager(Condition[] conditions) {
            this.counts = new AtomicInteger[Thread.MAX_PRIORITY];
            for (int i = 0; i < Thread.MAX_PRIORITY; i++) {
                this.counts[i] = new AtomicInteger();
            }
            this.conditions = conditions;
        }
        
        private int addThread(Thread currentThread) {
            int index = currentThread.getPriority() - 1;
            counts[index].incrementAndGet();
            return currentThread.getPriority();
        }
        
        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(32);
            builder.append('[');
            for (int i = 0; i < Thread.MAX_PRIORITY; i++) {
                builder.append(counts[i].get()).append(',');
            }
            builder.deleteCharAt(builder.length() - 1);
            builder.append(']');
            return builder.toString();
        }

        /**
         * Signal all threads waiting on this originalPriority to wake up so that each of them calls computeNextHigherPriority.
         * Does not call unlock.
         *
         * <p>Precondition: The internalLock is held by the current thread.
         * <p>Postcondition: The internalLock is held by the current thread.
         */
        private void cleanupImmediately(int originalPriority) {
            int index = originalPriority - 1;
            counts[index].decrementAndGet();
            // if we called condition.signal() it would only wake up one thread, and the others would be waiting for the higher priority thread to finish,
            // which will never happen if this thread is the last of the higher priority ones.
            conditions[index].signalAll();
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
         * @return true if this function actually waited
         * @throws InterruptedException if this thread is interrupted
         */
        private boolean waitForHigherPriorityTasksToFinish(PriorityLock priorityLock) throws InterruptedException {
            int priority = Thread.currentThread().getPriority();
            while (true) {
                Integer nextHigherPriorityIndex = computeNextHigherPriorityIndex(priority);
                if (nextHigherPriorityIndex == null) {
                    return false;
                }
                try {
                    conditions[nextHigherPriorityIndex].await();
                    return true;
                } catch (InterruptedException | RuntimeException | Error e) {
                    priorityLock.internalLock.unlock();
                    throw e;
                }
            }
        }

        private Integer computeNextHigherPriorityIndex(int priority) {
            for (int i = priority; i < Thread.MAX_PRIORITY; i++) {
                if (counts[i].get() > 0) {
                    return i;
                }
            }
            return null;
        }
    }
    
    
    private static class ThreadLockDetails {
        private int originalPriority; // to save the original priority of the thread in case user changes it while running the thread
        private long threadId;
        private int holdCount;
        
        /**
         * Set the member variables of the class.
         * Should only be called after the internal lock is locked.
         */
        private void setAll(int originalPriority) {
            this.originalPriority = originalPriority;
            this.threadId = Thread.currentThread().getId();
            this.holdCount = 1;
        }
        
        private void clear() {
            this.originalPriority = 0;
            this.threadId = 0;
            this.holdCount = 0;
        }

        public boolean isLockedByCurrentThread() {
            return Thread.currentThread().getId() == threadId;
        }

        public void incrementHoldCount() {
            ++holdCount;
        }

        public int decrementHoldCount() {
            return --holdCount;
        }
    }
    
    
    private final Lock internalLock;
    private final Condition[] priorityLockConditions;
    private final LevelManager levelManager;
    private final ThreadLockDetails threadLockDetails = new ThreadLockDetails();
    
    
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
        this.priorityLockConditions = new Condition[Thread.MAX_PRIORITY];
        for (int i = 0; i < Thread.MAX_PRIORITY; i++) {
            this.priorityLockConditions[i] = internalLock.newCondition();
        }
        this.levelManager = new LevelManager(priorityLockConditions);
    }

    @Override
    public void lock() {
        boolean alreadyLocked = threadLockDetails.isLockedByCurrentThread();
        if (alreadyLocked) {
            threadLockDetails.incrementHoldCount();
            return;
        }
        boolean acquired = false;
        int priority = addThread(Thread.currentThread());
        try {
            internalLock.lock();
            try {
                acquired = true;
                levelManager.waitForHigherPriorityTasksToFinish(this);
            } catch (InterruptedException e) {
                throw new CancellationException(e.getMessage());
            }
        } catch (RuntimeException | Error e) {
            cleanupQuicklyOrQueueCleanupAction(acquired, priority, e);
            throw e;
        }
        threadLockDetails.setAll(priority);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        boolean alreadyLocked = threadLockDetails.isLockedByCurrentThread();
        if (alreadyLocked) {
            threadLockDetails.incrementHoldCount();
            return;
        }
        boolean acquired = false;
        int priority = addThread(Thread.currentThread());
        try {
            internalLock.lockInterruptibly();
            acquired = true;
            levelManager.waitForHigherPriorityTasksToFinish(this);
        } catch (InterruptedException | RuntimeException | Error e) {
            cleanupQuicklyOrQueueCleanupAction(acquired, priority, e);
            throw e;
        }
        threadLockDetails.setAll(priority);
    }

    @Override
    public boolean tryLock() {
        boolean alreadyLocked = threadLockDetails.isLockedByCurrentThread();
        if (alreadyLocked) {
            threadLockDetails.incrementHoldCount();
            return true;
        }
        int priority = addThread(Thread.currentThread());
        try {
            boolean acquired = internalLock.tryLock();
            if (acquired) {
                Integer nextHigherPriority = levelManager.computeNextHigherPriorityIndex(priority);
                if (nextHigherPriority != null) {
                    internalLock.unlock();
                    acquired = false;
                    cleanupQuicklyOrQueueCleanupAction(true, priority, null);
                } else {
                    threadLockDetails.setAll(priority);
                }
            } else {
                cleanupQuicklyOrQueueCleanupAction(false, priority, null);
            }
            return acquired;
        } catch (RuntimeException | Error e) {
            cleanupQuicklyOrQueueCleanupAction(false, priority, e);
            throw e;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        boolean alreadyLocked = threadLockDetails.isLockedByCurrentThread();
        if (alreadyLocked) {
            threadLockDetails.incrementHoldCount();
            return true;
        }
        time = unit.toMillis(time);
        unit = TimeUnit.MILLISECONDS;
        int priority = addThread(Thread.currentThread());
        try {
            do {
                long now = System.currentTimeMillis();
                try {
                    if (internalLock.tryLock(time, unit)) {
                        Integer nextHigherPriority = levelManager.computeNextHigherPriorityIndex(priority);
                        if (nextHigherPriority != null) {
                            internalLock.unlock();
                        } else {
                            threadLockDetails.setAll(priority);
                            return true;
                        }
                    }
                } catch (InterruptedException e) {
                    cleanupQuicklyOrQueueCleanupAction(false, priority, null);
                    throw e;
                }
                long delta = System.currentTimeMillis() - now;
                time -= delta;
            } while (time > 0);
            cleanupQuicklyOrQueueCleanupAction(false, priority, null);
            return false;
        } catch (RuntimeException | Error e) {
            cleanupQuicklyOrQueueCleanupAction(false, priority, e);
            throw e;
        }
    }

    @Override
    public void unlock() {
        if (threadLockDetails.decrementHoldCount() == 0) {
            levelManager.cleanupImmediately(threadLockDetails.originalPriority);
            threadLockDetails.clear();
            internalLock.unlock();
        }
    }

    private int addThread(Thread currentThread) {
        return levelManager.addThread(currentThread);
    }

    /**
     * A variation of cleanupImmediately that does not assume that the current thread holds the internal lock.
     * This implementation calls tryLock to acquire the internal lock quickly, followed by decrement and signalAll.
     * If acquiring the lock fails then add the cleanup operation is pushed onto a queue so that the RemovePriorityActionThread can perform the cleanup.
     *
     * @param lockedByCallingFunction did the function calling this one lock the thread (used to determine whether to unlock the internal lock)
     * @param priority the priority to clean up. We cannot use originalPriority because it may not have been set (this function is called from catch blocks).
     * @param e the exception
     */
    private void cleanupQuicklyOrQueueCleanupAction(boolean lockedByCallingFunction, int priority, @Nullable Throwable e) {
        if (lockedByCallingFunction) {
            try {
                if (internalLock.tryLock()) {
                    try {
                        levelManager.cleanupImmediately(priority);
                    } finally {
                        internalLock.unlock();
                    }
                } else {
                    RemovePriorityAction.REMOVE_THREAD_QUEUE.add(new RemovePriorityAction(this, priority));
                }
            } catch (RuntimeException | Error e2) { 
                if (e != null) {
                    e.addSuppressed(e2);
                }
            }
        } else {
            levelManager.counts[priority - 1].decrementAndGet();
        }
    }
    
    /**
     * A variation of cleanupFast that is intended to be called by the queue.
     * This implementation calls lock to acquire the internal lock, followed by decrement and signalAll.
     */
    private void cleanupNow(int priority) {
        internalLock.lock();
        try {
            levelManager.cleanupImmediately(priority);
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
        StringBuilder builder = new StringBuilder(internalLock.toString());
        builder.append(levelManager.toString());
        return builder.toString();
    }

    /**
     * Implementation of Condition for PriorityLock that ensures this thread is the thread with the highest priority
     * when the await functions return.
     */
    private class PriorityLockCondition implements Condition {
        private final Condition internalCondition;
        private final LevelManager levelManager;
        
        private PriorityLockCondition() {
            this.internalCondition = PriorityLock.this.internalLock.newCondition();
            this.levelManager = new LevelManager(PriorityLock.this.priorityLockConditions);
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         *
         * <p>This implementation first signals threads waiting on this thread to wake up and proceed,
         * as a lower priority thread may signal the condition.
         */
        @Override
        public void await() throws InterruptedException {
            PriorityLock.this.levelManager.cleanupImmediately(PriorityLock.this.threadLockDetails.originalPriority);
            Thread currentThread = Thread.currentThread();
            int priority = levelManager.addThread(currentThread);
            internalCondition.await();
            try {
                while (true) {
                    boolean waited = levelManager.waitForHigherPriorityTasksToFinish(PriorityLock.this);
                    if (!waited) {
                        break;
                    }
                }
                levelManager.cleanupImmediately(priority);
            } catch (InterruptedException | RuntimeException | Error e) {
                levelManager.cleanupImmediately(priority);
            }
            try {
                PriorityLock.this.lockInterruptibly();
            } finally {
                PriorityLock.this.internalLock.unlock();
            }
        }


        @Override
        public void awaitUninterruptibly() {
            PriorityLock.this.levelManager.cleanupImmediately(PriorityLock.this.threadLockDetails.originalPriority);
            Thread currentThread = Thread.currentThread();
            int priority = levelManager.addThread(currentThread);
            internalCondition.awaitUninterruptibly();
            try {
                while (true) {
                    boolean waited = levelManager.waitForHigherPriorityTasksToFinish(PriorityLock.this);
                    if (!waited) {
                        break;
                    }
                }
                levelManager.cleanupImmediately(priority);
            } catch (InterruptedException | RuntimeException | Error e) {
                levelManager.cleanupImmediately(priority);
            }
            try {
                PriorityLock.this.lock();
            } finally {
                PriorityLock.this.internalLock.unlock();
            }
        }

        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            time = unit.toMillis(time);
            unit = TimeUnit.MILLISECONDS;
            PriorityLock.this.levelManager.cleanupImmediately(PriorityLock.this.threadLockDetails.originalPriority);
            long now = System.currentTimeMillis();
            if (!internalCondition.await(time, unit)) {
                long delta = System.currentTimeMillis() - now;
                if (PriorityLock.this.tryLock(delta, TimeUnit.MILLISECONDS)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            long now = System.currentTimeMillis();
            long waitTimeMillis = deadline.getTime() - now;
            if (waitTimeMillis < 0) {
                return true;
            }
            return await(waitTimeMillis, TimeUnit.MILLISECONDS);
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
            StringBuilder builder = new StringBuilder(internalCondition.toString());
            builder.append(levelManager.toString());
            return builder.toString();
        }
    }
}
