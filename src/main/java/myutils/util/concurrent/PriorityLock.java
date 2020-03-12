package myutils.util.concurrent;

import java.util.Date;
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
     * Helper class to manage a list of levels.  Used by both the PriorityLock and its Condition objects.
     */
    private static class LevelManager {
        private final AtomicInteger[] counts;
        private final Condition[] conditions;

        private LevelManager(Lock internalLock) {
            this.counts = new AtomicInteger[Thread.MAX_PRIORITY];
            this.conditions = new Condition[Thread.MAX_PRIORITY];
            for (int i = 0; i < Thread.MAX_PRIORITY; i++) {
                this.counts[i] = new AtomicInteger();
                conditions[i] = internalLock.newCondition();
            }
        }
        
        /**
         * Add this thread to the tree.
         * 
         * @param currentThread always same as Thread.currentThread()
         * @return the current thread's priority
         */
        private int addThread(Thread currentThread) {
            int index = currentThread.getPriority() - 1;
            counts[index].incrementAndGet();
            return currentThread.getPriority();
        }

        /**
         * Remove the thread with priority as 'originalPriority' from the tree.
         * Signal all threads waiting on 'originalPriority' to wake up so that each of them calls computeNextHigherPriority.
         * Calls unlock.
         *
         * <ul>
         *   <li>Precondition: The internalLock is held by the current thread.</li>
         *   <li>Postcondition: The internalLock is still held by the current thread.</li>
         * </ul>
         * 
         * @param originalPriority the priority of the thread when addThread was called,
         *                         as other threads are awaiting for this priority's condition object to be signaled
         * @param threadLockDetails not null when called from PriorityLock, null when called from PriorityLockCondition                         
         */
        private void removeThreadAndSignal(int originalPriority, @Nullable ThreadLockDetails threadLockDetails) {
            removeThreadOnly(originalPriority);
            if (threadLockDetails != null) {
                threadLockDetails.clear();
            }
            // if we called condition.signal() it would only wake up one thread, and the others would be waiting for the higher priority thread to finish,
            // which will never happen if this thread is the last of the higher priority ones.
            conditions[originalPriority - 1].signalAll();
        }

        /**
         * Remove the thread with priority as 'originalPriority' from the tree.
         * Does not signal all threads waiting on 'originalPriority' to wake up so that each of them calls computeNextHigherPriority.
         * Does not call unlock.
         *
         * <ul>
         *   <li>Precondition: The internalLock is held by the current thread.</li>
         *   <li>Postcondition: The internalLock is still held by the current thread.</li>
         * </ul>
         * 
         * @param originalPriority the priority of the thread when addThread was called,
         *                         as other threads are awaiting for this priority's condition object to be signaled
         */
        private void removeThreadOnly(int originalPriority) {
            int index = originalPriority - 1;
            counts[index].decrementAndGet();
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
         * @throws InterruptedException if this thread is interrupted
         */
        private void waitForHigherPriorityTasksToFinish(PriorityLock priorityLock) throws InterruptedException {
            int priority = Thread.currentThread().getPriority();
            while (true) {
                Integer nextHigherPriorityIndex = computeNextHigherPriorityIndex(priority);
                if (nextHigherPriorityIndex == null) {
                    return;
                }
                try {
                    conditions[nextHigherPriorityIndex].await();
                } catch (InterruptedException | RuntimeException | Error e) {
                    priorityLock.internalLock.unlock();
                    throw e;
                }
            }
        }

        private void waitUninterruptiblyForHigherPriorityTasksToFinish(PriorityLock priorityLock) {
            int priority = Thread.currentThread().getPriority();
            while (true) {
                Integer nextHigherPriorityIndex = computeNextHigherPriorityIndex(priority);
                if (nextHigherPriorityIndex == null) {
                    return;
                }
                try {
                    conditions[nextHigherPriorityIndex].awaitUninterruptibly();
                } catch (RuntimeException | Error e) {
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
            setAll(originalPriority, 1);
        }

        private void setAll(int originalPriority, int holdCount) {
            this.originalPriority = originalPriority;
            this.threadId = Thread.currentThread().getId();
            this.holdCount = holdCount;
        }

        private void clear() {
            this.originalPriority = 0;
            this.threadId = 0;
            this.holdCount = 0;
        }

        /**
         * Tell if this thread has already been locked by a call to priorityLock.lock, etc.
         * If this thread has been locked as a result of await returning, this function returns false
         * even though internalLock.isHeldByCurrentThread() is true.
         */
        public boolean isExplicitlyLockedByCurrentThread() {
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
        this.levelManager = new LevelManager(internalLock);
    }

    private void lockAfterAwait(int holdCount) {
        int priority = levelManager.addThread(Thread.currentThread());
        threadLockDetails.setAll(priority, holdCount);
        levelManager.waitUninterruptiblyForHigherPriorityTasksToFinish(this);
    }

    @Override
    public void lock() {
        boolean alreadyLocked = threadLockDetails.isExplicitlyLockedByCurrentThread();
        if (alreadyLocked) {
            threadLockDetails.incrementHoldCount();
            return;
        }
        int priority = levelManager.addThread(Thread.currentThread());
        try {
            internalLock.lock();
            levelManager.waitUninterruptiblyForHigherPriorityTasksToFinish(this);
        } catch (RuntimeException | Error e) {
            levelManager.removeThreadOnly(priority);
            throw e;
        }
        threadLockDetails.setAll(priority);
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        boolean alreadyLocked = threadLockDetails.isExplicitlyLockedByCurrentThread();
        if (alreadyLocked) {
            threadLockDetails.incrementHoldCount();
            return;
        }
        int priority = levelManager.addThread(Thread.currentThread());
        try {
            internalLock.lockInterruptibly();
            levelManager.waitForHigherPriorityTasksToFinish(this);
        } catch (InterruptedException | RuntimeException | Error e) {
            levelManager.removeThreadOnly(priority);
            throw e;
        }
        threadLockDetails.setAll(priority);
    }

    @Override
    public boolean tryLock() {
        boolean alreadyLocked = threadLockDetails.isExplicitlyLockedByCurrentThread();
        if (alreadyLocked) {
            threadLockDetails.incrementHoldCount();
            return true;
        }
        int priority = levelManager.addThread(Thread.currentThread());
        try {
            final boolean acquired;
            Integer nextHigherPriority = levelManager.computeNextHigherPriorityIndex(priority);
            if (nextHigherPriority != null) {
                acquired = false;
                levelManager.removeThreadOnly(priority);
            } else {
                acquired = internalLock.tryLock();
                if (acquired) {
                    threadLockDetails.setAll(priority);
                } else {
                    levelManager.removeThreadOnly(priority);
                }
            }
            return acquired;
        } catch (RuntimeException | Error e) {
            levelManager.removeThreadOnly(priority);
            throw e;
        }
    }

    @Override
    public boolean tryLock(long time, @Nonnull TimeUnit unit) throws InterruptedException {
        boolean alreadyLocked = threadLockDetails.isExplicitlyLockedByCurrentThread();
        if (alreadyLocked) {
            threadLockDetails.incrementHoldCount();
            return true;
        }
        time = unit.toMillis(time);
        unit = TimeUnit.MILLISECONDS;
        int priority = levelManager.addThread(Thread.currentThread());
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
                    levelManager.removeThreadOnly(priority);
                    throw e;
                }
                long delta = System.currentTimeMillis() - now;
                time -= delta;
            } while (time > 0);
            levelManager.removeThreadOnly(priority);
            return false;
        } catch (RuntimeException | Error e) {
            levelManager.removeThreadOnly(priority);
            throw e;
        }
    }

    @Override
    public void unlock() {
        if (threadLockDetails.decrementHoldCount() == 0) {
            levelManager.removeThreadAndSignal(threadLockDetails.originalPriority, threadLockDetails);
            internalLock.unlock();
        }
    }

    @Override
    public @Nonnull Condition newCondition() {
        return new PriorityLockCondition();
    }
    
    @Override
    public String toString() {
        return internalLock.toString() + levelManager.toString();
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
            this.levelManager = new LevelManager(PriorityLock.this.internalLock);
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         *
         * <p>This implementation first signals threads waiting on this thread to wake up and proceed,
         * as a lower priority thread may signal the condition.
         */
        @Override
        public void await() throws InterruptedException {
            int holdCount = PriorityLock.this.threadLockDetails.holdCount;
            PriorityLock.this.levelManager.removeThreadAndSignal(PriorityLock.this.threadLockDetails.originalPriority, PriorityLock.this.threadLockDetails);
            int priority = levelManager.addThread(Thread.currentThread());
            try {
                InterruptedException interruptedException = null;
                try {
                    internalCondition.await();
                    levelManager.waitForHigherPriorityTasksToFinish(PriorityLock.this);
                } catch (InterruptedException e) {
                    interruptedException = e;
                    levelManager.waitUninterruptiblyForHigherPriorityTasksToFinish(PriorityLock.this);
                }
                if (interruptedException != null) {
                    throw interruptedException;
                }
            } finally {
                levelManager.removeThreadAndSignal(priority, null);
                PriorityLock.this.lockAfterAwait(holdCount);
            }
        }

        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            long now = System.currentTimeMillis();
            int holdCount = PriorityLock.this.threadLockDetails.holdCount;
            PriorityLock.this.levelManager.removeThreadAndSignal(PriorityLock.this.threadLockDetails.originalPriority, PriorityLock.this.threadLockDetails);
            int priority = levelManager.addThread(Thread.currentThread());
            try {
                boolean acquired = false;
                InterruptedException interruptedException = null;
                try {
                    acquired = internalCondition.await(time, unit);
                    levelManager.waitForHigherPriorityTasksToFinish(PriorityLock.this);
                } catch (InterruptedException e) {
                    interruptedException = e;
                    levelManager.waitUninterruptiblyForHigherPriorityTasksToFinish(PriorityLock.this);
                }
                if (interruptedException != null) {
                    throw interruptedException;
                }
                if (acquired && System.currentTimeMillis() > now + unit.toMillis(time)) {
                    acquired = false;
                }
                return acquired;
            } finally {
                levelManager.removeThreadAndSignal(priority, null);
                PriorityLock.this.lockAfterAwait(holdCount);
            }
        }

        @Override
        public void awaitUninterruptibly() {
            int holdCount = PriorityLock.this.threadLockDetails.holdCount;
            PriorityLock.this.levelManager.removeThreadAndSignal(PriorityLock.this.threadLockDetails.originalPriority, PriorityLock.this.threadLockDetails);
            int priority = levelManager.addThread(Thread.currentThread());
            try {
                internalCondition.awaitUninterruptibly();
                levelManager.waitUninterruptiblyForHigherPriorityTasksToFinish(PriorityLock.this);
            } finally {
                levelManager.removeThreadAndSignal(priority, null);
                PriorityLock.this.lockAfterAwait(holdCount);
            }
        }

        @Override
        public long awaitNanos(long nanosTimeout) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            long now = System.currentTimeMillis();
            long waitTimeMillis = deadline.getTime() - now;
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
            return internalCondition.toString() + levelManager.toString();
        }
    }
}
