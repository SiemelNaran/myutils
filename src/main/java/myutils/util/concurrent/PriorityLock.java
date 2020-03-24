package myutils.util.concurrent;

import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;
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
 * <p>An alternate design is to use PriorityExecutorService, which is an executor service that runs higher priority items first.
 * 
 * @see Thread#MIN_PRIORITY
 * @see Thread#NORM_PRIORITY
 * @see Thread#MAX_PRIORITY
 * @see myutils.util.concurrent.MoreExecutors#newFixedPriorityThreadPool
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
            int priority = currentThread.getPriority();
            addThread(priority);
            return priority;
        }

        private void addThread(int priority) {
            int index = priority - 1;
            counts[index].incrementAndGet();
        }

        /**
         * Remove the thread with priority as 'originalPriority' from the tree.
         * Signal all threads waiting on 'originalPriority' to wake up so that each of them calls computeNextHigherPriority.
         * Does not call unlock.
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
            // if we called condition.signal() it would only wake up one thread, and the others would be waiting for the higher priority thread to finish,
            // which will never happen if this thread is the last of the higher priority ones.
            conditions[originalPriority - 1].signalAll();
            removeThreadOnly(originalPriority);
            if (threadLockDetails != null) {
                threadLockDetails.clear();
            }
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
            counts[originalPriority - 1].decrementAndGet();
        }

        /**
         * Wait for tasks with priority higher that currentThread to finish.
         * 
         * <p>This functions finds the smallest priority higher than this one and awaits on that priority's condition. 
         * 
         * <p>Precondition: when this function is called the internal lock is locked by this thread.
         * Postcondition:
         *   (1) upon successful exit from this function this thread still holds the internal lock,
         *   (2) upon exceptional exit from this function this thread holds the lock if parameter priorityLock is not null.
         * 
         * @param priorityLock upon exceptional exit unlock PriorityLock- non null when called from PriorityLock, null when called from PriorityLockCondition
         * @return true if this function actually waited
         */
        private void waitForHigherPriorityTasksToFinish(PriorityLock priorityLock) throws InterruptedException {
            int priority = Thread.currentThread().getPriority();
            while (true) {
                Integer nextHigherPriorityIndex = computeNextHigherPriorityIndex(priority);
                if (nextHigherPriorityIndex == null) {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException();
                    }
                    return;
                }
                try {
                    conditions[nextHigherPriorityIndex].await();
                    return;
                } catch (InterruptedException | RuntimeException | Error e) {
                    if (priorityLock != null) {
                        priorityLock.internalLock.unlock();
                    }
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
                    if (priorityLock != null) {
                        priorityLock.internalLock.unlock();
                    }
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

        private int computeHighestPriority() {
            for (int i = Thread.MAX_PRIORITY - 1; i > 0; i--) {
                if (counts[i].get() > 0) {
                    return i + 1;
                }
            }
            return 0;
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
    
    
    private final Supplier<Lock> lockCreator;
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
        this(() -> new ReentrantLock(fair));
    }
    
    /**
     * Create a priority lock backed by the given lock.
     * Implementations must be sure that no two PriorityLock's share the same internal lock.
     * 
     * @param internalLock the internal lock
     */
    protected PriorityLock(Supplier<Lock> internalLockCreator) {
        this.lockCreator = internalLockCreator;
        this.internalLock = internalLockCreator.get();
        this.levelManager = new LevelManager(internalLock);
    }

    private void lockAfterAwait(int holdCount) throws InterruptedException {
        int priority = levelManager.addThread(Thread.currentThread());
        try {
            internalLock.lock();
            levelManager.waitUninterruptiblyForHigherPriorityTasksToFinish(this);
        } catch (RuntimeException | Error e) {
            levelManager.removeThreadOnly(priority);
            Error exception = new FailedToReacquireLockError();
            exception.addSuppressed(e);
            throw exception;
        }
        threadLockDetails.setAll(priority, holdCount);
        if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedException();
        }
    }

    private void lockAfterAwaitWithoutInterrupt(int holdCount) {
        int priority = levelManager.addThread(Thread.currentThread());
        try {
            internalLock.lock();
            levelManager.waitUninterruptiblyForHigherPriorityTasksToFinish(this);
        } catch (RuntimeException | Error e) {
            levelManager.removeThreadOnly(priority);
            Error exception = new FailedToReacquireLockError();
            exception.addSuppressed(e);
            throw exception;
        }
        threadLockDetails.setAll(priority, holdCount);
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
    
    private int unlockForAwait() {
        int holdCount = threadLockDetails.holdCount;
        levelManager.removeThreadAndSignal(threadLockDetails.originalPriority, threadLockDetails);
        internalLock.unlock();
        return holdCount;
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
     * 
     * <ul>Here is how this class implements it:
     *   <li>This class has an internal ReentrantLock and condition.  This ReentrantLock is different from the one in the PriorityLock.
     *   <li>When a thread of priority N calls await, we
     *           unlock the lock
     *           and make a note that a thread of priority N is waiting for a signal by incrementing the counter for this priority,
     *           and instead await on the internal condition object.</li>
     *   <li>When a thread returns from await, we check if there are any threads with higher priority in the queue (simply by checking if the count > 0</li>
     *   <li>If there is, then we await on that priority's condition</li>
     *   <li>If there are none, then we re-acquire the lock</li>.
     * </ul>
     */
    private class PriorityLockCondition implements Condition {
        private final Lock awaitLock;
        private final Condition awaitCondition;
        private final LevelManager levelManager;
        private int highestThread;
        
        private PriorityLockCondition() {
            this.awaitLock = PriorityLock.this.lockCreator.get();
            this.awaitCondition = awaitLock.newCondition();
            this.levelManager = new LevelManager(awaitLock);
        }

        private void genericAwait(InterruptedExceptionRunnable waitFunction) throws InterruptedException {
            boolean isLocked = false;
            int holdCount = PriorityLock.this.unlockForAwait();
            try {
                int priority = PriorityLockCondition.this.levelManager.addThread(Thread.currentThread());
                awaitLock.lock();
                try {
                    waitFunction.run();
                    levelManager.waitForHigherPriorityTasksToFinish(null);
                } finally {
                    levelManager.removeThreadAndSignal(priority, null);
                    try {
                        PriorityLock.this.lockAfterAwait(holdCount);
                        isLocked = true;
                    } finally {
                        if (priority == highestThread) {
                            PriorityLock.this.levelManager.removeThreadOnly(highestThread);
                            highestThread = 0;
                        }
                        awaitLock.unlock();
                    }
                }
            } catch (RuntimeException | Error e) {
                if (!isLocked && !(e instanceof ThreadDeath)) {
                    if (highestThread > 0) {
                        PriorityLock.this.levelManager.removeThreadOnly(highestThread);
                        highestThread = 0;
                    }
                    PriorityLock.this.lockAfterAwaitWithoutInterrupt(holdCount);
                }
                throw e;
            }
        }

        private void genericAwaitWithoutInterrupt() {
            boolean isLocked = false;
            int holdCount = PriorityLock.this.unlockForAwait();
            try {
                int priority = PriorityLockCondition.this.levelManager.addThread(Thread.currentThread());
                awaitLock.lock();
                try {
                    awaitCondition.awaitUninterruptibly();
                    levelManager.waitUninterruptiblyForHigherPriorityTasksToFinish(null);
                } finally {
                    levelManager.removeThreadAndSignal(priority, null);
                    try {
                        PriorityLock.this.lockAfterAwaitWithoutInterrupt(holdCount);
                        isLocked = true;
                    } finally {
                        if (priority == highestThread) {
                            PriorityLock.this.levelManager.removeThreadOnly(highestThread);
                            highestThread = 0;
                        }
                        awaitLock.unlock();
                    }
                }
            } catch (RuntimeException | Error e) {
                if (!isLocked && !(e instanceof ThreadDeath)) {
                    if (highestThread > 0) {
                        PriorityLock.this.levelManager.removeThreadOnly(highestThread);
                        highestThread = 0;
                    }
                    PriorityLock.this.lockAfterAwaitWithoutInterrupt(holdCount);
                }
                throw e;
            }
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         * 
         * @throws FailedToReacquireLockError if the function failed to re-acquire the PriorityLock.
         */
        @Override
        public void await() throws InterruptedException {
            genericAwait(() -> awaitCondition.await());
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         * 
         * @throws FailedToReacquireLockError if the function failed to re-acquire the PriorityLock.
         */
        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            long startTime = System.currentTimeMillis();
            genericAwait(() -> awaitCondition.await(time, unit));
            return System.currentTimeMillis() < startTime + unit.toMillis(time);
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         * 
         * @throws FailedToReacquireLockError if the function failed to re-acquire the PriorityLock.
         */
        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            long startTime = System.nanoTime();
            genericAwait(() -> awaitCondition.awaitNanos(nanosTimeout));
            long timeLeft = nanosTimeout - (System.nanoTime() - startTime);
            return timeLeft;
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         * 
         * @throws FailedToReacquireLockError if the function failed to re-acquire the PriorityLock.
         */
        @Override
        public void awaitUninterruptibly() {
            genericAwaitWithoutInterrupt();
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         * 
         * @throws FailedToReacquireLockError if the function failed to re-acquire the PriorityLock.
         */
        @Override
        public boolean awaitUntil(Date deadline) throws InterruptedException {
            long now = System.currentTimeMillis();
            long waitTimeMillis = deadline.getTime() - now;
            return await(waitTimeMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public void signal() {
            // call signal in order to wake up all threads so that the highest priority thread can run
            signalAll();
        }

        @Override
        public void signalAll() {
            awaitLock.lock();
            try {
                awaitCondition.signalAll();
                int ht = levelManager.computeHighestPriority();
                if (ht > highestThread) {
                    if (highestThread != 0) {
                        PriorityLock.this.levelManager.removeThreadOnly(highestThread);
                    }
                    PriorityLock.this.levelManager.addThread(ht);
                    highestThread = ht;
                }
            } finally {
                awaitLock.unlock();
            }
        }
        
        @Override
        public String toString() {
            return awaitCondition.toString() + levelManager.toString();
        }
    }
    
    
    private interface InterruptedExceptionRunnable {
        void run() throws InterruptedException;
    }
    

    /**
     * Exception thrown when the await function is unable to re-acquire the lock.
     */
    public static class FailedToReacquireLockError extends ThreadDeath {
        private static final long serialVersionUID = 1L;
        
        private FailedToReacquireLockError() {            
        }
    }
}
