package myutils.util.concurrent;

import java.util.Date;
import java.util.Objects;
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
 *           and signal the condition object for this thread so that any threads waiting on this condition wake up and start running.</li>
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
        private void removeThreadAndSignal(int originalPriority, @Nonnull ThreadLockDetails threadLockDetails) {
            // if we called condition.signal() it would only wake up one thread, and the others would be waiting for the higher priority thread to finish,
            // which will never happen if this thread is the last of the higher priority ones.
            conditions[originalPriority - 1].signalAll();
            removeThreadOnly(originalPriority);
            threadLockDetails.clear();
        }

        private int removeThreadAndSignalHighest(int originalPriority, boolean existsThreadWaitingOnOriginalPriority) {
            removeThreadOnly(originalPriority);
            int priorityToLock = computeHighestPriority();
            int priorityToSignal = priorityToLock;
            if (existsThreadWaitingOnOriginalPriority) {
                priorityToSignal = Math.max(priorityToSignal, originalPriority);
            }
            if (priorityToSignal > 0) { // CodeCoverage: always true (as this function only called when signalCount > 0, implying that there is a thread to signal)
                conditions[priorityToSignal - 1].signalAll();
            }
            return priorityToLock;
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
        private void waitForHigherPriorityTasksToFinish(@Nonnull PriorityLock priorityLock) throws InterruptedException {
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
                } catch (InterruptedException | RuntimeException | Error e) {
                    priorityLock.internalLock.unlock();
                    throw e;
                }
            }
        }

        private void waitUninterruptiblyForHigherPriorityTasksToFinish(@Nullable PriorityLock priorityLock) {
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
        
        private void waitUninterruptiblyForHigherPriorityTasksToFinishFromAwait(int[] waitingOn) {
            int priority = Thread.currentThread().getPriority();
            while (true) {
                Integer nextHigherPriorityIndex = computeNextHigherPriorityIndex(priority);
                if (nextHigherPriorityIndex == null) {
                    return;
                }
                waitingOn[nextHigherPriorityIndex]++;
                try {
                    conditions[nextHigherPriorityIndex].awaitUninterruptibly();
                } finally {
                    waitingOn[nextHigherPriorityIndex]--;
                }
            }
        }
        
        private void waitInterruptiblyForHigherPriorityTasksToFinishFromAwait(int[] waitingOn) throws InterruptedException {
            int priority = Thread.currentThread().getPriority();
            while (true) {
                Integer nextHigherPriorityIndex = computeNextHigherPriorityIndex(priority);
                if (nextHigherPriorityIndex == null) {
                    return;
                }
                waitingOn[nextHigherPriorityIndex]++;
                try {
                    conditions[nextHigherPriorityIndex].await();
                } finally {
                    waitingOn[nextHigherPriorityIndex]--;
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
            for (int i = Thread.MAX_PRIORITY - 1; i >= 0; i--) {
                if (counts[i].get() > 0) {
                    return i + 1;
                }
            }
            return 0; // coverage: never hit
        }

        @Override
        public String toString() {
            StringBuilder builder = new StringBuilder(32);
            builder.append("levels=[");
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
    private final boolean allowEarlyInterruptFromAwait;
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
        this(new PriorityLockNamedParams().setInternalReentrantLockCreator(fair));
    }
    
    /**
     * Create a priority lock.
     */
    public PriorityLock(PriorityLockNamedParams params) {
        this.internalLock = Objects.requireNonNull(params.internalLockCreator.get());
        this.allowEarlyInterruptFromAwait = params.allowEarlyInterruptFromAwait;
        this.levelManager = new LevelManager(internalLock);
    }
    
    protected static class PriorityLockNamedParams {
        private static final Supplier<Lock> NONFAIR_REENTRANT_LOCK_CREATOR = () -> new ReentrantLock();
        private static final Supplier<Lock> FAIR_REENTRANT_LOCK_CREATOR = () -> new ReentrantLock(true);

        private Supplier<Lock> internalLockCreator = NONFAIR_REENTRANT_LOCK_CREATOR;
        private boolean allowEarlyInterruptFromAwait;
        
        public static PriorityLockNamedParams create() {
            return new PriorityLockNamedParams();
        }
        
        private PriorityLockNamedParams() {
        }
        
        /**
         * Set the internal lock creator to be a function that creates a ReentrantLock.
         * 
         * @param fair is this a fair lock
         */
        public PriorityLockNamedParams setInternalReentrantLockCreator(boolean fair) {
            return setInternalLockCreator(fair ? FAIR_REENTRANT_LOCK_CREATOR : NONFAIR_REENTRANT_LOCK_CREATOR);
        }
        
        /**
         * Set the internal lock creator to be a function that returns a custom lock.
         * Implementations must be sure that no two PriorityLock objects share the same internal lock.
         */
        public PriorityLockNamedParams setInternalLockCreator(@Nonnull Supplier<Lock> internalLockCreator) {
            this.internalLockCreator = internalLockCreator;
            return this;
        }
        
        /**
         * Set whether to allow await functions to throw InterruptedException upon receiving an InterruptedException.
         * For example, if a thread with a lower priority is waiting for a thread with much higher priority to finish,
         * and the thread with lower priority is interrupted,
         * then upon the thread with much higher priority getting unlocked, the thread with lower priority encounters an InterruptedException.
         * 
         * @param allowEarlyInterruptFromAwait false (the default) means wait for threads with higher priority to finish before throwing an InterruptedException<br/>
         *                                     true means throw InterruptedException right away
         */
        public PriorityLockNamedParams setAllowEarlyInterruptFromAwait(boolean allowEarlyInterruptFromAwait) {
            this.allowEarlyInterruptFromAwait = allowEarlyInterruptFromAwait;
            return this;
        }
    }

    /**
     * Locks this thread after a call to await.
     * After the call to internalCondition.await, internaLock is locked by this thread, so we cannot call internalLock.lock.
     *
     * @param wasSignalled true if the current thread was explicitly signaled by a call to condition.signal. 
     */
    private void lockUninterruptiblyAfterAwait(int holdCount, boolean wasSignalled) {
        int priority = wasSignalled ? Thread.currentThread().getPriority() : levelManager.addThread(Thread.currentThread());
        try {
            levelManager.waitUninterruptiblyForHigherPriorityTasksToFinish(null);
        } catch (RuntimeException | Error e) {
            Error exception = new MaybeNotHighestThreadAfterAwaitError();
            exception.addSuppressed(e);
            throw exception;
        } finally {
            threadLockDetails.setAll(priority, holdCount);
        }
    }

    private void lockInterruptiblyAfterAwait(int holdCount, boolean wasSignalled) throws InterruptedException {
        int priority = wasSignalled ? Thread.currentThread().getPriority() : levelManager.addThread(Thread.currentThread()); // CodeCoverage: only true branch hit
        try {
            levelManager.waitForHigherPriorityTasksToFinish(null);
        } catch (RuntimeException | Error e) {
            Error exception = new MaybeNotHighestThreadAfterAwaitError();
            exception.addSuppressed(e);
            throw exception;
        } finally {
            threadLockDetails.setAll(priority, holdCount);
        }
    }

    private void resetAfterAwait(int holdCount, boolean wasSignalled) throws InterruptedException {
        int priority = wasSignalled ? Thread.currentThread().getPriority() : levelManager.addThread(Thread.currentThread()); // CodeCoverage: only true branch hit
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
            Integer nextHigherPriorityIndex = levelManager.computeNextHigherPriorityIndex(priority);
            if (nextHigherPriorityIndex != null) {
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
                        Integer nextHigherPriorityIndex = levelManager.computeNextHigherPriorityIndex(priority);
                        if (nextHigherPriorityIndex != null) {
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
        return holdCount;
    }

    @Override
    public @Nonnull PriorityLockCondition newCondition() {
        return new PriorityLockCondition(allowEarlyInterruptFromAwait);
    }
    
    @Override
    public String toString() {
        return getClass().getName() + "@" + hashCode() + " " + levelManager.toString();
    }
    
    public Integer highestPriorityThread() {
        return nullIfZero(levelManager.computeHighestPriority());
    }

    /**
     * Implementation of Condition for PriorityLock that ensures this thread is the thread with the highest priority
     * when the await functions return.
     * 
     * <ul>Here is how this class implements it:
     *   <li>This class has 10 conditions based on the internal lock. These are different from the 10 conditions owned by the PriorityLock.</li>
     *   <li>When a thread of priority N calls await, we
     *           make a note that a thread of priority N is waiting for a signal by incrementing the counter for this priority, and
     *           await on the condition object of level N to be signaled (which unlocks the lock so that other threads can proceed).</li>
     *   <li>Calling signal signals the highest priority thread -- i.e. calls signalAll on the highest condition and increments signalCount by 1.</li>
     *   <li>Calling signalAll signals the highest priority thread --
     *           i.e. calls signalAll on the highest condition and increments signalCount by the number of threads in this condition object.</li>
     *   <li>When a thread returns from await, we check if there are any threads with higher priority in the queue (simply by checking if the count > 0</li>
     *   <li>If there is, then we await on that priority's condition.</li>
     *   <li>Decrement signalCount by 1.</li>
     *   <li>Then add this thread to the PriorityLock and re-acquire the lock (as we must wait for higher priority threads in the PriorityLock to run).</li>.
     *   <li>Signal the highest priority thread in this condition object.</li>
     * </ul>
     * 
     * <p>A key difference with PriorityLock is that
     * PriorityLock waits for a condition of a higher thread priority to be signaled,
     * whereas PriorityLockCondition waits for a condition of this thread priority to be signaled.
     */
    class PriorityLockCondition implements Condition {
        private final boolean allowEarlyInterruptFromAwait;
        private final LevelManager levelManager;
        private final int[] waitingOn = new int[Thread.MAX_PRIORITY];
        private int signalCount;
        private int signaledThreadPriority;
        
        private PriorityLockCondition(boolean allowEarlyInterruptFromAwait) {
            this.allowEarlyInterruptFromAwait = allowEarlyInterruptFromAwait;
            this.levelManager = new LevelManager(PriorityLock.this.internalLock);
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         */
        @Override
        public void awaitUninterruptibly() {
            int holdCount = PriorityLock.this.unlockForAwait();
            int priority = levelManager.addThread(Thread.currentThread());
            try {
                for (boolean done = false; !done; ) {
                    try {
                        levelManager.conditions[priority - 1].awaitUninterruptibly();
                        if (priority < signaledThreadPriority) { // CodeCoverage: never hit
                            levelManager.waitUninterruptiblyForHigherPriorityTasksToFinishFromAwait(waitingOn);
                        }
                    } finally {
                        if (signalCount > 0) {
                            signalCount--;
                            done = true;
                        }
                    }
                }
            } finally {
                try {
                    boolean wasSignaled = signaledThreadPriority == priority;
                    PriorityLock.this.lockUninterruptiblyAfterAwait(holdCount, wasSignaled);
                } finally {
                    signaledThreadPriority = 0;
                    if (signalCount > 0) {
                        int priorityToLock = levelManager.removeThreadAndSignalHighest(priority, waitingOn[priority - 1] > 0); // CodeCoverage: partially hit
                        if (priorityToLock != 0) { // CodeCoverage: always true
                            PriorityLock.this.levelManager.addThread(priorityToLock);
                            signaledThreadPriority = priorityToLock;
                        }
                    } else {
                        levelManager.removeThreadOnly(priority);
                    }
                }
            }
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         */
        @Override
        public void await() throws InterruptedException {
            InterruptedException interruptedException = null;
            int holdCount = PriorityLock.this.unlockForAwait();
            int priority = levelManager.addThread(Thread.currentThread());
            try {
                for (boolean done = false; !done; ) {
                    try {
                        try {
                            levelManager.conditions[priority - 1].await();
                        } catch (InterruptedException e) {
                            interruptedException = e;
                            if (allowEarlyInterruptFromAwait) {
                                throw e;
                            }
                        }
                        if (priority < signaledThreadPriority) {
                            // this condition will normally be false as signal/signalAll signal the highest thread
                            // but due to the phenomenon of spurious wakeup, a lower priority thread may wake up before it is signaled
                            if (allowEarlyInterruptFromAwait) {
                                try {
                                    levelManager.waitInterruptiblyForHigherPriorityTasksToFinishFromAwait(waitingOn);
                                } catch (InterruptedException e) {
                                    interruptedException = e;
                                    throw e;
                                }
                            } else {
                                levelManager.waitUninterruptiblyForHigherPriorityTasksToFinishFromAwait(waitingOn);
                            }
                        }
                    } finally {
                        if (signalCount > 0) {
                            signalCount--;
                            done = true;
                        }
                    }
                }
                if (interruptedException != null) {
                    throw interruptedException;
                }
            } finally {
                try {
                    boolean wasSignaled = signaledThreadPriority == priority;
                    if (allowEarlyInterruptFromAwait) {
                        if (interruptedException == null) {
                            PriorityLock.this.lockInterruptiblyAfterAwait(holdCount, wasSignaled);
                        } else {
                            PriorityLock.this.resetAfterAwait(holdCount, wasSignaled);
                        }
                    } else {
                        PriorityLock.this.lockUninterruptiblyAfterAwait(holdCount, wasSignaled);
                        if (Thread.interrupted()) {
                            throw new InterruptedException();
                        }
                    }
                } finally {
                    if (allowEarlyInterruptFromAwait && interruptedException != null && priority < signaledThreadPriority) {
                        levelManager.removeThreadOnly(priority);
                    } else {
                        signaledThreadPriority = 0;
                        if (signalCount > 0) {
                            int priorityToLock = levelManager.removeThreadAndSignalHighest(priority, waitingOn[priority - 1] > 0);
                            if (priorityToLock != 0) { // CodeCoverage: always true
                                PriorityLock.this.levelManager.addThread(priorityToLock);
                                signaledThreadPriority = priorityToLock;
                            }
                        } else {
                            levelManager.removeThreadOnly(priority);
                        }
                    }
                }
            }
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         */
        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            long startTime = System.currentTimeMillis();
            await();
            return System.currentTimeMillis() < startTime + unit.toMillis(time);
        }

        /**
         * Puts the current thread into a wait state until it is signaled.
         */
        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            long startTime = System.nanoTime();
            await();
            long timeLeft = nanosTimeout - (System.nanoTime() - startTime);
            return timeLeft;
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

        /**
         * Signal the highest priority thread in this condition object to wake up.
         */
        @Override
        public void signal() {
            int maxPossible = commonSignal();
            signalCount = Math.min(signalCount + 1, maxPossible);
        }

        /**
         * Signal all threads in this condition object to wake up, but the highest priority thread will go first.
         */
        @Override
        public void signalAll() {
            int maxPossible = commonSignal();
            signalCount = maxPossible;
        }
        
        private int commonSignal() {
            int maxPossible = 0;
            boolean shouldSignal = signalCount == 0;
            for (int priority = Thread.MAX_PRIORITY; priority >= Thread.MIN_PRIORITY; priority--) {
                int index = priority - 1;
                int times = levelManager.counts[index].get();
                if (times > 0) {
                    if (shouldSignal) {
                        levelManager.conditions[index].signal();
                        PriorityLock.this.levelManager.addThread(priority);
                        signaledThreadPriority = priority;
                        shouldSignal = false;
                    }
                    maxPossible += times;
                }
            }
            return maxPossible;
        }
        
        @Override
        public String toString() {
            return getClass().getName() + "@" + hashCode() + " " + levelManager.toString() + ", signalCount=" + signalCount;
        }
        
        public Integer highestPriorityThread() {
            return nullIfZero(levelManager.computeHighestPriority());
        }
    }
    
    
    private static Integer nullIfZero(int val) {
        return val == 0 ? null : val;
    }
    
    
    
    /**
     * Exception thrown when the await function fails to wait for the highest priority thread re-acquire the lock.
     * When this exception is thrown, the priority lock is locked by the current thread,
     * but the current thread is not the one with the highest priority.
     */
    public static class MaybeNotHighestThreadAfterAwaitError extends ThreadDeath {
        private static final long serialVersionUID = 1L;
        
        private MaybeNotHighestThreadAfterAwaitError() {            
        }
    }
}
