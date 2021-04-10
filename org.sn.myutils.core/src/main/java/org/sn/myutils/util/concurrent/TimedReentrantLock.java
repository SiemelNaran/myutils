package org.sn.myutils.util.concurrent;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.sn.myutils.annotations.NotNull;


/**
 * A reentrant lock that tracks the time spent waiting lock to be acquired,
 * as well as the total amount of time the lock ran for after it was acquired,
 * as well as the total approximate amount of time the lock was idle.
 */
public class TimedReentrantLock extends ReentrantLock {
    private static final long serialVersionUID = 1L;        
    private static ThreadLocal<Long> startTime = new ThreadLocal<>();
    
    private final long creationTime;
    private long lockStartTime;
    private long totalWaitTime;
    private long totalLockRunningTime;
    
    public TimedReentrantLock() {
        this(false);
    }
    
    public TimedReentrantLock(boolean fair) {
        super(fair);
        creationTime = System.currentTimeMillis();
    }
    
    @Override
    public void lock() {
        boolean alreadyLocked = setStartTimeIfNotLocked();
        super.lock();
        if (!alreadyLocked) {
            setTimesOnAcquireLock();
        }
    }
    
    @Override
    public void lockInterruptibly() throws InterruptedException {
        boolean alreadyLocked = setStartTimeIfNotLocked();
        try {
            super.lockInterruptibly();
            if (!alreadyLocked) {
                setTimesOnAcquireLock();
            }
        } catch (InterruptedException e) {
            setWaitTimeOnAcquireLock(System.currentTimeMillis());
            throw e;
        }
    }
    
    @Override
    public boolean tryLock() {
        boolean alreadyLocked = setStartTimeIfNotLocked();
        boolean acquired = super.tryLock();
        if (acquired && !alreadyLocked) {
            setTimesOnAcquireLock();
        }
        return acquired;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        boolean alreadyLocked = setStartTimeIfNotLocked();
        boolean acquired = super.tryLock(timeout, unit);
        if (!alreadyLocked) {
            if (acquired) {
                setTimesOnAcquireLock();
            } else {
                setWaitTimeOnAcquireLock(System.currentTimeMillis());
            }
        }
        return acquired;
    }

    @Override
    public void unlock() {
        long delta = setRunningTimeOnUnlock();
        super.unlock();
        if (isHeldByCurrentThread()) {
            totalLockRunningTime -= delta;
        }
    }
    
    @Override
    public @NotNull Condition newCondition() {
        return new InternalCondition(super.newCondition());
    }
    
    private class InternalCondition implements Condition {
        private final Condition internalCondition;

        public InternalCondition(Condition internalCondition) {
            this.internalCondition = internalCondition;
        }

        @Override
        public void awaitUninterruptibly() {
            setRunningTimeOnUnlock();
            setStartTimeWhenLocked();
            try {
                internalCondition.awaitUninterruptibly();
            } finally {
                setTimesOnAcquireLock();
            }
        }

        @Override
        public void await() throws InterruptedException {
            setRunningTimeOnUnlock();
            setStartTimeWhenLocked();
            try {
                internalCondition.await();
            } finally {
                setTimesOnAcquireLock();
            }
        }

        @Override
        public boolean await(long time, TimeUnit unit) throws InterruptedException {
            setRunningTimeOnUnlock();
            setStartTimeWhenLocked();
            try {
                return internalCondition.await(time, unit);
            } finally {
                setTimesOnAcquireLock();
            }
        }

        @Override
        public long awaitNanos(long nanosTimeout) throws InterruptedException {
            setRunningTimeOnUnlock();
            setStartTimeWhenLocked();
            try {
                return internalCondition.awaitNanos(nanosTimeout);
            } finally {
                setTimesOnAcquireLock();
            }
        }

        @Override
        public boolean awaitUntil(@NotNull Date deadline) throws InterruptedException {
            setRunningTimeOnUnlock();
            setStartTimeWhenLocked();
            try {
                return internalCondition.awaitUntil(deadline);
            } finally {
                setTimesOnAcquireLock();
            }
        }

        @Override
        public void signal() {
            internalCondition.signal();            
        }

        @Override
        public void signalAll() {
            internalCondition.signalAll();            
        }
        
    }

    /**
     * Called from lock functions.
     * If this.isHeldByCurrentThread is false then set startTime to now.
     * This is used to set totalWaitTime upon the lock getting acquired.
     *
     * @return this.isHeldByCurrentThread
     */
    private boolean setStartTimeIfNotLocked() {
        if (isHeldByCurrentThread()) {
            return true;
        } else {
            startTime.set(System.currentTimeMillis());
            return false;
        }
    }

    /**
     * Called from await functions.
     * Set startTime to now.
     * This is used to set totalWaitTime upon the lock getting acquired.
     */
    private void setStartTimeWhenLocked() {
        assert isHeldByCurrentThread();
        startTime.set(System.currentTimeMillis());
    }

    private void setTimesOnAcquireLock() {
        long now = System.currentTimeMillis();
        setWaitTimeOnAcquireLock(now);
        this.lockStartTime = now;
    }

    private void setWaitTimeOnAcquireLock(long now) {
        totalWaitTime += now - startTime.get();
    }
    
    private long setRunningTimeOnUnlock() {
        long delta = System.currentTimeMillis() - lockStartTime;
        totalLockRunningTime += delta;
        return delta;
    }
    
    public final Duration getTotalWaitTime() {
        return Duration.ofMillis(totalWaitTime);
    }

    public final Duration getTotalLockRunningTime() {
        return Duration.ofMillis(totalLockRunningTime);
    }
    
    public final Duration getTotalIdleTime() {
        return Duration.ofMillis(System.currentTimeMillis() - creationTime)
                       .minus(getTotalLockRunningTime());
    }
}
