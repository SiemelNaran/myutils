package myutils.util.concurrent;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


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
        setStartTime();
        super.lock();
        setTimesOnAcquireLock();
    }
    
    @Override
    public void lockInterruptibly() throws InterruptedException {
        setStartTime();
        try {
            super.lockInterruptibly();
            setTimesOnAcquireLock();
        } catch (InterruptedException e) {
            setWaitTimeOnAcquireLock(System.currentTimeMillis());
            throw e;
        }
    }
    
    @Override
    public boolean tryLock() {
        setStartTime();
        boolean acquired = super.tryLock();
        if (acquired) {
            setTimesOnAcquireLock();
        }
        return acquired;
    }

    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        setStartTime();
        boolean acquired = super.tryLock(timeout, unit);
        if (acquired) {
            setTimesOnAcquireLock();
        } else {
            setWaitTimeOnAcquireLock(System.currentTimeMillis());
        }
        return acquired;
    }

    @Override
    public void unlock() {
        totalLockRunningTime += System.currentTimeMillis() - lockStartTime;
        super.unlock();
    }
    
    private final void setStartTime() {
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
