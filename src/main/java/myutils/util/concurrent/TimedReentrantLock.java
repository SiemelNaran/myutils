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
        super.lockInterruptibly();
        setTimesOnAcquireLock();
    }
    
    @Override
    public boolean tryLock() {
        setStartTime();
        boolean acquired = super.tryLock();
        setTimesOnAcquireLock();
        return acquired;
    }
    
    @Override
    public boolean tryLock(long timeout, TimeUnit unit) throws InterruptedException {
        setStartTime();
        boolean acquired = super.tryLock(timeout, unit);
        setTimesOnAcquireLock();
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
        totalWaitTime += now - startTime.get();
        this.lockStartTime = now;
    }

    public final Duration getTotalWaitTime() {
        return Duration.ofMillis(totalWaitTime);
    }

    public final Duration getTotalLockRunningTime() {
        return Duration.ofMillis(totalLockRunningTime);
    }
    
    public final Duration getApproximateTotalIdleTime() {
        return Duration.ofMillis(System.currentTimeMillis() - creationTime)
                       .minus(getTotalLockRunningTime());
    }
}


