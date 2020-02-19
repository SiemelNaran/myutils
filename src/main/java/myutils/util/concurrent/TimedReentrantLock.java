package myutils.util.concurrent;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import javax.annotation.Nullable;


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

    
    public static TimedReentrantLock newLock() {
        return new TimedReentrantLock(false);
    }
    
    public static TimedReentrantLock newFairLock() {
        return new TimedReentrantLock(true);
    }
    
    public static Statistics toStatistics(TimedReentrantLock lock, @Nullable Set<String> collisionTracking) {
        return new Statistics(lock, collisionTracking);
    }
    
    /**
     * The statistics of each lock.
     * Use to fine-tune hashLocksSize.
     */
    public static final class Statistics {
        private final boolean locked;
        private final int queueLength;
        private final Duration totalWaitTime;
        private final Duration totalLockRunningTime;
        private final Duration approximateTotalIdleTime;
        private final int usage;
        
        private Statistics(TimedReentrantLock lock, @Nullable Set<String> collisionTracking) {
            this.locked = lock.isLocked();
            this.queueLength = lock.getQueueLength();
            this.totalWaitTime = lock.getTotalWaitTime();
            this.totalLockRunningTime = lock.getTotalLockRunningTime();
            this.approximateTotalIdleTime = lock.getApproximateTotalIdleTime();
            this.usage = collisionTracking != null ? collisionTracking.size() : -1;
        }
        
        /**
         * Tells whether the lock is locked right now.
         */
        public boolean isLocked() {
            return locked;
        }

        /**
         * An estimate of the number of threads waiting on this lock right now.
         */
        public int getQueueLength() {
            return queueLength;
        }

        /**
         * The total time to lock the lock.
         */
        public Duration getTotalWaitTime() {
            return totalWaitTime;
        }

        /**
         * The time from the time the lock was acquired to unlock.
         */
        public Duration getTotalLockRunningTime() {
            return totalLockRunningTime;
        }
        
        /**
         * The approximate time the lock has not bee in use.
         * This is simply the difference between the lock creation time and now, and the total lock running time.
         * 
         * <p>It is approximate because if the lock is in use at the time this function called,
         * totalLockRunningTime has not been updated (it is only updated upon unlock).
         */
        public Duration getApproximateTotalIdleTimes() {
            return approximateTotalIdleTime;
        }

        /**
         * The number of distinct strings using this lock.
         * 0 indicates that hashLocksSize is too large.
         * >1 indicates that hashLocksSize is too small.
         */
        public int getUsage() {
            return usage;
        }
    }    
}
