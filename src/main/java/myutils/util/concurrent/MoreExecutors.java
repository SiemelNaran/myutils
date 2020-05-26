package myutils.util.concurrent;

import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;


public class MoreExecutors {

    /**
     * Similar to {@link java.util.concurrent.Executors#newFixedThreadPool}
     * except that queued threads are run in order of priority, with highest priority threads first.
     */
    public static PriorityExecutorService newFixedPriorityThreadPool(int numThreads) {
        return newFixedPriorityThreadPool(numThreads, Executors.defaultThreadFactory());
    }
    
    
    /**
     * Similar to {@link java.util.concurrent.Executors#newFixedThreadPool}
     * except that queued threads are run in order of priority, with highest priority threads first.
     */
    public static PriorityExecutorService newFixedPriorityThreadPool(int numThreads, ThreadFactory threadFactory) {
        return new PriorityThreadPoolExecutor(numThreads, numThreads,
                                              0L, TimeUnit.MILLISECONDS,
                                              threadFactory);
    }
    
    
    /**
     * Create a scheduled executor service for testing.
     * Similar to {@link java.util.concurrent.Executors#newScheduledThreadPool}
     * except that scheduled tasks don't run until advanceTime is called.
     * 
     * @param corePoolSize the number of threads in the real executor used to actually run jobs
     * @param threadFactory the thread factory
     * @param startTime the initial time. Will typically be System.currentTimeMillis(), but can set to something else for unit tests.
     */
    public static ScheduledExecutorService newTestScheduledThreadPool(int corePoolSize, long startTime) {
        return new ScheduledThreadPoolTestExecutor(corePoolSize, Executors.defaultThreadFactory(), startTime);
    }

    /**
     * Create a scheduled executor service for testing.
     * Similar to {@link java.util.concurrent.Executors#newScheduledThreadPool}
     * except that scheduled tasks don't run until advanceTime is called.
     * 
     * @param corePoolSize the number of threads in the real executor used to actually run jobs
     * @param threadFactory the thread factory
     * @param startTime the initial time. Will typically be System.currentTimeMillis(), but can set to something else for unit tests.
     */
    public static ScheduledExecutorService newTestScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory, long startTime) {
        return new ScheduledThreadPoolTestExecutor(corePoolSize, threadFactory, startTime);
    }
    
    /**
     * Advance the time to the given time, executing all runnables up till the given time.
     * 
     * <p>The purpose of this static function is so that code can call ScheduledThreadPoolTestExecutor.advanceTime(service, time, unit)
     * and it calls the real advanceTime if service is a myutils.util.concurrent.ScheduledThreadPoolTestExecutor,
     * and waits for the desired time if service is a java.util.concurrent.ScheduledThreadPoolExecutor.
     * 
     * @throws CompletionException if this thread is interrupted with the cause as the InterruptedException
     * @see ScheduledThreadPoolTestExecutor#advanceTime(long, TimeUnit)
     */
    public static void advanceTime(ScheduledExecutorService service, long time, TimeUnit unit) {
        if (service instanceof ScheduledThreadPoolTestExecutor) {
            ScheduledThreadPoolTestExecutor testService = (ScheduledThreadPoolTestExecutor) service;
            testService.advanceTime(time, unit);
        } else {
            long millis = unit.toMillis(time);
            int nanos = unit == TimeUnit.NANOSECONDS ? nanos = (int) time % 1_000_000 : 0;
            try {
                Thread.sleep(millis, nanos);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
        }
    }
    
    /**
     * Return the time at which the current thread spawned from a schedule executor service runs.
     * If called for a thread spawned from ScheduledThreadPoolTestExecutor return the time at which the thread is scheduled to run.
     * If called for a thread spawned from the regular java.util.concurrent.ScheduledThreadPoolExecutor or such then return System.currentTimeMillis().
     */
    public static long currentTimeMillis(ScheduledExecutorService service) {
        if (service instanceof ScheduledThreadPoolTestExecutor) {
            ScheduledThreadPoolTestExecutor testService = (ScheduledThreadPoolTestExecutor) service; 
            return testService.currentTimeMillis();
        } else {
            return System.currentTimeMillis();
        }
    }
}
