package org.sn.myutils.util.concurrent;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;


public class MoreExecutors {
    private MoreExecutors() {
    }

    /**
     * Similar to {@link java.util.concurrent.Executors#newFixedThreadPool}
     * except that queued threads are run in order of priority, with the highest priority threads first.
     */
    public static PriorityExecutorService newFixedPriorityThreadPool(int numThreads) {
        return newFixedPriorityThreadPool(numThreads, Executors.defaultThreadFactory());
    }
    
    
    /**
     * Similar to {@link java.util.concurrent.Executors#newFixedThreadPool}
     * except that queued threads are run in order of priority, with the highest priority threads first.
     */
    public static PriorityExecutorService newFixedPriorityThreadPool(int numThreads, ThreadFactory threadFactory) {
        return new PriorityThreadPoolExecutor(numThreads, numThreads,
                                              0L, TimeUnit.MILLISECONDS,
                                              threadFactory);
    }
    
    
    /**
     * Create a serializable scheduled executor service.
     * If scheduled tasks inherit from SerializableRunnable or SerializableCallable, the executor can serialize/export unfinished tasks.
     */
    public static SerializableScheduledExecutorService newSerializableScheduledThreadPool(int corePoolSize) {
        return new SerializableScheduledThreadPoolExecutor(corePoolSize);
    }

    /**
     * Create a serializable scheduled executor service.
     * If scheduled tasks inherit from SerializableRunnable or SerializableCallable, the executor can serialize/export unfinished tasks.
     */
    public static SerializableScheduledExecutorService newSerializableScheduledThreadPool(int corePoolSize, ThreadFactory threadFactory) {
        return new SerializableScheduledThreadPoolExecutor(corePoolSize, threadFactory);
    }


    /**
     * Create a scheduled executor service that stores tasks to run in time buckets (i.e. files)
     * so that we don't have to store millions of tasks in memory.
     *
     * @throws IOException if there was an error loading the existing time buckets
     */
    public static ScheduledExecutorService newTimeBucketScheduledThreadPool(Path folder,
                                                                                         Duration timeBucketLength,
                                                                                         int corePoolSize,
                                                                                         ThreadFactory threadFactory) throws IOException {
        return new TimeBucketScheduledThreadPoolExecutor(folder, timeBucketLength, corePoolSize, threadFactory, new ThreadPoolExecutor.AbortPolicy());
    }


    /**
     * Create a scheduled executor service for testing.
     * Similar to {@link java.util.concurrent.Executors#newScheduledThreadPool}
     * except that scheduled tasks don't run until advanceTime is called.
     * 
     * @param corePoolSize the number of threads in the real executor used to actually run jobs
     * @param startTime the initial time. Will typically be System.currentTimeMillis(), but can set to something else for unit tests.
     */
    public static ScheduledExecutorService newTestScheduledThreadPool(int corePoolSize, long startTime) {
        return new TestScheduledThreadPoolExecutor(corePoolSize, Executors.defaultThreadFactory(), startTime);
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
        return new TestScheduledThreadPoolExecutor(corePoolSize, threadFactory, startTime);
    }
    
    /**
     * Advance the time to the given time, executing all runnables up till the given time.
     * 
     * <p>The purpose of this static function is so that code can call TestScheduledThreadPoolExecutor.advanceTime(service, time, unit)
     * and it calls the real advanceTime if service is a utils.util.concurrent.TestScheduledThreadPoolExecutor,
     * and waits for the desired time if service is a java.util.concurrent.ScheduledThreadPoolExecutor.
     * 
     * @throws CompletionException if this thread is interrupted with the cause as the InterruptedException
     * @see TestScheduledThreadPoolExecutor#advanceTime(long, TimeUnit)
     */
    public static void advanceTime(ScheduledExecutorService service, long time, TimeUnit unit) {
        if (service instanceof TestScheduledThreadPoolExecutor testService) {
            testService.advanceTime(time, unit);
        } else {
            long millis = unit.toMillis(time);
            int nanos = unit == TimeUnit.NANOSECONDS ? (int) time % 1_000_000 : 0;
            try {
                Thread.sleep(millis, nanos);
            } catch (InterruptedException e) {
                throw new CompletionException(e);
            }
        }
    }
    
    /**
     * Return the time at which the current thread spawned from a schedule executor service runs.
     * If called for a thread spawned from TestScheduledThreadPoolExecutor return the time at which the thread is scheduled to run.
     * If called for a thread spawned from the regular java.util.concurrent.ScheduledThreadPoolExecutor or such then return System.currentTimeMillis().
     */
    public static long currentTimeMillis(ScheduledExecutorService service) {
        if (service instanceof TestScheduledThreadPoolExecutor testService) {
            return testService.currentTimeMillis();
        } else {
            return System.currentTimeMillis();
        }
    }
    
    
    /**
     * Create a thread factory.
     * 
     * @param basename the thread names will be basename-1, basename-2, etc
     * @param daemon true if the new threads should be daemon threads
     * @return a thread factory whose first thread is basename-1, second is basename-2, etc.
     */
    public static ThreadFactory createThreadFactory(String basename, boolean daemon) {
        ThreadGroup threadGroup = new ThreadGroup(basename);
        final AtomicInteger count = new AtomicInteger();
        return runnable -> {
            var thread = new Thread(threadGroup, runnable, basename + "-" + count.incrementAndGet());
            thread.setDaemon(daemon);
            return thread;
        };
    }
}
