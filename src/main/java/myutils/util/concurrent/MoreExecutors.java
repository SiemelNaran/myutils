package myutils.util.concurrent;

import java.util.concurrent.Executors;
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
    
    
}
