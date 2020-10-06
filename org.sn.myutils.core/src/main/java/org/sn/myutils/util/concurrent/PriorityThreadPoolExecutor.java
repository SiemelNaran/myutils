package org.sn.myutils.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.sn.myutils.util.OptionalUtils;


class PriorityThreadPoolExecutor extends ThreadPoolExecutor implements PriorityExecutorService {

    public PriorityThreadPoolExecutor(int corePoolSize,
                                      int maximumPoolSize,
                                      long keepAliveTime,
                                      TimeUnit unit,
                                      ThreadFactory threadFactory) {
        super(corePoolSize, maximumPoolSize, keepAliveTime, unit, new PriorityBlockingQueue<Runnable>(256), threadFactory);
    }
    
    @Override
    public <V> Future<V> submit(int priority, Callable<V> task) {
        return super.submit(new PriorityCallable<V>(priority, task));
    }

    @Override
    public Future<?> submit(int priority, Runnable task) {
        return super.submit(new PriorityRunnable(priority, task));
    }

    @Override
    public <V> Future<V> submit(int priority, Runnable task, V result) {
        return super.submit(new PriorityRunnable(priority, task), result);
    }
    
    @Override
    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        int priority = OptionalUtils.of(runnable, PriorityRunnable.class).map(PriorityRunnable::getPriority).orElse(Thread.NORM_PRIORITY);
        return new PriorityFutureTask<T>(runnable, value, priority);
    }

    @Override
    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        int priority = OptionalUtils.of(callable, PriorityCallable.class).map(PriorityCallable::getPriority).orElse(Thread.NORM_PRIORITY);
        return new PriorityFutureTask<T>(callable, priority);
    }
    
    private static class PriorityCallable<V> implements Callable<V> {
        private final int priority;
        private final Callable<V> callable;
        
        PriorityCallable(int priority, Callable<V> callable) {
            this.priority = priority;
            this.callable = callable;
        }

        private int getPriority() {
            return priority;
        }
        
        @Override
        public V call() throws Exception {
            Thread.currentThread().setPriority(priority);
            return callable.call();
        }
    }

    private static class PriorityRunnable implements Runnable {
        private final int priority;
        private final Runnable runnable;
        
        PriorityRunnable(int priority, Runnable runnable) {
            this.priority = priority;
            this.runnable = runnable;
        }

        private int getPriority() {
            return priority;
        }
        
        @Override
        public void run() {
            Thread.currentThread().setPriority(priority);
            runnable.run();
        }        
    }

    private static class PriorityFutureTask<V> extends FutureTask<V> implements Runnable, Comparable<PriorityFutureTask<V>> {
        private final int priority;
        
        public PriorityFutureTask(Runnable runnable, V value, int priority) {
            super(runnable, value);
            this.priority = priority;
        }

        public PriorityFutureTask(Callable<V> callable, int priority) {
            super(callable);
            this.priority = priority;
        }

        @Override
        public int compareTo(PriorityFutureTask<V> that) {
            return that.priority - this.priority;
        }        
    }
}
