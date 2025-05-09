package org.sn.myutils.util.concurrent;

import java.io.Serial;
import java.lang.System.Logger.Level;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Stream;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.util.concurrent.SerializableLambdaUtils.RunnableInfo;
import org.sn.myutils.util.concurrent.SerializableLambdaUtils.TimeInfo;



public class SerializableScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor implements SerializableScheduledExecutorService {

    private final ThreadLocal<RunnableInfo> threadLocalRunnableInfo = new ThreadLocal<>();
    private volatile Level logIfCannotSerializeLevel = Level.TRACE;
    private ArrayList<RunnableInfo> exceptionalRunnableInfos = new ArrayList<>();
    private UnfinishedTasksImpl unfinishedTasks;
    
    
    // constructors
    
    public SerializableScheduledThreadPoolExecutor(int corePoolSize) {
        super(corePoolSize);
    }

    public SerializableScheduledThreadPoolExecutor(int corePoolSize,
                                                   ThreadFactory threadFactory) {
        super(corePoolSize, threadFactory);
    }

    public SerializableScheduledThreadPoolExecutor(int corePoolSize,
                                                   RejectedExecutionHandler handler) {
        super(corePoolSize, handler);
    }

    public SerializableScheduledThreadPoolExecutor(int corePoolSize,
                                                   ThreadFactory threadFactory,
                                                   RejectedExecutionHandler handler) {
        super(corePoolSize, threadFactory, handler);
    }
    
    
    public void setLogIfCannotSerializeLevel(Level level) {
        this.logIfCannotSerializeLevel = level;
    }

    /**
     * {@inheritdoc}
     *
     * <p>This function does not return tasks that are serializable and not canceled.
     * Instead, they are added to a member variable 'unfinishedTasks',
     * and the initial delay is reset from what the user created the task as to from the time of export.
     * Running time O(N^2).
     */
    @Override
    public @NotNull List<Runnable> shutdownNow() {
        List<Runnable> runnables = super.shutdownNow();
        ArrayList<RunnableInfo> tasks = new ArrayList<>();
        for (Iterator<Runnable> iter = runnables.iterator(); iter.hasNext(); ) {
            Runnable runnable = iter.next();
            if (runnable instanceof RunnableScheduledFuture<?> standardRunnable) {
                if (standardRunnable.isCancelled()) {
                    continue;
                }
            }
                
            if (!(runnable instanceof DecoratedRunnableScheduledFuture<?> decoratedRunnable)) {
                continue;
            }

            RunnableInfo runnableInfo = decoratedRunnable.getRunnableInfo();
            if (runnableInfo != null) {
                TimeInfo timeInfo = runnableInfo.getTimeInfo();
                long timeToNextRun = decoratedRunnable.getDelay(timeInfo.getUnit());
                timeInfo.setInitialDelay(Math.max(timeToNextRun, 0));
                tasks.add(runnableInfo);
                iter.remove();
            }
        }
        unfinishedTasks = new UnfinishedTasksImpl(tasks);
        return runnables;
    }
    
    @Override
    public UnfinishedTasks exportUnfinishedTasks(boolean includeExceptions) {
        UnfinishedTasksImpl tasks = unfinishedTasks;
        unfinishedTasks = null;
        if (includeExceptions) {
            tasks.addRunnableInfos(exceptionalRunnableInfos);
        }
        exceptionalRunnableInfos = new ArrayList<>();
        return tasks;
    }

    @Override
    public Map<Class<?>, List<ScheduledFuture<?>>> importUnfinishedTasks(UnfinishedTasks unfinished,
                                                                         Collection<Class<?>> returnFutures)
                throws RecreateRunnableFailedException {
        UnfinishedTasksImpl unfinishedImpl = (UnfinishedTasksImpl) unfinished;
        
        Map<Class<?>, List<ScheduledFuture<?>>> result = new HashMap<>();
        ArrayList<Class<?>> errors = new ArrayList<>();
        
        for (RunnableInfo runnableInfo: unfinishedImpl.getRunnables()) {
            try {
                SimpleEntry<Class<?>, RunnableScheduledFuture<?>> data = runnableInfo.apply(this);
                if (returnFutures.contains(data.getKey())) {
                    List<ScheduledFuture<?>> list = result.computeIfAbsent(data.getKey(), ignored -> new ArrayList<>());
                    list.add(data.getValue());
                }
            } catch (RecreateRunnableFailedException e) {
                errors.addAll(e.getFailedClasses());
            }
        }
        
        if (!errors.isEmpty()) {
            throw new RecreateRunnableFailedException(errors);
        }
        
        return result;
    }

    @Override
    public @NotNull ScheduledFuture<?> schedule(@NotNull Runnable command, long delay, @NotNull TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(SerializableLambdaUtils.computeRunnableInfo(command, delay, 0, unit, logIfCannotSerializeLevel));
            return super.schedule(command, delay, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    public @NotNull <V> ScheduledFuture<V> schedule(@NotNull Callable<V> callable, long delay, @NotNull TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(SerializableLambdaUtils.computeRunnableInfo(callable, delay, unit, logIfCannotSerializeLevel));
            return super.schedule(callable, delay, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    public @NotNull ScheduledFuture<?> scheduleAtFixedRate(@NotNull Runnable command, long initialDelay, long period, @NotNull TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(SerializableLambdaUtils.computeRunnableInfo(command, initialDelay, period, unit, logIfCannotSerializeLevel));
            return super.scheduleAtFixedRate(command, initialDelay, period, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    public @NotNull ScheduledFuture<?> scheduleWithFixedDelay(@NotNull Runnable command, long initialDelay, long delay, @NotNull TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(SerializableLambdaUtils.computeRunnableInfo(command, initialDelay, -delay, unit, logIfCannotSerializeLevel));
            return super.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
        task = super.decorateTask(runnable, task);
        RunnableInfo runnableInfo = threadLocalRunnableInfo.get();
        if (runnableInfo != null) {
            return new DecoratedRunnableScheduledFuture<>(task, runnableInfo);
        } else {
            return task;
        }
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
        task = super.decorateTask(callable, task);
        RunnableInfo runnableInfo = threadLocalRunnableInfo.get();
        if (runnableInfo != null) {
            return new DecoratedRunnableScheduledFuture<>(task, runnableInfo);
        } else {
            return task;
        }
    }

    
    @Override
    protected void afterExecute(Runnable runnable, Throwable throwable) {
        if (runnable instanceof DecoratedRunnableScheduledFuture<?> decoratedRunnable) {
            if (decoratedRunnable.isDone()) {
                RunnableInfo runnableInfo = decoratedRunnable.getRunnableInfo();
                if (runnableInfo != null) {
                    try {
                        decoratedRunnable.get();
                    } catch (ExecutionException e) {
                        runnableInfo.setCompletedExceptionally();
                        exceptionalRunnableInfos.add(runnableInfo);
                    }  catch (InterruptedException e) {
                        runnableInfo.setInterrupted();
                        runnableInfo.setCompletedExceptionally();
                        exceptionalRunnableInfos.add(runnableInfo);
                    }
                }
            }
        }
    }

    private static class DecoratedRunnableScheduledFuture<V> implements RunnableScheduledFuture<V> {
        private final RunnableScheduledFuture<V> future;
        private RunnableInfo runnableInfo;
        
        DecoratedRunnableScheduledFuture(RunnableScheduledFuture<V> future, RunnableInfo runnableInfo) {
            this.future = future;
            this.runnableInfo = runnableInfo;
        }
        
        @Override
        public void run() {
            try {
                future.run();
            } finally {
                if (!isPeriodic()) {
                    runnableInfo = null;
                }
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            runnableInfo = null;
            return future.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return future.isCancelled();
        }

        @Override
        public boolean isDone() {
            return future.isDone();
        }

        @Override
        public V get() throws InterruptedException, ExecutionException {
            return future.get();
        }

        @Override
        public V get(long timeout, @NotNull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return future.get(timeout, unit);
        }

        @Override
        public long getDelay(@NotNull TimeUnit unit) {
            return future.getDelay(unit);
        }

        @Override
        public int compareTo(@NotNull Delayed that) {
            return future.compareTo(that);
        }

        @Override
        public boolean isPeriodic() {
            return future.isPeriodic();
        }
        
        private RunnableInfo getRunnableInfo() {
            return runnableInfo;
        }
    }
    
    
    static class UnfinishedTasksImpl implements UnfinishedTasks {
        @Serial
        private static final long serialVersionUID = 1L;

        private final ArrayList<RunnableInfo> runnableInfos;

        public UnfinishedTasksImpl(ArrayList<RunnableInfo> runnableInfos) {
            this.runnableInfos = runnableInfos;
        }
        
        public void addRunnableInfos(List<RunnableInfo> moreRunnableInfos) {
            runnableInfos.addAll(moreRunnableInfos);
        }

        protected ArrayList<RunnableInfo> getRunnables() {
            return runnableInfos;
        }
        
        @Override
        public Stream<TaskInfo> stream() {
            return runnableInfos.stream().map(RunnableInfo::toTaskInfo);
        }
    }
}
