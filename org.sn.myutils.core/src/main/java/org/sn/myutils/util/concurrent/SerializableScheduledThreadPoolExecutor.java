package org.sn.myutils.util.concurrent;

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
import javax.annotation.Nonnull;
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

    @Override
    public @Nonnull List<Runnable> shutdownNow() {
        List<Runnable> runnables = super.shutdownNow();
        ArrayList<RunnableInfo> tasks = new ArrayList<>();
        for (Iterator<Runnable> iter = runnables.iterator(); iter.hasNext(); ) {
            Runnable runnable = iter.next();
            if (runnable instanceof RunnableScheduledFuture) {
                RunnableScheduledFuture<?> standardRunnable = (RunnableScheduledFuture<?>) runnable;
                if (standardRunnable.isCancelled()) {
                    continue;
                }
            }
                
            if (!(runnable instanceof DecoratedRunnableScheduledFuture)) {
                continue;
            }
            
            DecoratedRunnableScheduledFuture<?> decoratedRunnable = (DecoratedRunnableScheduledFuture<?>) runnable;
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
        List<Class<?>> errors = new ArrayList<>();
        
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
    public @Nonnull ScheduledFuture<?> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(SerializableLambdaUtils.computeRunnableInfo(command, delay, 0, unit, logIfCannotSerializeLevel));
            return super.schedule(command, delay, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    public @Nonnull <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(SerializableLambdaUtils.computeRunnableInfo(callable, delay, unit, logIfCannotSerializeLevel));
            return super.schedule(callable, delay, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    public @Nonnull ScheduledFuture<?> scheduleAtFixedRate(@Nonnull Runnable command, long initialDelay, long period, @Nonnull TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(SerializableLambdaUtils.computeRunnableInfo(command, initialDelay, period, unit, logIfCannotSerializeLevel));
            return super.scheduleAtFixedRate(command, initialDelay, period, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    public @Nonnull ScheduledFuture<?> scheduleWithFixedDelay(@Nonnull Runnable command, long initialDelay, long delay, @Nonnull TimeUnit unit) {
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
        if (runnable instanceof DecoratedRunnableScheduledFuture) {
            DecoratedRunnableScheduledFuture<?> decoratedRunnable = (DecoratedRunnableScheduledFuture<?>) runnable;
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
        public V get(long timeout, @Nonnull TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return future.get(timeout, unit);
        }

        @Override
        public long getDelay(@Nonnull TimeUnit unit) {
            return future.getDelay(unit);
        }

        @Override
        public int compareTo(@Nonnull Delayed that) {
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
