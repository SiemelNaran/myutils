package org.sn.myutils.util.concurrent;

import java.io.Serializable;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;


public class SerializableScheduledThreadPoolExecutor extends ScheduledThreadPoolExecutor implements SerializableScheduledExecutorService {

    private static final Logger LOGGER = Logger.getLogger(SerializableScheduledThreadPoolExecutor.class.getName());
    
    private final ThreadLocal<RunnableInfo> threadLocalRunnableInfo = new ThreadLocal<>();
    private volatile Level logIfCannotSerialize = Level.FINEST;
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
    
    
    public void logIfCannotSerialize(Level logIfCannotSerialize) {
        this.logIfCannotSerialize = logIfCannotSerialize;
    }

    @Override
    public List<Runnable> shutdownNow() {
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
        UnfinishedTasks tasks = unfinishedTasks;
        unfinishedTasks = null;
        if (includeExceptions) {
            ((UnfinishedTasksImpl) tasks).addRunnableInfos(exceptionalRunnableInfos);
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
                SimpleEntry<Class<?>, ScheduledFuture<?>> data = runnableInfo.apply(this);
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
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(computeRunnableInfo(command, delay, 0, unit));
            return super.schedule(command, delay, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(computeRunnableInfo(callable, delay, 0, unit));
            return super.schedule(callable, delay, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(computeRunnableInfo(command, initialDelay, period, unit));
            return super.scheduleAtFixedRate(command, initialDelay, period, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        try {
            threadLocalRunnableInfo.set(computeRunnableInfo(command, initialDelay, -delay, unit));
            return super.scheduleWithFixedDelay(command, initialDelay, delay, unit);
        } finally {
            threadLocalRunnableInfo.remove();
        }
    }
    
    private RunnableInfo computeRunnableInfo(Runnable command, long initialDelay, long period, TimeUnit unit) {
        if (command instanceof SerializableRunnable) {
            return new RunnableInfo(new TimeInfo(initialDelay, period, unit), (SerializableRunnable) command);
        } else {
            Class<? extends Runnable> clazz = command.getClass();
            if (!clazz.isLocalClass()) { 
                if (!clazz.isSynthetic()) {
                    try {
                        clazz.getConstructor();
                        return new RunnableInfo(new TimeInfo(initialDelay, period, unit), clazz);
                    } catch (NoSuchMethodException | SecurityException e) {
                        LOGGER.log(logIfCannotSerialize, "Cannot serialize " + clazz.getName() + " - no public constructor");
                    }
                } else {
                    LOGGER.log(logIfCannotSerialize, "Cannot serialize " + clazz.getName() + " - synthetic class");
                }
            } else {
                LOGGER.log(logIfCannotSerialize, "Cannot serialize " + clazz.getName() + " - local class");
            }
        }
        return null;
    }
    
    
    @SuppressWarnings("rawtypes")
    private <V> RunnableInfo computeRunnableInfo(Callable<V> callable, long initialDelay, long period, TimeUnit unit) {
        if (callable instanceof SerializableCallable) {
            SerializableRunnable command = new AdaptSerializableCallable((SerializableCallable<V>) callable); 
            return new RunnableInfo(new TimeInfo(initialDelay, period, unit), command);
        } else {
            Class<? extends Callable> clazz = callable.getClass();
            if (!clazz.isLocalClass()) { 
                if (!clazz.isSynthetic()) {
                    try {
                        clazz.getConstructor();
                        SerializableRunnable command = new AdaptSerializableCallableClass(clazz); 
                        return new RunnableInfo(new TimeInfo(initialDelay, period, unit), command);
                    } catch (NoSuchMethodException | SecurityException e) {
                        LOGGER.log(logIfCannotSerialize, "Cannot serialize " + clazz.getName() + " - no public constructor");
                    }
                } else {
                    LOGGER.log(logIfCannotSerialize, "Cannot serialize " + clazz.getName() + " - synthetic class");
                }
            } else {
                LOGGER.log(logIfCannotSerialize, "Cannot serialize " + clazz.getName() + " - local class");
            }
        }
        return null;
    }
    
    private static class AdaptSerializableCallable implements SerializableRunnable {
        private static final long serialVersionUID = 1L;
        
        private final SerializableCallable<?> callable;

        AdaptSerializableCallable(SerializableCallable<?> callable) {
            this.callable = callable;
        }

        @Override
        public void run() {
            try {
                callable.call();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }
    }
    
    @SuppressWarnings("rawtypes")
    private static class AdaptSerializableCallableClass implements SerializableRunnable {
        private static final long serialVersionUID = 1L;

        private final Class<? extends Callable> clazz;

        <V> AdaptSerializableCallableClass(Class<? extends Callable> clazz) {
            this.clazz = clazz;
        }

        @Override
        public void run() {
            try {
                Callable callable = clazz.getDeclaredConstructor().newInstance();
                callable.call();
            } catch (Exception e) {
                throw new CompletionException(e);
            }
        }
    }
    

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Runnable runnable, RunnableScheduledFuture<V> task) {
        task = super.decorateTask(runnable, task);
        RunnableInfo runnableInfo = threadLocalRunnableInfo.get();
        if (runnableInfo != null) {
            return new DecoratedRunnableScheduledFuture<V>(task, runnableInfo);
        } else {
            return task;
        }
    }

    @Override
    protected <V> RunnableScheduledFuture<V> decorateTask(Callable<V> callable, RunnableScheduledFuture<V> task) {
        task = super.decorateTask(callable, task);
        RunnableInfo runnableInfo = threadLocalRunnableInfo.get();
        if (runnableInfo != null) {
            return new DecoratedRunnableScheduledFuture<V>(task, runnableInfo);
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

    protected static final class TimeInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private long initialDelay;
        private final long period;
        private final TimeUnit unit;
        
        private TimeInfo(long initialDelay, long period, TimeUnit unit) {
            this.initialDelay = initialDelay;
            this.period = period;
            this.unit = unit;
        }
        
        protected long getInitialDelay() {
            return initialDelay;
        }
        
        /**
         * Return the period.
         * A positive value means scheduleAtFixedRate.
         * A negative value means scheduleWithFixedDelay.
         */
        protected long getPeriod() {
            return period;
        }
        
        protected TimeUnit getUnit() {
            return unit;
        }
        
        private void setInitialDelay(long initialDelay) {
            this.initialDelay = initialDelay;
        }
    }
    
    protected static class RunnableInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final byte FLAGS_COMPLETED_EXCEPTIONALLY = 1;
        private static final byte FLAGS_INTERRUPTED = 2;
        
        private final TimeInfo timeInfo;
        private final SerializableRunnable serializableRunnable;
        private final Class<? extends Runnable> runnableClass;
        private byte flags;
        
        private RunnableInfo(TimeInfo info, SerializableRunnable runnable) {
            this.timeInfo = info;
            this.serializableRunnable = runnable;
            this.runnableClass = null;
        }
        
        private RunnableInfo(TimeInfo info, Class<? extends Runnable> runnableClass) {
            this.timeInfo = info;
            this.serializableRunnable = null;
            this.runnableClass = runnableClass;
        }
        
        protected TimeInfo getTimeInfo() {
            return timeInfo;
        }
        

        protected SimpleEntry<Class<?>, ScheduledFuture<?>> apply(ScheduledExecutorService service) throws RecreateRunnableFailedException {
            Runnable runnable = recreateRunnable();
            Class<?> clazz = toTaskInfo().getUnderlyingClass();
            ScheduledFuture<?> future;
            if (timeInfo.getPeriod() > 0) {
                future = service.scheduleAtFixedRate(runnable, timeInfo.getInitialDelay(), timeInfo.getPeriod(), timeInfo.getUnit());
            } else if (timeInfo.getPeriod() < 0) {
                future = service.scheduleWithFixedDelay(runnable, timeInfo.getInitialDelay(), -timeInfo.getPeriod(), timeInfo.getUnit());
            } else  {
                if (Callable.class.isAssignableFrom(clazz)) {
                    Callable<?> callable = recreateCallable();
                    future = service.schedule(callable, timeInfo.getInitialDelay(), timeInfo.getUnit());
                } else {                    
                    future = service.schedule(runnable, timeInfo.getInitialDelay(), timeInfo.getUnit());
                }
            }
            return new SimpleEntry<Class<?>, ScheduledFuture<?>>(clazz, future);
        }

        private void setCompletedExceptionally() {
            this.flags |= FLAGS_COMPLETED_EXCEPTIONALLY;
        }

        private void setInterrupted() {
            this.flags |= FLAGS_INTERRUPTED;
        }

        private Runnable recreateRunnable() throws RecreateRunnableFailedException {
            if (serializableRunnable != null) {
                return serializableRunnable;
            } else {
                try {
                    return runnableClass.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    throw new RecreateRunnableFailedException(runnableClass);
                }
            }
        }
        
        @SuppressWarnings("rawtypes")
        private Callable<?> recreateCallable() throws RecreateRunnableFailedException {
            if (serializableRunnable instanceof AdaptSerializableCallable) {
                return ((AdaptSerializableCallable) serializableRunnable).callable;
            } else if (serializableRunnable instanceof AdaptSerializableCallableClass) {
                Class<? extends Callable> callableClass = ((AdaptSerializableCallableClass) serializableRunnable).clazz;
                try {
                    return callableClass.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    throw new RecreateRunnableFailedException(callableClass);
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }
        
        UnfinishedTasks.TaskInfo toTaskInfo() {
            return new UnfinishedTasks.TaskInfo() {
                @Override
                public Class<?> getUnderlyingClass() {
                    if (serializableRunnable != null) {
                        if (serializableRunnable instanceof AdaptSerializableCallable) {
                            return ((AdaptSerializableCallable) serializableRunnable).callable.getClass();
                        } else if (serializableRunnable instanceof AdaptSerializableCallableClass) {
                            return ((AdaptSerializableCallableClass) serializableRunnable).clazz;
                        } else {
                            return serializableRunnable.getClass();
                        }
                    } else {
                        return runnableClass;
                    }
                }

                @Override
                public SerializableRunnable getSerializableRunnable() {
                    return serializableRunnable;
                }
                
                @Override
                public SerializableCallable<?> getSerializableCallable() {
                    if (serializableRunnable != null) {
                        if (serializableRunnable instanceof AdaptSerializableCallable) {
                            return ((AdaptSerializableCallable) serializableRunnable).callable;
                        }
                    }
                    return null;
                }
                
                @Override
                public long getInitialDelayInNanos() {
                    return timeInfo.unit.toNanos(timeInfo.initialDelay);
                }
                
                @Override
                public boolean isPeriodic() {
                    return timeInfo.period != 0;
                }
                
                @Override
                public boolean isCompletedExceptionally() {
                    return (flags & FLAGS_COMPLETED_EXCEPTIONALLY) != 0;
                }
                
                @Override
                public boolean wasInterrupted() {
                    return (flags & FLAGS_INTERRUPTED) != 0;
                }
            };
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
        public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return future.get(timeout, unit);
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return future.getDelay(unit);
        }

        @Override
        public int compareTo(Delayed that) {
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
    
    
    protected static class UnfinishedTasksImpl implements UnfinishedTasks {
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
