package org.sn.myutils.util.concurrent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.AbstractMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.sn.myutils.util.concurrent.SerializableScheduledExecutorService.RecreateRunnableFailedException;
import org.sn.myutils.util.concurrent.SerializableScheduledExecutorService.TaskInfo;


class SerializableLambdaUtils {
    private static final System.Logger LOGGER = System.getLogger(SerializableLambdaUtils.class.getName());

    static final class TimeInfo implements Serializable {
        private static final long serialVersionUID = 1L;

        private long initialDelay;
        private final long period;
        private TimeUnit unit;

        private TimeInfo(long initialDelay, long period, TimeUnit unit) {
            this.initialDelay = initialDelay;
            this.period = period;
            this.unit = unit;
        }

        long getInitialDelay() {
            return initialDelay;
        }

        long getInitialDelayMillis() {
            return unit.toMillis(initialDelay);
        }

        /**
         * Return the period.
         * A positive value means scheduleAtFixedRate.
         * A negative value means scheduleWithFixedDelay.
         */
        long getPeriod() {
            return period;
        }

        TimeUnit getUnit() {
            return unit;
        }

        /**
         * Reset the initial delay.
         * For example, when you call shutdownNow on a serialized scheduled executor,
         * the unfinished task's initial delay is reset to be from the time of shutdown.
         */
        void setInitialDelay(long initialDelay) {
            this.initialDelay = initialDelay;
        }

        void setInitialDelay(long initialDelay, TimeUnit unit) {
            this.initialDelay = initialDelay;
            this.unit = unit;
        }
    }

    static class RunnableInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        private static final byte FLAGS_COMPLETED_EXCEPTIONALLY = 1;
        private static final byte FLAGS_INTERRUPTED = 2;

        private final TimeInfo timeInfo;
        private final SerializableRunnable serializableRunnable;
        private final Class<? extends Runnable> runnableClass;
        private byte flags;

        RunnableInfo(TimeInfo info, SerializableRunnable runnable) {
            this.timeInfo = info;
            this.serializableRunnable = runnable;
            this.runnableClass = null;
        }

        RunnableInfo(TimeInfo info, Class<? extends Runnable> runnableClass) {
            this.timeInfo = info;
            this.serializableRunnable = null;
            this.runnableClass = runnableClass;
        }

        TimeInfo getTimeInfo() {
            return timeInfo;
        }

        /**
         * Add this RunnableInfo to a scheduled service executor.
         * In other words, call service.schedule, service.scheduleAtFixedRate, or service.scheduleWithFixedDelay.
         *
         * @return a tuple of the serializable class and the scheduled future.
         */
        AbstractMap.SimpleEntry<Class<?>, RunnableScheduledFuture<?>> apply(ScheduledExecutorService service) throws RecreateRunnableFailedException {
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
            return new AbstractMap.SimpleEntry<>(clazz, (RunnableScheduledFuture<?>) future);
        }

        void setCompletedExceptionally() {
            this.flags |= FLAGS_COMPLETED_EXCEPTIONALLY;
        }

        void setInterrupted() {
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
            if (serializableRunnable instanceof AdaptSerializableCallable adapt) {
                return adapt.callable;
            } else if (serializableRunnable instanceof AdaptSerializableCallableClass adapt) {
                Class<? extends Callable> callableClass = adapt.clazz;
                try {
                    return callableClass.getDeclaredConstructor().newInstance();
                } catch (Exception e) {
                    throw new RecreateRunnableFailedException(callableClass);
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }

        TaskInfo toTaskInfo() {
            return new TaskInfo() {
                @Override
                public Class<?> getUnderlyingClass() {
                    if (serializableRunnable != null) {
                        if (serializableRunnable instanceof AdaptSerializableCallable adapt) {
                            return adapt.callable.getClass();
                        } else if (serializableRunnable instanceof AdaptSerializableCallableClass adapt) {
                            return adapt.clazz;
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
                        if (serializableRunnable instanceof AdaptSerializableCallable adapt) {
                            return adapt.callable;
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

        public byte[] toBytes() throws IOException {
            var bos = new ByteArrayOutputStream();
            try (var oos = new ObjectOutputStream(bos)) {
                oos.writeObject(this);
                return bos.toByteArray();
            }
        }

        static RunnableInfo fromBytes(byte[] array) throws IOException {
            var bis = new ByteArrayInputStream(array);
            try (var ois = new ObjectInputStream(bis)) {
                return (RunnableInfo) ois.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }
        }
    }

    static RunnableInfo computeRunnableInfo(Runnable command, long initialDelay, long period, TimeUnit unit, System.Logger.Level logIfCannotSerializeLevel) {
        if (command instanceof SerializableRunnable serializableRunnable) {
            return new RunnableInfo(new TimeInfo(initialDelay, period, unit), serializableRunnable);
        } else {
            Class<? extends Runnable> clazz = command.getClass();
            if (!clazz.isLocalClass()) {
                if (!clazz.isSynthetic()) {
                    try {
                        clazz.getConstructor();
                        return new RunnableInfo(new TimeInfo(initialDelay, period, unit), clazz);
                    } catch (NoSuchMethodException | SecurityException e) {
                        LOGGER.log(logIfCannotSerializeLevel, "Cannot serialize " + clazz.getName() + " - no public constructor");
                    }
                } else {
                    LOGGER.log(logIfCannotSerializeLevel, "Cannot serialize " + clazz.getName() + " - synthetic class");
                }
            } else {
                LOGGER.log(logIfCannotSerializeLevel, "Cannot serialize " + clazz.getName() + " - local class");
            }
        }
        return null;
    }

    @SuppressWarnings("rawtypes")
    static <V> RunnableInfo computeRunnableInfo(Callable<V> callable, long initialDelay, TimeUnit unit, System.Logger.Level logIfCannotSerializeLevel) {
        if (callable instanceof SerializableCallable<V> serializableCallable) {
            SerializableRunnable command = new AdaptSerializableCallable(serializableCallable);
            return new RunnableInfo(new TimeInfo(initialDelay, 0, unit), command);
        } else {
            Class<? extends Callable> clazz = callable.getClass();
            if (!clazz.isLocalClass()) {
                if (!clazz.isSynthetic()) {
                    try {
                        clazz.getConstructor();
                        SerializableRunnable command = new AdaptSerializableCallableClass(clazz);
                        return new RunnableInfo(new TimeInfo(initialDelay, 0, unit), command);
                    } catch (NoSuchMethodException | SecurityException e) {
                        LOGGER.log(logIfCannotSerializeLevel, "Cannot serialize " + clazz.getName() + " - no public constructor");
                    }
                } else {
                    LOGGER.log(logIfCannotSerializeLevel, "Cannot serialize " + clazz.getName() + " - synthetic class");
                }
            } else {
                LOGGER.log(logIfCannotSerializeLevel, "Cannot serialize " + clazz.getName() + " - local class");
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

        AdaptSerializableCallableClass(Class<? extends Callable> clazz) {
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
}
