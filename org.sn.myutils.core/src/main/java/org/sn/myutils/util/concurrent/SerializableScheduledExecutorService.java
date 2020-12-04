package org.sn.myutils.util.concurrent;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Stream;
import javax.annotation.Nonnull;


/**
 * Interface extending ScheduledExecutorService that allows the scheduled executions
 * to be serialized.
 * For example, you may add a shutdown hook that saves the executions to disk.
 * Upon startup, import the saved executions from disk.
 * 
 * <p>A task is serializable if it implements SerializableRunnable/SerializableCallable or
 * if the runnable/callable class has a public default constructor.
 */
public interface SerializableScheduledExecutorService extends ScheduledExecutorService {
    
    default UnfinishedTasks exportUnfinishedTasks() {
        return exportUnfinishedTasks(false);
    }
    
    
    /**
     * When called before shutdownNow is called, returns null.
     * When called after shutdownNow is called, return a list of unfinished tasks that are serializable.
     * 
     * <p>shutdownNow is modified to return the tasks that are not returned by exportUnfinishedTasks
     * -- i.e. should return the tasks that are not serializable.
     * 
     * <p>Cancelled tasks will be returned by shutdownNow if the remove on cancel policy is false (the default),
     * but cancelled tasks are not exported. 
     * 
     * @param includeExceptions include periodic tasks that ended with an exception
     * 
     * @return a serializable representation of the runnables that can be serialized.
     */
    UnfinishedTasks exportUnfinishedTasks(boolean includeExceptions);
    

    /**
     * Return the list of tasks that never commenced execution,
     * but not those that would be returned by exportUnfinishedTasks.
     * 
     * @return list of tasks
     */
    @Override
    @Nonnull List<Runnable> shutdownNow();
    
    
    default void importUnfinishedTasks(UnfinishedTasks unfinishedTasks) throws RecreateRunnableFailedException {
        importUnfinishedTasks(unfinishedTasks, Collections.emptyList());
    }
    

    /**
     * Import the UnfinishedTasks returned by the export function.
     * 
     * @param unfinishedTasks the tasks to import
     * @param returnFutures Return the ScheduledFuture's for these classes.
     * @return map of classes in returnFutures to a collection of futures of that type, in no sorted order
     * @throws RecreateRunnableFailedException if we failed to recreate the runnable
     */
    Map<Class<?>, List<ScheduledFuture<?>>> importUnfinishedTasks(UnfinishedTasks unfinishedTasks,
                                                                  Collection<Class<?>> returnFutures)
            throws RecreateRunnableFailedException;

    
    interface UnfinishedTasks extends Serializable {
        Stream<TaskInfo> stream();
    }

    @SuppressWarnings("checkstyle:SummaryJavadoc")
    interface TaskInfo {
        /**
         * @return Class<? extends Runnable> or Class<? extends Callable>
         */
        Class<?> getUnderlyingClass();

        /**
         * @return the serializable runnable, if any, or null.
         */
        SerializableRunnable getSerializableRunnable();

        /**
         * @return the serializable callable, if any, or null.
         */
        SerializableCallable<?> getSerializableCallable();

        /**
         * @return the time till the next execution of this task in nanoseconds.
         */
        long getInitialDelayInNanos();

        /**
         * @return true if the task is periodic.
         */
        boolean isPeriodic();

        /**
         * @return if this runnable ended with an exception
         */
        boolean isCompletedExceptionally();

        /**
         * @return if this runnable ended with an InterruptedException, implies isCompletedExceptionally() is also true
         */
        boolean wasInterrupted();
    }

    /**
     * Checked exception that is thrown when import fails.
     */
    class RecreateRunnableFailedException extends Exception {
        private static final long serialVersionUID = 1L;
        private static final String UNABLE_TO_RECREATE = "Unable to recreate ";

        private final List<Class<?>> listClass;

        RecreateRunnableFailedException(Class<?> clazz) {
            this.listClass = Collections.singletonList(clazz);
        }

        RecreateRunnableFailedException(@Nonnull List<Class<?>> clazzList) {
            this.listClass = Collections.unmodifiableList(clazzList);
        }

        @Override
        public String getMessage() {
            return UNABLE_TO_RECREATE + listClass;
        }

        /**
         * Return a list of class of runnable or callable.
         */
        public @Nonnull List<Class<?>> getFailedClasses() {
            return listClass;
        }
    }
}
