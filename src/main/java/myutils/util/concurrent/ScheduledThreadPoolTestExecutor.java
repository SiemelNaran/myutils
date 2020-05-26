package myutils.util.concurrent;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import myutils.util.MultimapUtils;


/**
 * A scheduled executor service for testing.
 * In a real scheduled executor, scheduled tasks run when the system clock advances to the time when the task is scheduled to run.
 * In this test scheduled executor, scheduled tasks never run, but they run once advanceTime is called.
 * This allow us to write unit tests that run scheduled tasks immediately.
 */
public class ScheduledThreadPoolTestExecutor implements ScheduledExecutorService {
    private final ExecutorService realExecutor;
    private final SortedMap<Long /*millis*/, Collection<TestScheduledFutureTask<?>>> scheduledTasks = new TreeMap<>();
    private final List<TestScheduledFutureTask<?>> recurringTasksToReschedule = new ArrayList<>(); // TODO: do we need this variable?
    private final Lock recurringTasksToRescheduleLock = new ReentrantLock();
    private final ThreadLocal<Long> currentTimeMillis = new ThreadLocal<>();
    private long nowMillis;
    private boolean shutdown;
   
    /**
     * Create a scheduled executor service for testing.
     * 
     * @param corePoolSize the number of threads in the real executor used to actually run jobs
     * @param threadFactory the thread factory
     * @param startTime the initial time. Will typically be System.currentTimeMillis(), but can set to something else for unit tests.
     */
    public ScheduledThreadPoolTestExecutor(int corePoolSize, ThreadFactory threadFactory, long startTime) {
        realExecutor = Executors.newFixedThreadPool(corePoolSize, threadFactory);
        nowMillis = startTime;
    }

    // Overrides:
    
    @Override
    public synchronized void shutdown() {
        removePeriodicTasks();
        shutdown = true;
    }
    
    private void removePeriodicTasks() {
        for (var timeIter = this.scheduledTasks.entrySet().iterator(); timeIter.hasNext(); ) {
            var entry = timeIter.next();
            var tasks = entry.getValue();
            tasks.removeIf(TestScheduledFutureTask::isPeriodic);
            if (tasks.isEmpty()) {
                timeIter.remove();
            }
        }
    }
    
    @Override
    public synchronized List<Runnable> shutdownNow() {
        List<Runnable> scheduledNotStartedTasks =
                scheduledTasks.values().stream()
                                       .flatMap(listNextRunnable -> listNextRunnable.stream())
                                       .peek(TestScheduledFutureTask::clearExecutor)
                                       .collect(Collectors.toList());
        scheduledTasks.clear();
        List<Runnable> notStartedTasks = realExecutor.shutdownNow();
        notStartedTasks.addAll(scheduledNotStartedTasks);
        shutdown = true;
        return notStartedTasks;
    }

    @Override
    public boolean isShutdown() {
        return shutdown;
    }

    @Override
    public boolean isTerminated() {
        return shutdown && scheduledTasks.isEmpty() && realExecutor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        long startTime = System.nanoTime();
        advanceTimeWithException(timeout, unit, false);
        long timeTakenNanos = System.nanoTime() - startTime;
        boolean finishedAllTasks = scheduledTasks.isEmpty();
        if (!realExecutor.isShutdown()) {
            // shutdown() does not call realExecutor.shutdown() in order to let jobs already submitted to this executor service to run
            // so call it now
            realExecutor.shutdown();
        }
        finishedAllTasks &= realExecutor.awaitTermination(unit.toNanos(timeout) - timeTakenNanos, TimeUnit.NANOSECONDS);
        return finishedAllTasks;
    }

    // Overrides that forward to realExecutor:
    
    @Override
    public Future<?> submit(Runnable task) {
        return realExecutor.submit(task);
    }

    @Override
    public <T> Future<T> submit(Runnable task, T result) {
        return realExecutor.submit(task, result);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        return realExecutor.submit(task);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return realExecutor.invokeAll(tasks);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
        return realExecutor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return realExecutor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        return realExecutor.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(Runnable command) {
        realExecutor.execute(command);
    }
    
    // Overrides of schedule functions:

    @Override
    public synchronized ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        long delayMillis = unit.toMillis(delay);
        TestScheduledFutureTask<?> task = newTaskFor(command, delayMillis, 0);
        MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
        multimap.put(task.timeMillis, task);
        return task;
    }

    @Override
    public synchronized <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        long delayMillis = unit.toMillis(delay);
        TestScheduledFutureTask<V> task = newTaskFor(callable, delayMillis, 0);
        MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
        multimap.put(task.timeMillis, task);
        return task;
    }

    @Override
    public synchronized ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
        long initialDelayMillis = unit.toMillis(initialDelay);
        long periodMillis = unit.toMillis(period);
        TestScheduledFutureTask<?> task = newTaskFor(command, initialDelayMillis, periodMillis);
        MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
        multimap.put(task.timeMillis, task);
        return task;
    }

    @Override
    public synchronized ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
        long initialDelayMillis = unit.toMillis(initialDelay);
        long delayMillis = unit.toMillis(delay);
        TestScheduledFutureTask<?> task = newTaskFor(command, initialDelayMillis, -delayMillis);
        MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
        multimap.put(task.timeMillis, task);
        return task;
    }
    
    // Private functions:

    private TestScheduledFutureTask<Void> newTaskFor(Runnable runnable, long triggerTime, long period) {
        return new TestScheduledFutureTask<Void>(this, runnable, triggerTime, period);
    }
    
    private <T> TestScheduledFutureTask<T> newTaskFor(Callable<T> callable, long triggerTime, long period) {
        return new TestScheduledFutureTask<T>(this, callable, triggerTime, period);
    }
    
    private static class TestScheduledFutureTask<T> extends FutureTask<T> implements RunnableScheduledFuture<T>, Callable<Void> {
        private @Nullable ScheduledThreadPoolTestExecutor executor;
        private final long periodMillis; // zero means non recurring, positive means scheduleAtFixedRate, negative means scheduleWithFixedDelay
        private long timeMillis;
        
        private TestScheduledFutureTask(@Nonnull ScheduledThreadPoolTestExecutor executor, Runnable runnable, long triggerTimeMillis, long periodMillis) {
            super(runnable, null);
            this.executor = executor;
            this.periodMillis = periodMillis;
            this.timeMillis = executor.nowMillis + triggerTimeMillis;
        }

        private TestScheduledFutureTask(@Nonnull ScheduledThreadPoolTestExecutor executor, Callable<T> callable, long triggerTimeMillis, long periodMillis) {
            super(callable);
            this.executor = executor;
            this.periodMillis = periodMillis;
            this.timeMillis = executor.nowMillis + triggerTimeMillis;
        }
        
        private void clearExecutor() {
            executor = null;
        }
        
        @Override
        public long getDelay(TimeUnit unit) {
            if (executor == null) {
                return 0;
            }
            return unit.convert(timeMillis - executor.nowMillis, TimeUnit.MILLISECONDS);
        }

        @Override
        public int compareTo(Delayed that) {
            return Long.compare(this.getDelay(TimeUnit.MILLISECONDS), that.getDelay(TimeUnit.MILLISECONDS));
        }

        @Override
        public boolean isPeriodic() {
            return periodMillis != 0;
        }
        
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (executor != null) {
                executor.remove(this);
            }
            return cancelled;
        }

        @Override
        public void run() {
            if (executor != null) {
                runNormal();
            } else {
                runDetached();
            }
        }

        
        private void runNormal() {
            executor.setCurrentTimeMillis(timeMillis);
            if (!isPeriodic()) {
                super.run();
            } else {
                if (periodMillis > 0) {
                    if (super.runAndReset()) {
                        executor.move(this, timeMillis + periodMillis);
                    }
                } else {
                    long startTimeNanos = System.nanoTime();
                    if (super.runAndReset()) {
                        long timeTakenNanos = System.nanoTime() - startTimeNanos;
                        long timeTakenMillis = timeTakenNanos / 1_000_000;
                        executor.move(this, timeMillis + timeTakenMillis - periodMillis);
                    }
                }
            }
            executor.setCurrentTimeMillis(null);
        }
        
        private void runDetached() {
            super.run();
        }

        @Override
        public Void call() {
            run();
            return null;
        }
    }

    
    /**
     * User called scheduledFuture.cancel() so remove the future task from the scheduled task map.
     * This function is not called from TestScheduledFutureTask.run() so it should synchronize.
     */
    private synchronized boolean remove(TestScheduledFutureTask<?> task) {
        MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
        return multimap.remove(task.timeMillis, task);
    }

    /**
     * A period task just ran is is being rescheduled to run at a future time.
     * This function is initially called from advanceTime, so the task has already been removed from the tasks map.
     * This function is called from TestScheduledFutureTask.run() so do not synchronize as this would cause a deadlock.
     */
    private void move(TestScheduledFutureTask<?> task, long newTimeMillis) {
        recurringTasksToRescheduleLock.lock();
        try {
            task.timeMillis = newTimeMillis;
            recurringTasksToReschedule.add(task);
        } finally {
            recurringTasksToRescheduleLock.unlock();
        }
    }

    /**
     * Advance the time to the given time, executing all runnables up till the given time.
     * Blocks until all tasks to finish because that's probably what unit tests want.
     * 
     * @throws CompletionException if this thread is interrupted with the cause as the InterruptedException
     */
    public synchronized void advanceTime(long time, TimeUnit unit) {
        try {
            advanceTimeWithException(time, unit, true);
        } catch (InterruptedException e) {
            throw new CompletionException(e);
        }
    }
    
    private synchronized void advanceTimeWithException(long time, TimeUnit unit, boolean waitForever) throws InterruptedException {
        long timeMillis = unit.toMillis(time);
        nowMillis += timeMillis;
        recurringTasksToReschedule.clear();
        doAdvanceTime(waitForever ? null : System.currentTimeMillis() + unit.toMillis(time));
    }
    
    private void doAdvanceTime(final Long waitUntilMillis) throws InterruptedException {
        while (true) {
            Collection<TestScheduledFutureTask<?>> tasksToRun = extractAndClearTasksToRun();
            if (tasksToRun == null) {
                break;
            }
            try {
                if (waitUntilMillis == null) {
                    invokeAll(tasksToRun);
                } else {
                    long timeMillis = tasksToRun.iterator().next().timeMillis;
                    long waitMillis = waitUntilMillis - timeMillis;
                    System.out.println("snaran " + waitMillis);
                    invokeAll(tasksToRun, waitMillis, TimeUnit.MILLISECONDS);

                }
            } finally {
                addRecurringTasks();
            }
        }
    }

    /**
     * Return all tasks to run at the next scheduled time, or null if there are no tasks.
     */
    private @Nullable Collection<TestScheduledFutureTask<?>> extractAndClearTasksToRun() {
        var timeIter = scheduledTasks.entrySet().iterator();
        if (!timeIter.hasNext()) {
            return null;
        }
        var entry = timeIter.next();
        if (entry.getKey() > nowMillis) {
            return null;
        }
        Collection<TestScheduledFutureTask<?>> tasksToRun = entry.getValue();
        timeIter.remove();
        return tasksToRun;
    }

    private void addRecurringTasks() {
        recurringTasksToRescheduleLock.lock();
        try {
            if (recurringTasksToReschedule.isEmpty()) {
                return;
            }
            MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
            for (var task : recurringTasksToReschedule) {
                multimap.put(task.timeMillis, task);
            }
            recurringTasksToReschedule.clear();
        } finally {
            recurringTasksToRescheduleLock.unlock();
        }
    }

    private void setCurrentTimeMillis(Long timeMillis) {
        currentTimeMillis.set(timeMillis);
    }
    
    public long currentTimeMillis() {
        Long result = currentTimeMillis.get();
        if (result == null) {
            return nowMillis;
        }
        return result;
    }
}
