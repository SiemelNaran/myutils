package myutils.util.concurrent;

import static java.lang.Math.abs;
import static java.lang.Math.max;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
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
 * 
 * <p>It is expected that unit tests will create a TestScheduledThreadPoolExecutor and call advanceTime as needed,
 * whereas actual code will create a real ScheduledExecutorService. This could be accomplished for example with Guice.
 * 
 * <p>Test code could very well use a direct executor that runs tasks synchronously, so this class is only useful
 * for the cases where that approach does not work.
 */
public class TestScheduledThreadPoolExecutor implements ScheduledExecutorService {
    private final ThreadPoolExecutor realExecutor;
    private final SortedMap<Long /*millis*/, Collection<TestScheduledFutureTask<?>>> scheduledTasks = new TreeMap<>();
    private final Lock taskFinishedLock = new ReentrantLock(); // lock this when changing scheduledTasks (and function is not synchronized)
    private final Condition taskFinishedCondition = taskFinishedLock.newCondition();
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
    public TestScheduledThreadPoolExecutor(int corePoolSize, ThreadFactory threadFactory, long startTime) {
        realExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(corePoolSize, threadFactory);
        nowMillis = startTime;
    }

    // Overrides:

    /**
     * {@inheritDoc}
     * 
     * <p>Cancels periodic tasks that have not started. Non periodic tasks will run at the next scheduled time.
     */
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
    public synchronized @Nonnull List<Runnable> shutdownNow() {
        List<Runnable> scheduledNotStartedTasks =
                scheduledTasks.values().stream()
                                       .flatMap(Collection::stream)
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
    public boolean awaitTermination(long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        AdvanceTimeResult advanceTimeResult = advanceTimeWithException(timeout, unit, false);
        boolean finishedAllTasks = scheduledTasks.isEmpty();
        if (!realExecutor.isShutdown()) {
            // shutdown() does not call realExecutor.shutdown() in order to let jobs already submitted to this executor service to run
            // so call it now
            realExecutor.shutdown();
        }
        finishedAllTasks &= realExecutor.awaitTermination(advanceTimeResult.nanosLeft, TimeUnit.NANOSECONDS);
        return finishedAllTasks;
    }

    // Overrides that forward to realExecutor:
    
    @Override
    public @Nonnull Future<?> submit(@Nonnull Runnable task) {
        return realExecutor.submit(task);
    }

    @Override
    public @Nonnull <T> Future<T> submit(@Nonnull Runnable task, T result) {
        return realExecutor.submit(task, result);
    }

    @Override
    public @Nonnull <T> Future<T> submit(@Nonnull Callable<T> task) {
        return realExecutor.submit(task);
    }

    @Override
    public @Nonnull <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks) throws InterruptedException {
        return realExecutor.invokeAll(tasks);
    }

    @Override
    public @Nonnull <T> List<Future<T>> invokeAll(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit) throws InterruptedException {
        return realExecutor.invokeAll(tasks, timeout, unit);
    }

    @Override
    public @Nonnull <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
        return realExecutor.invokeAny(tasks);
    }

    @Override
    public <T> T invokeAny(@Nonnull Collection<? extends Callable<T>> tasks, long timeout, @Nonnull TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return realExecutor.invokeAny(tasks, timeout, unit);
    }

    @Override
    public void execute(@Nonnull Runnable command) {
        realExecutor.execute(command);
    }
    
    // Overrides of schedule functions:

    @Override
    public synchronized @Nonnull ScheduledFuture<?> schedule(@Nonnull Runnable command, long delay, @Nonnull TimeUnit unit) {
        long delayMillis = unit.toMillis(delay);
        TestScheduledFutureTask<?> task = newTaskFor(command, delayMillis, 0);
        MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
        multimap.put(task.timeMillis, task);
        return task;
    }

    @Override
    public synchronized @Nonnull <V> ScheduledFuture<V> schedule(@Nonnull Callable<V> callable, long delay, @Nonnull TimeUnit unit) {
        long delayMillis = unit.toMillis(delay);
        TestScheduledFutureTask<V> task = newTaskFor(callable, delayMillis);
        MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
        multimap.put(task.timeMillis, task);
        return task;
    }

    @Override
    public synchronized @Nonnull ScheduledFuture<?> scheduleAtFixedRate(@Nonnull Runnable command, long initialDelay, long period, @Nonnull TimeUnit unit) {
        long initialDelayMillis = unit.toMillis(initialDelay);
        long periodMillis = unit.toMillis(period);
        TestScheduledFutureTask<?> task = newTaskFor(command, initialDelayMillis, periodMillis);
        MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
        multimap.put(task.timeMillis, task);
        return task;
    }

    @Override
    public synchronized @Nonnull ScheduledFuture<?> scheduleWithFixedDelay(@Nonnull Runnable command, long initialDelay, long delay, @Nonnull TimeUnit unit) {
        long initialDelayMillis = unit.toMillis(initialDelay);
        long delayMillis = unit.toMillis(delay);
        TestScheduledFutureTask<?> task = newTaskFor(command, initialDelayMillis, -delayMillis);
        MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
        multimap.put(task.timeMillis, task);
        return task;
    }
    
    // Private functions:

    private @Nonnull TestScheduledFutureTask<Void> newTaskFor(Runnable runnable, long triggerTime, long period) {
        return new TestScheduledFutureTask<>(this, runnable, triggerTime, period);
    }
    
    private @Nonnull <T> TestScheduledFutureTask<T> newTaskFor(Callable<T> callable, long triggerTime) {
        return new TestScheduledFutureTask<>(this, callable, triggerTime, 0);
    }
    
    private static class TestScheduledFutureTask<T> extends FutureTask<T> implements RunnableScheduledFuture<T> {
        private @Nullable TestScheduledThreadPoolExecutor executor;
        private final long periodMillis; // zero means non recurring, positive means scheduleAtFixedRate, negative means scheduleWithFixedDelay
        private long timeMillis;
        private volatile Future<?> realFuture;
        
        private TestScheduledFutureTask(@Nonnull TestScheduledThreadPoolExecutor executor, Runnable runnable, long triggerTimeMillis, long periodMillis) {
            super(runnable, null);
            this.executor = executor;
            this.periodMillis = periodMillis;
            this.timeMillis = executor.nowMillis + triggerTimeMillis;
        }

        private TestScheduledFutureTask(@Nonnull TestScheduledThreadPoolExecutor executor, Callable<T> callable, long triggerTimeMillis, long periodMillis) {
            super(callable);
            this.executor = executor;
            this.periodMillis = periodMillis;
            this.timeMillis = executor.nowMillis + triggerTimeMillis;
        }
        
        private void setRealFuture(Future<?> realFuture) {
            this.realFuture = realFuture;
        }

        private void clearExecutor() {
            executor = null;
        }
        
        @Override
        public long getDelay(@Nonnull TimeUnit unit) {
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
            if (realFuture != null) {
                cancelled &= realFuture.cancel(mayInterruptIfRunning);
            }
            if (executor != null) {
                executor.remove(this, timeMillis + abs(periodMillis));
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
            assert executor != null; // to avoid IntelliJ warning about NullPointerException
            executor.setCurrentTimeMillis(timeMillis);
            if (!isPeriodic()) {
                super.run();
            } else {
                long realStartNanos = System.nanoTime();
                if (periodMillis > 0) {
                    if (super.runAndReset()) {
                        long timeTakenNanos = System.nanoTime() - realStartNanos;
                        long timeTakenMillis = TimeUnit.NANOSECONDS.toMillis(timeTakenNanos); 
                        long nextScheduledMillis = timeMillis + (timeTakenMillis > periodMillis ? timeTakenMillis : periodMillis);
                        executor.reschedule(this, nextScheduledMillis);
                    }
                } else {
                    if (super.runAndReset()) {
                        long timeTakenNanos = System.nanoTime() - realStartNanos;
                        long timeTakenMillis = TimeUnit.NANOSECONDS.toMillis(timeTakenNanos); 
                        long nextScheduledMillis = timeMillis - periodMillis + timeTakenMillis;
                        executor.reschedule(this, nextScheduledMillis);
                    }
                }
            }
            realFuture = null;
            executor.setCurrentTimeMillis(null);
            executor.signalMainLoopInAdvanceTime();
        }
        
        /**
         * The purpose of this function is to run the runnable when the user calls shutdownNow and then runs the runnables
         * in the list returned by shutdownNow.
         */
        private void runDetached() {
            super.run();
        }
    }

    /**
     * Advance the time to the given time, executing all runnables up till the given time.
     * Blocks until all tasks to finish because that's probably what unit tests want.
     * 
     * @throws CompletionException if this thread is interrupted with the cause as the InterruptedException
     */
    public synchronized void advanceTime(long time, @Nonnull TimeUnit unit) {
        try {
            advanceTimeWithException(time, unit, true);
        } catch (InterruptedException e) {
            throw new CompletionException(e);
        }
    }
    
    private static class AdvanceTimeResult {
        private final long nanosLeft;
        
        AdvanceTimeResult(long nanosLeft) {
            this.nanosLeft = nanosLeft;
        }
    }
    
    private synchronized AdvanceTimeResult advanceTimeWithException(long time, @Nonnull TimeUnit unit, boolean waitForever) throws InterruptedException {
        long originalNowMillis = nowMillis;
        long timeMillis = unit.toMillis(time);
        nowMillis += timeMillis;
        return doAdvanceTime(originalNowMillis, waitForever ? null : unit.toNanos(time));
    }
    
    private AdvanceTimeResult doAdvanceTime(final long originalNowMillis, final Long originalWaitNanos) throws InterruptedException {
        List<Future<?>> futures = new ArrayList<>(); // used to guard against spurious wakeup
        
        int corePoolSize = realExecutor.getCorePoolSize();
        long timeOfLastFutureMillis = originalNowMillis;
        long nanosLeft = originalWaitNanos != null ? originalWaitNanos : Long.MAX_VALUE;
        
        while (nanosLeft > 0) {
            Collection<TestScheduledFutureTask<?>> tasksToRun = extractAndClearTasksToRun(max(corePoolSize - futures.size(), 0));
            if (tasksToRun.isEmpty()) {
                if (futures.isEmpty()) {
                    break;
                }
            }
            for (var task : tasksToRun) {
                var timeMillis = task.timeMillis;
                timeOfLastFutureMillis = timeMillis;
                var future = realExecutor.submit(task);
                task.setRealFuture(future);
                futures.add(future);
            }
            if (originalWaitNanos == null) {
                waitForSignal(futures);
            } else {
                long offsetMillis = timeOfLastFutureMillis - originalNowMillis;
                nanosLeft = originalWaitNanos - TimeUnit.MILLISECONDS.toNanos(offsetMillis); 
                nanosLeft = waitForSignal(futures, nanosLeft);
            }
            futures.removeIf(Future::isDone);
        }
        
        return new AdvanceTimeResult(nanosLeft);
    }


    /**
     * Return all tasks to run at the next scheduled times.
     * 
     * @param numTasks maximum number of tasks to retrieve.
     */
    private @Nonnull Collection<TestScheduledFutureTask<?>> extractAndClearTasksToRun(int numTasks) {
        taskFinishedLock.lock();
        try {
            Collection<TestScheduledFutureTask<?>> tasksToRun = new ArrayList<>();
            boolean done = numTasks == 0;
            for (var timeIter = scheduledTasks.entrySet().iterator(); !done && timeIter.hasNext(); ) {
                var entry = timeIter.next();
                long timeMillis = entry.getKey();
                if (timeMillis > nowMillis) {
                    break;
                }
                Collection<TestScheduledFutureTask<?>> tasks = entry.getValue(); // will never be empty
                for (var taskIter = tasks.iterator(); !done && taskIter.hasNext(); ) {
                    var task = taskIter.next();
                    tasksToRun.add(task);
                    taskIter.remove();
                    if (tasksToRun.size() == numTasks) {
                        done = true;
                    }
                }
                if (tasks.isEmpty()) {
                    timeIter.remove();
                }
            }
            return tasksToRun;
        } finally {
            taskFinishedLock.unlock();
        }
    }
    
    private void waitForSignal(@Nonnull List<Future<?>> futures) throws InterruptedException {
        taskFinishedLock.lock();
        try {
            while (futures.stream().noneMatch(Future::isDone)) {
                taskFinishedCondition.await();
            }
        } finally {
            taskFinishedLock.unlock();
        }
    }

    private long waitForSignal(@Nonnull List<Future<?>> futures, long timeLeft) throws InterruptedException {
        taskFinishedLock.lock();
        try {
            while (timeLeft > 0 && futures.stream().noneMatch(Future::isDone)) {
                timeLeft = taskFinishedCondition.awaitNanos(timeLeft);
            }
            return timeLeft;
        } finally {
            taskFinishedLock.unlock();
        }
    }

    /**
     * User called scheduledFuture.cancel() so remove the future task from the scheduled task map.
     */
    private void remove(@Nonnull TestScheduledFutureTask<?> task, long nextScheduledMillis) {
        taskFinishedLock.lock();
        try {
            MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
            multimap.remove(nextScheduledMillis, task);
        } finally {
            taskFinishedLock.unlock();
        }
    }

    /**
     * A periodic task just finished, so reschedule it to run at a later time.
     * This function is called from TestScheduledFutureTask.run() so do not synchronize as this would cause a deadlock.
     */
    private void reschedule(TestScheduledFutureTask<?> task, long nextScheduledMillis) {
        taskFinishedLock.lock();
        try {
            if (!shutdown) {
                MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
                task.timeMillis = nextScheduledMillis;
                multimap.put(nextScheduledMillis, task);
            }
        } finally {
            taskFinishedLock.unlock();
        }
    }
    
    /**
     * Notify the main loop in doAdvanceTime that a task is done.
     */
    private void signalMainLoopInAdvanceTime() {
        taskFinishedLock.lock();
        try {
            taskFinishedCondition.signal();
        } finally {
            taskFinishedLock.unlock();
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
