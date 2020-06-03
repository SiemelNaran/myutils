package myutils.util.concurrent;

import static java.lang.Math.abs;

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
    private final ExecutorService realExecutor;
    private final SortedMap<Long /*millis*/, Collection<TestScheduledFutureTask<?>>> scheduledTasks = new TreeMap<>();
    private final Lock taskFinishedLock = new ReentrantLock();
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
        realExecutor = Executors.newFixedThreadPool(corePoolSize, threadFactory);
        nowMillis = startTime;
    }

    // Overrides:

    /**
     * {@inheritDoc}
     * 
     * Cancels periodic tasks that have not started. Non periodic tasks will run at the next scheduled time.
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
        
        private boolean isRunning() {
            return realFuture != null;
        }

        private void setRealFuture(Future<?> realFuture) {
            this.realFuture = realFuture;
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
            executor.setCurrentTimeMillis(timeMillis);
            if (!isPeriodic()) {
                super.run();
            } else {
                if (periodMillis > 0) {
                    super.runAndReset();
                } else {
                    long startTimeNanos = System.nanoTime();
                    if (super.runAndReset()) {
                        long timeTakenNanos = System.nanoTime() - startTimeNanos;
                        long timeTakenMillis = timeTakenNanos / 1_000_000;
                        executor.addTimeTakenForScheduleWithFixedDelay(this, timeMillis - periodMillis, timeTakenMillis);
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
    public synchronized void advanceTime(long time, TimeUnit unit) {
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
    
    private synchronized AdvanceTimeResult advanceTimeWithException(long time, TimeUnit unit, boolean waitForever) throws InterruptedException {
        long originalNowMillis = nowMillis;
        taskFinishedLock.lock();
        try {
            long timeMillis = unit.toMillis(time);
            nowMillis += timeMillis;
        } finally {
            taskFinishedLock.unlock();
        }
        return doAdvanceTime(originalNowMillis, waitForever ? null : unit.toNanos(time));
    }
    
    private AdvanceTimeResult doAdvanceTime(final long originalNowMillis, final Long originalWaitNanos) throws InterruptedException {
        List<Future<?>> futures = new ArrayList<>(); // used to guard against spurious wakeup
        
        long timeOfLastFutureMillis = originalNowMillis;
        long nanosLeft = originalWaitNanos != null ? originalWaitNanos : Long.MAX_VALUE;
        
        while (nanosLeft > 0) {
            TasksToRun tasksToRun = extractAndClearTasksToRun();
            if (tasksToRun == null) {
                if (futures.isEmpty()) {
                    break;
                }
            } else {
                long timeMillis = tasksToRun.timeMillis;
                for (var task : tasksToRun.tasks) {
                    timeOfLastFutureMillis = timeMillis;
                    if (!task.isRunning()) {
                        task.timeMillis = timeMillis;
                        reschedulePeriodicTask(task);
                        var future = realExecutor.submit(task);
                        task.setRealFuture(future);
                        futures.add(future);
                    } else {
                        addBackPeriodicTaskThatIsStillRunning(task, timeMillis);
                    }
                }
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
     * Return all tasks to run at the next scheduled time, or null if there are no tasks.
     * The returned list will never be empty if it is not null.
     * Reschedules periodic tasks now so that ordering can be guaranteed.
     * 
     * Example:
     * - task A starts at 300ms, takes 60ms, and repeats every 400ms
     * - task B starts at 900ms, takes 60ms
     * - user calls advanceTime(1000ms)
     * - this function returns task A and also reschedules the next instance of task A as 700ms
     * - thus when task A finishes the next task pulled from the scheduledTasks tree map is tasks A
     */
    private @Nullable TasksToRun extractAndClearTasksToRun() {
        taskFinishedLock.lock();
        try {
            var timeIter = scheduledTasks.entrySet().iterator();
            if (!timeIter.hasNext()) {
                return null;
            }
            var entry = timeIter.next();
            long timeMillis = entry.getKey();
            if (timeMillis > nowMillis) {
                return null;
            }
            Collection<TestScheduledFutureTask<?>> tasksToRun = entry.getValue(); // will never be empty
            timeIter.remove();
            return new TasksToRun(timeMillis, tasksToRun);
        } finally {
            taskFinishedLock.unlock();
        }
    }
    
    private static class TasksToRun {
        private final long timeMillis;
        private final @Nonnull Collection<TestScheduledFutureTask<?>> tasks; // also not empty
        
        TasksToRun(long timeMillis, @Nonnull Collection<TestScheduledFutureTask<?>> tasks) {
            this.timeMillis = timeMillis;
            this.tasks = tasks;
        }
    }
    
    private void reschedulePeriodicTask(TestScheduledFutureTask<?> task) {
        if (shutdown) {
            return;
        }
        if (task.isPeriodic()) {
            long newTimeMillis = task.timeMillis + abs(task.periodMillis);
            MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
            multimap.put(newTimeMillis, task); // note that the nowTimeMillis is out of sync with task.timeMillis
        }
    }
    
    /**
     * We just finished a task that ran at earlier time, and the next task fetched by extractAndClearTasksToRun is one that is still running, so add it back to scheduledTasks.
     * 
     * Example:
     * - task A starts at 300ms, takes 60ms and repeats every 400ms
     * - task B starts at 300ms, takes 0ms
     * - user calls advanceTime(1000ms)
     * - task A is rescheduled to 700ms and runs
     * - task B runs
     * - when task B is finished is finds the next task to run is task A (at 700ms) and removes A from this.scheduledTasks,
     *       but since the instance of the task at 300ms is still running, add A at 700ms back to this.scheduledTasks
     */
    private void addBackPeriodicTaskThatIsStillRunning(TestScheduledFutureTask<?> task, long existingTimeMillis) {
        if (shutdown) {
            return;
        }
        if (task.isPeriodic()) {
            MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
            multimap.put(existingTimeMillis, task); // note that the existingTimeMillis is out of sync with task.timeMillis
        }
    }
    
    private boolean waitForSignal(List<Future<?>> futures) throws InterruptedException {
        taskFinishedLock.lock();
        try {
            boolean timedOut = false;
            while (!timedOut && futures.stream().noneMatch(Future::isDone)) {
                taskFinishedCondition.await();
            }
            return timedOut;
        } finally {
            taskFinishedLock.unlock();
        }
    }

    private long waitForSignal(List<Future<?>> futures, long timeLeft) throws InterruptedException {
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
    private boolean remove(TestScheduledFutureTask<?> task, long nextScheduledMillis) {
        taskFinishedLock.lock();
        try {
            MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
            return multimap.remove(nextScheduledMillis, task);
        } finally {
            taskFinishedLock.unlock();
        }
    }

    /**
     * A periodic task with fixed delay just ran.
     * The scheduledTasks map already contains the next instance of this task assuming it ran for 0ms.
     * Reschedule the task to add the time taken.
     * This function is called from TestScheduledFutureTask.run() so do not synchronize as this would cause a deadlock.
     */
    private void addTimeTakenForScheduleWithFixedDelay(TestScheduledFutureTask<?> task, long nextScheduledMillis, long timeTakenMillis) {
        taskFinishedLock.lock();
        try {
            if (!shutdown) {
                MultimapUtils<Long, TestScheduledFutureTask<?>> multimap = new MultimapUtils<>(scheduledTasks, ArrayList::new);
                multimap.remove(nextScheduledMillis, task);
                multimap.put(nextScheduledMillis + timeTakenMillis, task);
            }
        } finally {
            taskFinishedLock.unlock();
        }
    }
    
    /**
     * Notify the main loop that a task is done.
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
