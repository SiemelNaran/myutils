package myutils.util.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;


public class PriorityLockTest {
    long startOfTime;
    
    @BeforeEach
    void setStartOfTime(TestInfo testInfo) {
        startOfTime = System.currentTimeMillis();
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("test started: " + testInfo.getDisplayName());
    }
    
    @AfterEach
    void printTestFinished(TestInfo testInfo) {
        System.out.println("test finished: " + testInfo.getDisplayName());
    }
    
    @AfterAll
    static void printAllTestsFinished() {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("all tests finished");
    }
    
    /**
     * Test min and max thread priority.
     * The purpose of this test is to guard against Java changing max priority to some huge number,
     * whence we the PriorityLock would have too many Condition objects.
     */
    @Test
    void testMinAndMaxPriority() {
        assertEquals(1, Thread.MIN_PRIORITY);
        assertEquals(10, Thread.MAX_PRIORITY);
    }

    @Test
    void testToString() {
        PriorityLock priorityLock = new PriorityLock();
        priorityLock.lock();
        System.out.println(priorityLock.toString());
        assertThat(priorityLock.toString(), Matchers.matchesRegex("^java.util.concurrent.locks.ReentrantLock@[a-f0-9]+\\[Locked by thread main\\]$"));
    }

    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * As for the rest, the thread with the highest priority runs first, and so on, till the thread with the lowest priority runs.
     */
    @Test
    void testEnoughThreads() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThreadLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7); }, 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "end thread with priority 4",
                           "end thread with priority 7",
                           "end thread with priority 6",
                           "end thread with priority 6",
                           "end thread with priority 5",
                           "end thread with priority 5"));
    }


    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * One thread with a lower priority that is waiting on a thread with higher priority is cancelled.
     * Verify that the threads waiting on the canceled thread finish.
     */
    @Test
    void testEnoughThreadsWithCancellation() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThreadLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 400, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future500 =
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7); }, 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future500 of priority 6");
            future500.cancel(true);
        }, 700, TimeUnit.MILLISECONDS); 

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "end thread with priority 4",
                           "end thread with priority 7",
                           "thread with priority 6 encountered exception java.util.concurrent.CancellationException",
                           "end thread with priority 6",
                           "end thread with priority 5",
                           "end thread with priority 5"));
    }


    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * But the executor running the threads only supports 3 threads at a time,
     * so the thread with the highest priority does not run next, because even though it is in the scheduled executor's queue,
     * it hasn't started running yet so its presence is not known to the running threads,
     * and thus the threads with lower priorities will not wait for it to finish.
     * The test is an explanation of the order in which the tests run.
     */
    @Test
    void testNotEnoughThreads() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThreadLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7); }, 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue, but only 2 of them run
                           // running: [priority 4 ,priority 5, priority 6] priority 4 has lock
                           "end thread with priority 4",
                           // running: [priority 5, priority 6, priority 5] priority 6 has lock
                           "end thread with priority 6",
                           // running: [priority 5, priority 5, priority 6] priority 5 has lock because it starts before priority 6 moved from queue to active thread
                           "end thread with priority 5",
                           // running: [priority 5, priority 6, priority 7] priority 6 has lock because it starts before priority 7 moved from queue to active thread
                           "end thread with priority 6",
                           // running: [priority 5, priority 7] priority 7 has lock
                           "end thread with priority 7",
                           // running: [priority 5]
                           "end thread with priority 5"));
    }


    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.o
     * As for the rest, the thread with the highest priority runs first, and so on, till the thread with the lowest priority runs.
     * There is one new catch: While the thread with the highest priority is running we change its priority.
     * This should not break other threads which are waiting on it to finish.
     */
    @Test
    void testChangingThreadPriority() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThreadLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7, 1, null); }, 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains("end thread with priority 4", // thread with priority 4 runs first, and while it is running all others get added to the queue
                                     "end thread with priority 1",
                                     "end thread with priority 6",
                                     "end thread with priority 6",
                                     "end thread with priority 5",
                                     "end thread with priority 5"));
    }


    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * There is another thread that cancels the thread with the highest priority --
     * i.e. it receives an InterruptedExcedption while in ReentrantLock::lockInterruptibly.
     * So it does not run, and all the other threads run in order.
     */
    @Test
    void testExceptionWhileAcquireLockInterruptibly1() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThreadLockInterruptibly(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future600 =
        executor.schedule(() -> { doThread.action(7); }, 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread of priority 7");
            future600.cancel(true);
        }, 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "thread with priority 7 encountered exception java.lang.InterruptedException",
                           "end thread with priority 4",
                           "end thread with priority 6",
                           "end thread with priority 6",
                           "end thread with priority 5",
                           "end thread with priority 5"));
    }


    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * There is another thread that cancels all threads with less than the highest priority --
     * i.e. they receive an InterruptedExcedption while in Condition::await.
     * So only the first two threads run.
     */
    @Test
    void testExceptionWhileAcquireLockInterruptibly2() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = new DoThreadLockInterruptibly(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future200 =
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future300 =
        executor.schedule(() -> { doThread.action(6); }, 300, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future400 =
        executor.schedule(() -> { doThread.action(5); }, 400, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future500 =
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7); }, 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread of priority 5");
            future200.cancel(true);
            logString("about to interrupt thread of priority 6");
            future300.cancel(true);
            logString("about to interrupt thread of priority 5");
            future400.cancel(true);
            logString("about to interrupt thread of priority 6");
            future500.cancel(true);
        }, 1500, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        // TODO: the interrupted stuff occurs in different order each time
        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "end thread with priority 4",
                           "end thread with priority 7",
                           "thread with priority 5 encountered exception java.lang.InterruptedException",
                           "thread with priority 6 encountered exception java.lang.InterruptedException",
                           "thread with priority 5 encountered exception java.lang.InterruptedException",
                           "thread with priority 6 encountered exception java.lang.InterruptedException"));
    }


    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * Of the 4 threads that start at time 1200ms, only one of them gets to run.
     * Long after there is a 6th thread that runs, but it's the only thread running at the time so it goes ahead. 
     */
    @Test
    void testTryLock() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(new ThrowAtPrioritySevenReentrantLock(true));

        DoThread doThread = new DoThreadTryLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(8); }, 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(9); }, 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(1); }, 2400, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        // thread with priority 4 runs first, and while it is running all others get added to the queue
                        "end thread with priority 4",
                        "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "thread with priority 8 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "end thread with priority 9",
                        "end thread with priority 1"));
    }


    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * The thread with the highest priority goes next,
     * then the thread with the next highest priority,
     * but the other threads time out waiting for a lock.
     * Long after there is a 6th thread that runs, but it's the only thread running at the time so it goes ahead. 
     */
    @Test
    void testTryLockWithArgs() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThreadTryLockWithArgs(priorityLock, 2000);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 300, TimeUnit.MILLISECONDS); // will wait till 2300, never runs as lock only available at 3200
        executor.schedule(() -> { doThread.action(6); }, 400, TimeUnit.MILLISECONDS); // will wait till 2400, never runs as lock only available at 3200
        executor.schedule(() -> { doThread.action(7); }, 500, TimeUnit.MILLISECONDS); // will wait till 2500, but (b) gets lock at 2100
        executor.schedule(() -> { doThread.action(8); }, 600, TimeUnit.MILLISECONDS); // will wait till 2600, but (a) gets lock at 1100 
        executor.schedule(() -> { doThread.action(1); }, 3800, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        // thread with priority 4 runs first, and while it is running all others get added to the queue
                        "end thread with priority 4",
                        "end thread with priority 8",
                        "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "end thread with priority 7",
                        "end thread with priority 1"));
    }


    /**
     * This test starts 3 threads with different priorities, and acquiring a lock on the first thread throws.
     * The test verifies that the second thread still runs.
     */
    @Test
    void testLockException() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(new ThrowAtPrioritySevenReentrantLock(false));

        DoThread doThread = new DoThreadLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7); }, 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 1300, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "end thread with priority 4",
                           "thread with priority 7 encountered exception java.lang.IllegalArgumentException: priority 7 not allowed",
                           "end thread with priority 6"));
    }


    /**
     * This test starts 3 threads with different priorities, and acquiring a lock on the first thread throws.
     * The test verifies that the second thread still runs.
     */
    @Test
    void testLockInterruptiblyException() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(new ThrowAtPrioritySevenReentrantLock(false));

        DoThread doThread = new DoThreadLockInterruptibly(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7); }, 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 1300, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "end thread with priority 4",
                           "thread with priority 7 encountered exception java.lang.IllegalArgumentException: priority 7 not allowed",
                           "end thread with priority 6"));
    }


    /**
     * This test starts 3 threads with different priorities, and acquiring a lock on the first thread throws.
     * The test verifies that the second thread still runs.
     */
    @Test
    void testTryLockWithException() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(new ThrowAtPrioritySevenReentrantLock(false));

        DoThread doThread = new DoThreadTryLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7); }, 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 1300, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "end thread with priority 4",
                           "thread with priority 7 encountered exception java.lang.IllegalArgumentException: priority 7 not allowed",
                           "end thread with priority 6"));
    }


    /**
     * This test starts 3 threads with different priorities, and acquiring a lock on the first thread throws.
     * The test verifies that the second thread still runs.
     */
    @Test
    void testTryLockWithArgsWithException() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(new ThrowAtPrioritySevenReentrantLock(false));

        DoThread doThread = new DoThreadTryLockWithArgs(priorityLock, 2000);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7); }, 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 1300, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "end thread with priority 4",
                           "thread with priority 7 encountered exception java.lang.IllegalArgumentException: priority 7 not allowed",
                           "end thread with priority 6"));
    }


    /**
     * This test sets up 5 threads waiting on a condition.
     * A 6th threads has the lowest priority and calls signal.
     * The test verifies that the thread with the highest priority wins.
     */
    @Test
    void testCondition() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = new DoThreadLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.awaitAction(4, false); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.awaitAction(5, false); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.awaitAction(6, false); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.awaitAction(7, false); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.awaitAction(8, false); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6, 1, Signal.SIGNAL_ALL); }, 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           "end thread with priority 1",
                           "end thread with priority 8",
                           "end thread with priority 7",
                           "end thread with priority 6",
                           "end thread with priority 5",
                           "end thread with priority 4"));
    }

    
    /**
     * This test sets up 3 threads waiting on a condition.
     * A 4th threads has the lowest priority and calls signal.
     * The test verifies that the thread with the highest priority wins.
     * Difference with the previous test is that the thread sleeps for 1 second before calling await,
     * so this test proves that the call to await from removes the thread's priority from the wait tree so that lower priority tasks can finish.
     */
    @Test
    void testConditionWithSleep() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = new DoThreadLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(4, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.awaitAction(4, true); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.awaitAction(5, true); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.awaitAction(6, true); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(3, 1, Signal.SIGNAL_ALL); }, 400, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           "end thread with priority 1",
                           "end thread with priority 6",
                           "end thread with priority 5",
                           "end thread with priority 4"));
    }

    
    private enum Signal {
        SIGNAL_ALL
    };

    private abstract class DoThread {
        final PriorityLock priorityLock;
        final Condition condition;
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

        private DoThread(PriorityLock priorityLock) {
            this.priorityLock = priorityLock;
            this.condition = priorityLock.newCondition();
        }

        void action(int priority) {
            action(priority, null, null);
        }
        
        void awaitAction(int priority, boolean shouldSleep) {
            awaitAction(priority, shouldSleep, null);
        }
        
        void action(int priority, Integer newPriority, Signal signal) {
            final Thread currentThread = Thread.currentThread();
            currentThread.setPriority(priority);
            logString("start");
            try {
                getLock();
                try {
                    logString("acquired lock");
                    sleep(1000);
                    if (newPriority != null) {
                        logString("changing priority of thread from " + currentThread.getPriority() + " to " + newPriority);
                        currentThread.setPriority(newPriority);
                    }
                    if (signal == Signal.SIGNAL_ALL) {
                        logString("about to call condition.signalAll");
                        condition.signalAll();
                    }
                    logString("end");
                    messages.add("end thread with priority " + currentThread.getPriority());
                } finally {
                    priorityLock.unlock();
                }
            } catch (InterruptedException | RuntimeException | Error e) {
                logString("caught exception " + e.toString());
                messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
            }
        }
        
        void awaitAction(int priority, boolean shouldSleep, Integer newPriority) {
            final Thread currentThread = Thread.currentThread();
            currentThread.setPriority(priority);
            logString("start");
            try {
                getLock();
                try {
                    logString("acquired lock");
                    if (shouldSleep) {
                        sleep(1000);
                    }
                    if (newPriority != null) {
                        logString("changing priority of thread from " + currentThread.getPriority() + " to " + newPriority);
                        currentThread.setPriority(newPriority);
                    }
                    logString("waiting on condition");
                    doAwait();
                    logString("end");
                    messages.add("end thread with priority " + currentThread.getPriority());
                } finally {
                    priorityLock.unlock();
                }
            } catch (InterruptedException | RuntimeException e) {
                logString("caught exception " + e.toString());
                messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
            }
        }
        
        abstract void getLock() throws InterruptedException;
        
        abstract void doAwait() throws InterruptedException;
        
        List<String> getMessages() {
            return messages;
        }
    }
    
    private class DoThreadLock extends DoThread {
        private DoThreadLock(PriorityLock priorityLock) {
            super(priorityLock);
        }
        
        @Override
        void getLock() {
            priorityLock.lock();
        }
        
        @Override
        void doAwait() throws InterruptedException {
            condition.awaitUninterruptibly();
        }
    }

    private class DoThreadLockInterruptibly extends DoThread {
        private DoThreadLockInterruptibly(PriorityLock priorityLock) {
            super(priorityLock);
        }
        
        @Override
        void getLock() throws InterruptedException {
            priorityLock.lockInterruptibly();
        }
        
        @Override
        void doAwait() throws InterruptedException {
            condition.await();
        }
    }

    private class DoThreadTryLock extends DoThread {
        private DoThreadTryLock(PriorityLock priorityLock) {
            super(priorityLock);
        }
        
        @Override
        void getLock() {
            boolean acquired = priorityLock.tryLock();
            if (!acquired) {
                throw new FailedToAcquireLockException();
            }
        }
        
        @Override
        void doAwait() throws InterruptedException {
            boolean failedToAcquire = condition.await(0, TimeUnit.MILLISECONDS);
            if (failedToAcquire) {
                throw new FailedToAcquireLockException();
            }
        }
    }

    private class DoThreadTryLockWithArgs extends DoThread {
        private final long waitTimeMillis;

        private DoThreadTryLockWithArgs(PriorityLock priorityLock, long waitTime) {
            super(priorityLock);
            this.waitTimeMillis = waitTime;
        }
        
        @Override
        void getLock() throws InterruptedException {
            boolean acquired = priorityLock.tryLock(waitTimeMillis, TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new FailedToAcquireLockException();
            }
        }
        
        @Override
        void doAwait() throws InterruptedException {
            boolean failedToAcquire = condition.await(waitTimeMillis, TimeUnit.MILLISECONDS);
            if (failedToAcquire) {
                throw new FailedToAcquireLockException();
            }
        }
    }
    
    
    private static class FailedToAcquireLockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
    
    
    private static class ThrowAtPrioritySevenReentrantLock extends ReentrantLock {
        private static final long serialVersionUID = 1L;

        ThrowAtPrioritySevenReentrantLock(boolean fair) {
            super(fair);
        }
        
        @Override
        public void lock() {
            sleep(50);
            if (Thread.currentThread().getPriority() == 7) {
                throw new IllegalArgumentException("priority 7 not allowed");
            }
            super.lock();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            sleep(50);
            if (Thread.currentThread().getPriority() == 7) {
                throw new IllegalArgumentException("priority 7 not allowed");
            }
            super.lockInterruptibly();;
        }

        @Override
        public boolean tryLock() {
            sleep(50);
            if (Thread.currentThread().getPriority() == 7) {
                throw new IllegalArgumentException("priority 7 not allowed");
            }
            return super.tryLock();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            sleep(50);
            if (Thread.currentThread().getPriority() == 7) {
                throw new IllegalArgumentException("priority 7 not allowed");
            }
            return super.tryLock(time, unit);
        }

        @Override
        public void unlock() {
            super.unlock();
        }

        @Override
        public Condition newCondition() {
            return super.newCondition();
        }        
    }

    
    private void logString(String message) {
        Thread currentThread = Thread.currentThread();
        System.out.println(
                String.format("%4d", System.currentTimeMillis() - startOfTime)
                + " : " + currentThread.getName() + "@" + currentThread.hashCode() + " at priority " + currentThread.getPriority()
                + " : " + message);
    }
    
    private static void prettyPrintList(String title, List<String> list) {
        System.out.println(title + " = [");
        list.forEach(s -> System.out.println("  " + s));
        System.out.println("]");
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
