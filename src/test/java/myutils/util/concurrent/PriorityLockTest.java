package myutils.util.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


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
    
    
    /**
     * Test that toString prints out the value of internal lock toString followed by the counts of each level.
     */
    @Test
    void testToString() {
        PriorityLock priorityLock = new PriorityLock();
        Thread.currentThread().setPriority(10);
        priorityLock.lock();
        Condition condition = priorityLock.newCondition();
        System.out.println(priorityLock.toString());
        System.out.println(condition.toString());
        assertThat(priorityLock.toString(), Matchers.matchesRegex("^java.util.concurrent.locks.ReentrantLock@[a-f0-9]+\\[Locked by thread main\\]\\[0,0,0,0,0,0,0,0,0,1\\]$"));
        assertThat(condition.toString(), Matchers.matchesRegex("^java.util.concurrent.locks.AbstractQueuedSynchronizer\\$ConditionObject@[a-f0-9]+\\[0,0,0,0,0,0,0,0,0,0\\]$"));
    }
    
    

    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * As for the rest, the thread with the highest priority runs first, and so on, till the thread with the lowest priority runs.
     * There is one catch: While the thread with the highest priority is running we change its priority.
     * This should not break other threads which are waiting on it to finish.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testLock(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7, 2, null), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(1), 3800, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                       Matchers.contains(
                               // thread with priority 4 runs first, and while it is running all others get added to the queue
                               "end thread with priority 4",
                               "thread with priority 7 changed to 2",
                               "end thread with priority 2",
                               "end thread with priority 6",
                               "end thread with priority 6",
                               "end thread with priority 5",
                               "end thread with priority 5",
                               "end thread with priority 1"));
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 4",
                            "end thread with priority 1"));
        } else if (DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            Collections.sort(doThread.getMessages().subList(3, 6)); // the tryLock failing happens in a different order each time
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "end thread with priority 4",
                            "thread with priority 7 changed to 2", // thread could wait till 2600 starts at 1100 and ends at 2100
                            "end thread with priority 2",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 6", // thread could wait till 2300 but starts at 2100 and ends at 3100
                            "end thread with priority 1"));
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    
    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * One thread with a lower priority that is waiting on a thread with higher priority is cancelled.
     * Verify that the threads waiting on the canceled thread finish.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testLockWithCancellation1(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 400, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future500 =
        executor.schedule(() -> doThread.action(6), 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7, 2, null), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future500 of priority 6");
            future500.cancel(true);
        }, 700, TimeUnit.MILLISECONDS); 

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                       Matchers.contains(
                               // thread with priority 4 runs first, and while it is running all others get added to the queue
                               "end thread with priority 4",
                               "thread with priority 7 changed to 2",
                               "end thread with priority 2",
                               "end thread with priority 6",
                               "thread with priority 6 encountered exception java.lang.RuntimeException: java.lang.InterruptedException: sleep interrupted",
                               "end thread with priority 5",
                               "end thread with priority 5"));
        } else if (DoThreadLockInterruptibly.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 6 encountered exception java.lang.InterruptedException",
                            "end thread with priority 4",
                            "thread with priority 7 changed to 2",
                            "end thread with priority 2",
                            "end thread with priority 6",
                            "end thread with priority 5",
                            "end thread with priority 5"));
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 4"));
        } else if (DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 6 encountered exception java.lang.InterruptedException",
                            "end thread with priority 4",
                            "thread with priority 7 changed to 2", // thread could wait till 2600 starts at 1100 and ends at 2100
                            "end thread with priority 2",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 6")); // thread could wait till 2300 but starts at 2100 and ends at 3100
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }

    
    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * There is another thread that cancels the thread with the highest priority --
     * i.e. it receives an InterruptedExcedption while in ReentrantLock::lockInterruptibly.
     * So it does not run, and all the other threads run in order.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testLockWithCancellation2(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 500, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future600 =
        executor.schedule(() -> doThread.action(7), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread of priority 7");
            future600.cancel(true);
        }, 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                       Matchers.contains(
                               // thread with priority 4 runs first, and while it is running all others get added to the queue
                               "end thread with priority 4",
                               "thread with priority 7 encountered exception java.lang.RuntimeException: java.lang.InterruptedException: sleep interrupted",
                               "end thread with priority 6",
                               "end thread with priority 6",
                               "end thread with priority 5",
                               "end thread with priority 5"));
        } else if (DoThreadLockInterruptibly.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 7 encountered exception java.lang.InterruptedException",
                            "end thread with priority 4",
                            "end thread with priority 6",
                            "end thread with priority 6",
                            "end thread with priority 5",
                            "end thread with priority 5"));
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 4"));
        } else if (DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 7 encountered exception java.lang.InterruptedException",
                            "end thread with priority 4",
                            "end thread with priority 6",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 6")); // thread could wait till 2300 but starts at 2100 and ends at 3100
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * There is another thread that cancels all threads with less than the highest priority --
     * i.e. they receive an InterruptedExcedption while in Condition::await.
     * So only the first two threads run.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testLockWithCancellation3(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future200 =
        executor.schedule(() -> doThread.action(5), 200, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future300 =
        executor.schedule(() -> doThread.action(6), 300, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future400 =
        executor.schedule(() -> doThread.action(5), 400, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future500 =
        executor.schedule(() -> doThread.action(6), 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7), 600, TimeUnit.MILLISECONDS);
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

        if (DoThreadLock.class.equals(clazz)) {
            Collections.sort(doThread.getMessages().subList(2, 6)); // the exceptions occur in a different order each time
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "end thread with priority 4",
                            "end thread with priority 7",
                            "thread with priority 5 encountered exception java.lang.RuntimeException: java.lang.InterruptedException: sleep interrupted",
                            "thread with priority 5 encountered exception java.lang.RuntimeException: java.lang.InterruptedException: sleep interrupted",
                            "thread with priority 6 encountered exception java.lang.RuntimeException: java.lang.InterruptedException: sleep interrupted",
                            "thread with priority 6 encountered exception java.lang.RuntimeException: java.lang.InterruptedException: sleep interrupted"));
        } else if (DoThreadLockInterruptibly.class.equals(clazz)) {
            Collections.sort(doThread.getMessages().subList(2, 6)); // the exceptions occur in a different order each time
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "end thread with priority 4",
                            "end thread with priority 7",
                            "thread with priority 5 encountered exception java.lang.InterruptedException",
                            "thread with priority 5 encountered exception java.lang.InterruptedException",
                            "thread with priority 6 encountered exception java.lang.InterruptedException",
                            "thread with priority 6 encountered exception java.lang.InterruptedException"));
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 4"));
        } else if (DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            Collections.sort(doThread.getMessages().subList(1, 5)); // the interrupted exception occurs in a different order each time
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "end thread with priority 4",
                            "thread with priority 5 encountered exception java.lang.InterruptedException",
                            "thread with priority 5 encountered exception java.lang.InterruptedException",
                            "thread with priority 6 encountered exception java.lang.InterruptedException",
                            "thread with priority 6 encountered exception java.lang.InterruptedException",
                            "end thread with priority 7"));
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }

    
    /**
     * This test starts 3 threads with different priorities, and each thread acquires a priority lock.
     * A thread with a lower priority is waiting on a thread with higher priority.
     * The thread with the higher priority changes its priority and locks the thread again.
     * Verify that all threads finish.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testLockThreadThatIsAlreadyLocked1(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(4, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { 
            final Thread currentThread = Thread.currentThread();
            currentThread.setPriority(6);
            logString("start");
            try {
                doThread.getLock();
                try {
                    logString("acquired lock");
                    sleep(1000);
                    logString("changing priority of thread from 6 to 3");
                    doThread.getMessages().add("thread with priority 6 changed to 3");
                    currentThread.setPriority(3);
                    doThread.getLock();
                    try {
                        logString("acquired lock again");
                    } finally {
                        priorityLock.unlock();
                        logString("inner end");
                        doThread.getMessages().add("release lock in thread with priority " + currentThread.getPriority());
                    }
                    logString("end");
                    doThread.getMessages().add("end thread with priority " + currentThread.getPriority());
                } finally {
                    priorityLock.unlock();
                }
            } catch (InterruptedException | RuntimeException | Error e) {
                logString("caught exception " + e.toString());
                doThread.getMessages().add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
            }
        }, 300, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "end thread with priority 4",
                            "thread with priority 6 changed to 3",
                            "release lock in thread with priority 3",
                            "end thread with priority 3",
                            "end thread with priority 5"));
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 4"));
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }

    
    /**
     * This test starts 2 threads with different priorities, and each thread acquires a priority lock.
     * The first thread has lower priority and acquires the lock twice.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testLockThreadThatIsAlreadyLocked2(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(4, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { 
            final Thread currentThread = Thread.currentThread();
            currentThread.setPriority(6);
            logString("start");
            try {
                doThread.getLock();
                try {
                    logString("acquired lock");
                    sleep(1000);
                    logString("changing priority of thread from 6 to 3");
                    doThread.getMessages().add("thread with priority 6 changed to 3");
                    currentThread.setPriority(3);
                    doThread.getLock();
                    try {
                        logString("acquired lock again");
                    } finally {
                        priorityLock.unlock();
                        logString("inner end");
                        doThread.getMessages().add("release lock in thread with priority " + currentThread.getPriority());
                    }
                    logString("end");
                    doThread.getMessages().add("end thread with priority " + currentThread.getPriority());
                } finally {
                    priorityLock.unlock();
                }
            } catch (InterruptedException | RuntimeException | Error e) {
                logString("caught exception " + e.toString());
                doThread.getMessages().add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
            }
        }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7), 300, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 6 changed to 3",
                            "release lock in thread with priority 3",
                            "end thread with priority 3",
                            "end thread with priority 7"));
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 changed to 3",
                            "release lock in thread with priority 3",
                            "end thread with priority 3"));
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }

    
    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * But the executor running the threads only supports 3 threads at a time,
     * so the thread with the highest priority does not run next, because even though it is in the scheduled executor's queue,
     * it hasn't started running yet so its presence is not known to the running threads,
     * and thus the threads with lower priorities will not wait for it to finish.
     * The test is an explanation of the order in which the tests run.
     *
     * <p>An alternate design is to use a ThreadPoolExecutor backed by a PriorityQueue as the workQueue
     * in the argument to new ThreadPoolExecutor.
     */
    @Test
    void testNotEnoughThreads() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThreadLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(9), 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());
        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue, but only 2 of them run
                           // active threads: [priority 4, priority 5, priority 6] priority 4 has lock
                           "end thread with priority 4",
                           // active threads: [priority 5, priority 6, priority 7] priority 6 has lock because it starts running before priority 7 added to active list
                           "end thread with priority 6",
                           // active threads: [priority 5, priority 7, priority 8] priority 7 has lock because it starts running before priority 8 added to active list
                           "end thread with priority 7",
                           // running: [priority 5, priority 8, priority 9] priority 8 has lock because it starts before priority 9 added to active list
                           "end thread with priority 8",
                           // running: [priority 5, priority 9] priority 7 has lock
                           "end thread with priority 9",
                           // running: [priority 5]
                           "end thread with priority 5"));

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }



    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * The next 4 threads start after 1st ends.
     * Of the 4 threads that start at time 1200ms, in theory only one of them gets to run.
     * Since Runtime.getRuntime().availableProcessors() is usually 2, some of the threads run sequentially,
     * so the higher priority thread may running a few nanoseconds after a lower priority one,
     * so tryLock() would return true for the lower priority lock.
     * Long after there is a 6th thread that runs, but it's the only thread running at the time so it goes ahead. 
     */
    @Test
    void testTryLock1() throws InterruptedException {
        System.out.println("availableProcessors=" + Runtime.getRuntime().availableProcessors());

        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = new DoThreadTryLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(9), 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(1), 2300, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        Collections.sort(doThread.getMessages().subList(1, 4)); // the FailedToAcquireLockException stuff occurs in different order each time

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        // thread with priority 4 runs first, and while it is running all others get added to the queue
                        "end thread with priority 4",
                        "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "thread with priority 8 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "end thread with priority 9",
                        "end thread with priority 1"));

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    /**
     * Same as the previous test, except the 4 threads run while the 1st is running, so all of them fail.
     */
    @Test
    void testTryLock2() throws InterruptedException {
        System.out.println("availableProcessors=" + Runtime.getRuntime().availableProcessors());

        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = new DoThreadTryLock(priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(9), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(1), 1200, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        Collections.sort(doThread.getMessages().subList(0, 4)); // the FailedToAcquireLockException stuff occurs in different order each time

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        // thread with priority 4 runs first, and while it is running all others get added to the queue
                        "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "thread with priority 8 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "thread with priority 9 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                        "end thread with priority 4",
                        "end thread with priority 1"));

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    /**
     * This test starts 3 threads with different priorities, and acquiring a lock on one of them throws.
     * The test verifies that the threads with lower priority still run.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testLockException(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(new ThrowAtPrioritySevenReentrantLock());

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7), 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 7 encountered exception java.lang.IllegalArgumentException: priority 7 not allowed",
                            "end thread with priority 4",
                            "end thread with priority 6"));
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 7 encountered exception java.lang.IllegalArgumentException: priority 7 not allowed",
                            "end thread with priority 4"));
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    /**
     * This test sets up 4 threads waiting on a condition.
     * A 5th threads has the lowest priority and calls signal.
     * The test verifies that the thread with the highest priority wins.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testAwait(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(4, null, new WaitArgMillis(4600)), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, new WaitArgMillis(3500)), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, new WaitArgMillis(3000)), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, 1, new WaitArgMillis(3000)), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "end thread with priority 8", // at 2600
                            "thread with priority 7 changed to 1", // at 2600
                            "end thread with priority 1", // at 3600
                            "end thread with priority 6", // at 4600
                            "end thread with priority 5", // at 5600
                            "end thread with priority 4")); // at 6600
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 4 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 5 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 6 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 7 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 8 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 3 changed to 2",
                            "end thread with priority 2"));
        } else if (DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "end thread with priority 8", // at 2600
                            "thread with priority 7 changed to 1",
                            "end thread with priority 1", // will wait till 400+3000=3400, lock acquired at 2600, ends at 3600
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException", // will wait till 300+3000=3300
                            "end thread with priority 5", // will wait till 200+3500=3700, lock acquired at 3600, ends at 4600
                            "end thread with priority 4")); // will wait till 100+4600=4700, lock acquired at 4600, ends at 5600
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    /**
     * Same as the above except that some threads are cancelled.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testAwaitWithInterrupt(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(4, null, new WaitArgMillis(4600)), 100, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future200 =
        executor.schedule(() -> doThread.awaitAction(5, null, new WaitArgMillis(3500)), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, new WaitArgMillis(3000)), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, 1, new WaitArgMillis(3000)), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread of priority 5");
            future200.cancel(true);
        }, 1200, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "end thread with priority 8", // at 2600
                            "thread with priority 7 changed to 1",
                            "end thread with priority 1",
                            "end thread with priority 6",
                            "thread with priority 5 encountered exception java.lang.RuntimeException: java.lang.InterruptedException: sleep interrupted", // at 4600
                            "end thread with priority 4")); // at 4600
        } else if (DoThreadLockInterruptibly.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "end thread with priority 8", // at 2600
                            "thread with priority 7 changed to 1",
                            "end thread with priority 1",
                            "end thread with priority 6",
                            "InterruptedException in await of thread with priority 5",
                            "end thread with priority 5",
                            "end thread with priority 4")); // at 4600
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 4 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 5 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 6 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 7 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 8 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 3 changed to 2",
                            "end thread with priority 2"));
        } else if (DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "end thread with priority 8", // at 2600
                            "thread with priority 7 changed to 1",
                            "end thread with priority 1", // will wait till 400+3000=3400, lock acquired at 2600, ends at 3600
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException", // will wait till 300+3000=3300
                            "InterruptedException in await of thread with priority 5", // at 2600
                            "end thread with priority 5", // at 2600
                            "end thread with priority 4")); // will wait till 100+4600=4700, lock acquired at 3600, ends at 4600
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    @Test
    void testAwaitUntil() throws InterruptedException {
        WaitArg deadline = new WaitArgDeadline(new Date(System.currentTimeMillis() + 4000));
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(DoThreadTryLockWith2000Timeout.class, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(4, null, deadline), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, deadline), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, deadline), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, 1, deadline), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 3 changed to 2", // at 1600
                        "end thread with priority 2", // at 1600
                        "end thread with priority 8", // at 2600
                        "thread with priority 7 changed to 1", // at 2600
                        "end thread with priority 1", // at 3600
                        "end thread with priority 6", // at 4600
                        "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException", // at 4600
                        "thread with priority 4 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException")); // at 4600

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    @Test
    void testAwaitNanos() throws InterruptedException {
        WaitArg nanos = new WaitArgNanos(TimeUnit.SECONDS.toNanos(4));
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(DoThreadTryLockWith2000Timeout.class, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(4, null, nanos), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, nanos), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, nanos), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, 1, nanos), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 3 changed to 2", // at 1600
                        "end thread with priority 2", // at 1600
                        "end thread with priority 8", // at 2600
                        "thread with priority 7 changed to 1", // at 2600
                        "end thread with priority 1", // at 3600
                        "end thread with priority 6", // at 4600
                        "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException", // at 4600
                        "thread with priority 4 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException")); // at 4600

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    /**
     * This test starts 3 threads with different priorities, and await on one of them throws.
     * The test verifies that the threads with lower priority still run.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLock.class, DoThreadTryLockWith2000Timeout.class})
    void testAwaitException(Class<?> clazz) throws InterruptedException {
        WaitArg millis = new WaitArgMillis(5000);
        PriorityLock priorityLock = new PriorityLock(new ThrowAtPrioritySevenReentrantAwait());

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(4, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(4, null, millis), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, millis), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, null, millis), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, Signal.SIGNAL_ALL), 1200, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", 
                            "end thread with priority 2", 
                            "thread with priority 7 encountered exception java.lang.IllegalArgumentException: priority 7 not allowed",
                            "end thread with priority 6",
                            "end thread with priority 4"));
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 4 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 6 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 7 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 3 changed to 2", 
                            "end thread with priority 2"));
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    private enum Signal {
        SIGNAL_ALL
    }

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
        
        /**
         * Call lock, followed by sleep(1000).
         * 
         * @param priority the initial priority of the thread, and lock is called right after setting the priority
         * @param newPriority if not null the new priority of the thread after lock returns. Used to verify that threads waiting on the original thread are signaled.
         * @param signal if not null call signal or signal_all
         */
        void action(int priority, Integer newPriority, @Nullable Signal signal) {
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
                        messages.add("thread with priority " + currentThread.getPriority() + " changed to " + newPriority);
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

        /**
         * Call await, followed by sleep(1000).
         *
         * @param priority the initial priority of the thread, and await is called right after setting the priority
         * @param newPriority if not null the new priority of the thread after await returns. Used to verify that threads waiting on the original thread are signaled.
         * @param waitTimeMillis the timeout parameter to await - only applicable for DoThreadTryLockWith2000Timeout
         */
        void awaitAction(int priority, Integer newPriority, WaitArg waitArg) {
            final Thread currentThread = Thread.currentThread();
            currentThread.setPriority(priority);
            logString("start");
            try {
                getLock();
                try {
                    logString("acquired lock");
                    logString("waiting on condition");
                    try {
                        waitArg.doWait(this);
                        logString("await finished");
                        if (newPriority != null) {
                            logString("changing priority of thread from " + currentThread.getPriority() + " to " + newPriority);
                            messages.add("thread with priority " + currentThread.getPriority() + " changed to " + newPriority);
                            currentThread.setPriority(newPriority);
                        }
                        sleep(1000);
                    } catch (InterruptedException e) {
                        messages.add("InterruptedException in await of thread with priority " + currentThread.getPriority());
                        logString("await finished with InterruptedException");
                    }
                    logString("end");
                    messages.add("end thread with priority " + currentThread.getPriority());
                } catch (RuntimeException e) {
                    logString("caught exception " + e.toString());
                    messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
                } finally {
                    priorityLock.unlock();
                }
            } catch (InterruptedException | RuntimeException e) {
                logString("caught exception " + e.toString());
                messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
            }
        }
        
        abstract void getLock() throws InterruptedException;
        
        abstract void doAwait(long waitTimeMillis) throws InterruptedException;
        abstract void doAwaitUntil(Date deadline) throws InterruptedException;        
        abstract void doAwaitNanos(long nanos) throws InterruptedException;
        
        List<String> getMessages() {
            return messages;
        }

        final String conditiontoString() {
            return condition.toString();
        }
    }
    
    interface WaitArg {
        void doWait(DoThread doThread) throws InterruptedException;
    }
    
    private static class WaitArgMillis implements WaitArg {
        private final long millis;

        WaitArgMillis(long millis) {
            this.millis = millis;
        }

        @Override
        public void doWait(DoThread doThread) throws InterruptedException {
            doThread.doAwait(millis);
        }
    }
    
    private static class WaitArgDeadline implements WaitArg {
        private final Date deadline;

        WaitArgDeadline(Date deadline) {
            this.deadline = deadline;
        }

        @Override
        public void doWait(DoThread doThread) throws InterruptedException {
            doThread.doAwaitUntil(deadline);
        }
    }
    
    private static class WaitArgNanos implements WaitArg {
        private final long nanos;

        WaitArgNanos(long nanos) {
            this.nanos = nanos;
        }

        @Override
        public void doWait(DoThread doThread) throws InterruptedException {
            doThread.doAwaitNanos(nanos);
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
        void doAwait(long waitTimeMillis) {
            condition.awaitUninterruptibly();
        }
        
        
        @Override
        void doAwaitUntil(Date deadline) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        void doAwaitNanos(long nanos) {
            throw new UnsupportedOperationException();
        }
    }

    private class DoThreadLockInterruptibly extends DoThread {
        DoThreadLockInterruptibly(PriorityLock priorityLock) {
            super(priorityLock);
        }
        
        @Override
        void getLock() throws InterruptedException {
            priorityLock.lockInterruptibly();
        }
        
        @Override
        void doAwait(long waitTimeMillis) throws InterruptedException {
            condition.await();
        }
        
        @Override
        void doAwaitUntil(Date deadline) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        void doAwaitNanos(long nanos) {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Helper class to test tryLock.
     * getLock sleeps for 50ms so that if several threads call tryLock at the same time, we can be sure that the one with the highest priority goes first.
     *
     */
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
        void doAwait(long waitTimeMillis) {
            throw new UnsupportedOperationException("DoThreadTryLock.doAwait");
        }
        
        @Override
        void doAwaitUntil(Date deadline) {
            throw new UnsupportedOperationException();
        }
        
        @Override
        void doAwaitNanos(long nanos) {
            throw new UnsupportedOperationException();
        }
    }

    private class DoThreadTryLockWith2000Timeout extends DoThread {
        private static final long DEFAULT_WAIT_TIME_MILLIS = 2000;

        private DoThreadTryLockWith2000Timeout(PriorityLock priorityLock) {
            super(priorityLock);
        }
        
        @Override
        void getLock() throws InterruptedException {
            boolean acquired = priorityLock.tryLock(DEFAULT_WAIT_TIME_MILLIS, TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new FailedToAcquireLockException();
            }
        }
        
        @Override
        void doAwait(long waitTimeMillis) throws InterruptedException {
            waitTimeMillis = fallbackToDefault(waitTimeMillis);
            boolean timedOut = !condition.await(waitTimeMillis, TimeUnit.MILLISECONDS);
            if (timedOut) {
                throw new AwaitReturnsFalseException();
            }
        }
        
        @Override
        void doAwaitUntil(Date deadline) throws InterruptedException {
            boolean timedOut = !condition.awaitUntil(deadline);
            if (timedOut) {
                throw new AwaitReturnsFalseException();
            }
        }
        
        @Override
        void doAwaitNanos(long nanos) throws InterruptedException {
            long timeLeft = condition.awaitNanos(nanos);
            if (timeLeft == 0) {
                throw new AwaitReturnsFalseException();
            }
        }

        private long fallbackToDefault(Long input) {
            if (input != null) {
                return input;
            }
            return DEFAULT_WAIT_TIME_MILLIS;
        }
    }
    
    
    private static class FailedToAcquireLockException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    private static class AwaitReturnsFalseException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }
    
    
    private static class ThrowAtPrioritySevenReentrantLock extends ReentrantLock {
        private static final long serialVersionUID = 1L;

        ThrowAtPrioritySevenReentrantLock() {
            super();
        }
        
        @Override
        public void lock() {
            sleep(300);
            if (Thread.currentThread().getPriority() == 7) {
                throw new IllegalArgumentException("priority 7 not allowed");
            }
            super.lock();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            sleep(300);
            if (Thread.currentThread().getPriority() == 7) {
                throw new IllegalArgumentException("priority 7 not allowed");
            }
            super.lockInterruptibly();
        }

        @Override
        public boolean tryLock() {
            sleep(300);
            if (Thread.currentThread().getPriority() == 7) {
                throw new IllegalArgumentException("priority 7 not allowed");
            }
            return super.tryLock();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            sleep(300);
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
        public @Nonnull Condition newCondition() {
            return super.newCondition();
        }        
    }

    private static class ThrowAtPrioritySevenReentrantAwait extends ReentrantLock {
        private static final long serialVersionUID = 1L;

        ThrowAtPrioritySevenReentrantAwait() {
            super();
        }
        
        @Override
        public @Nonnull Condition newCondition() {
            return new ThrowAtPrioritySevenReentrantAwaitCondition(super.newCondition());
        }
        
        private static class ThrowAtPrioritySevenReentrantAwaitCondition implements Condition {
            private final Condition condition;

            public ThrowAtPrioritySevenReentrantAwaitCondition(Condition condition) {
                this.condition = condition;
            }

            @Override
            public void await() throws InterruptedException {
                condition.await();
                if (Thread.currentThread().getPriority() == 7) {
                    throw new IllegalArgumentException("priority 7 not allowed");
                }
            }

            @Override
            public void awaitUninterruptibly() {
                condition.awaitUninterruptibly();
                if (Thread.currentThread().getPriority() == 7) {
                    throw new IllegalArgumentException("priority 7 not allowed");
                }
            }

            @Override
            public long awaitNanos(long nanosTimeout) throws InterruptedException {
                long timeLeft = condition.awaitNanos(nanosTimeout);
                if (Thread.currentThread().getPriority() == 7) {
                    throw new IllegalArgumentException("priority 7 not allowed");
                }
                return timeLeft;
            }

            @Override
            public boolean await(long time, TimeUnit unit) throws InterruptedException {
                boolean acquired = condition.await(time, unit);
                if (Thread.currentThread().getPriority() == 7) {
                    throw new IllegalArgumentException("priority 7 not allowed");
                }
                return acquired;
            }

            @Override
            public boolean awaitUntil(Date deadline) throws InterruptedException {
                boolean acquired = condition.awaitUntil(deadline);
                if (Thread.currentThread().getPriority() == 7) {
                    throw new IllegalArgumentException("priority 7 not allowed");
                }
                return acquired;
            }

            @Override
            public void signal() {
                condition.signal();
            }

            @Override
            public void signalAll() {
                condition.signalAll();                
            }
            
        }
    }

    private DoThread createDoThread(Class<?> clazz, PriorityLock priorityLock) {
        if (DoThreadLock.class.equals(clazz)) {
            return new DoThreadLock(priorityLock);
        }
        if (DoThreadLockInterruptibly.class.equals(clazz)) {
            return new DoThreadLockInterruptibly(priorityLock);
        }
        if (DoThreadTryLock.class.equals(clazz)) {
            return new DoThreadTryLock(priorityLock);
        }
        if (DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            return new DoThreadTryLockWith2000Timeout(priorityLock);
        }
        throw new UnsupportedOperationException();
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

    /**
     * The purpose of this thread is to demonstrate how await with timeout works.
     * After await(time, unit) finishes and more than 'time' has passed,
     * the current thread still holds the lock, but await returns false,
     * indicating that the current thread can release the lock and abort further processing.
     */
    @Test
    void testReentrantLock() throws InterruptedException {
        ReentrantLock reentrantLock = new ReentrantLock();
        Condition condition = reentrantLock.newCondition();

        logStringPlus(reentrantLock, "start");
        reentrantLock.lock();
        try {
            logStringPlus(reentrantLock, "after lock");
            Thread thread2 = new Thread(() -> {
                logStringPlus(reentrantLock, "start of thread2");
                reentrantLock.lock();
                try {
                    sleep(4000);
                    logStringPlus(reentrantLock, "after lock in thread2");
                    logStringPlus(reentrantLock, "about to call signal");
                    condition.signal();
                } finally {
                    reentrantLock.unlock();
                    logStringPlus(reentrantLock, "after unlock in thread2");
                }
            }, "thread2");
            thread2.start();

            boolean acquired = condition.await(3000, TimeUnit.MILLISECONDS);
            logStringPlus(reentrantLock, "after await acquired=" + acquired);
            assertTrue(reentrantLock.isLocked());
            assertTrue(reentrantLock.isHeldByCurrentThread());
            assertFalse(acquired);
        } finally {
            reentrantLock.unlock();
            logStringPlus(reentrantLock, "after unlock");
        }
    }

    private void logStringPlus(ReentrantLock reentrantLock, String message) {
        message += " :";
        message += " isLocked=" + reentrantLock.isLocked() + " isHeldByCurrentThread=" + reentrantLock.isHeldByCurrentThread();
        logString(message);
    }
}
