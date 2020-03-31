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
import org.junit.jupiter.params.provider.EnumSource;
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
     * Test that we don't run into IndexOutOfBoundsException.
     */
    @ParameterizedTest
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    void testAllLevels(int level) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(DoThreadLock.class, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(2, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(level, null, new WaitArgMillis(0)), 0, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(level, null, null, Signal.SIGNAL_ALL), 100, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        assertThat(priorityLock.toString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }
    

    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * As for the rest, the thread with the highest priority runs first, and so on, till the thread with the lowest priority runs.
     * There is one catch: While the thread with the highest priority is running we change its priority.
     * This should not break other threads which are waiting on it to finish.
     */
    @ParameterizedTest
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
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
        executor.schedule(() -> doThread.action(7, 2, null, null), 600, TimeUnit.MILLISECONDS);
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
        } else if (DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
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
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    @SuppressWarnings("checkstyle:LineLength")
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
        executor.schedule(() -> doThread.action(7, 2, null, null), 600, TimeUnit.MILLISECONDS);
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
                               "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted",
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
        } else if (DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
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
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    @SuppressWarnings("checkstyle:LineLength")
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
                               "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted",
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
        } else if (DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
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
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    @SuppressWarnings("checkstyle:LineLength")
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
        executor.schedule(() -> doThread.action(7), 600, TimeUnit.MILLISECONDS); // DoThreadTryLockWith2000Timeout: will wait till 2600
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
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted"));
        } else if (DoThreadLockInterruptibly.class.equals(clazz)) {
            Collections.sort(doThread.getMessages().subList(2, 6)); // the exceptions occur in a different order each time
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "end thread with priority 4", // at 1100
                            "end thread with priority 7", // at 2100
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
        } else if (DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
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
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
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
                doThread.getLock(null);
                try {
                    logString("acquired lock");
                    sleep(1000);
                    logString("changing priority of thread from 6 to 3");
                    doThread.getMessages().add("thread with priority 6 changed to 3");
                    currentThread.setPriority(3);
                    doThread.getLock(null);
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

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
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
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
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
                doThread.getLock(null);
                try {
                    logString("acquired lock");
                    sleep(1000);
                    logString("changing priority of thread from 6 to 3");
                    doThread.getMessages().add("thread with priority 6 changed to 3");
                    currentThread.setPriority(3);
                    doThread.getLock(null);
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

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
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
     * <p>An alternate design is to use PriorityExecutorService.
     * 
     * @see MoreExecutorsTest#testNotEnoughThreads1()
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
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    void testLockException(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(() -> new ThrowAtPrioritySevenLock());

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

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed",
                            "end thread with priority 4",
                            "end thread with priority 6"));
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed",
                            "end thread with priority 4"));
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }


    @ParameterizedTest
    @EnumSource(Signal.class)
    void testSignalAndSignalAll(Signal signal) throws InterruptedException {
        WaitArg nanos = new WaitArgMillis(TimeUnit.SECONDS.toMillis(4));
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(DoThreadLockInterruptibly.class, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(11, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(3, null, nanos), 100, TimeUnit.MILLISECONDS); // will wait till 4300
        ScheduledFuture<?> future200 =
        executor.schedule(() -> doThread.awaitAction(4, null, nanos), 200, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future250 =
        executor.schedule(() -> doThread.awaitAction(5, null, nanos), 250, TimeUnit.MILLISECONDS); // will wait will 4250, but thread is interrupted before that time
        executor.schedule(() -> doThread.awaitAction(6, null, nanos), 300, TimeUnit.MILLISECONDS); // will wait will 4250, but thread is interrupted before that time
        executor.schedule(() -> doThread.awaitAction(9, 1, nanos), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(9, 1, nanos), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(9, 1, nanos), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, signal), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future200 of priority 4");
            future200.cancel(true);
        }, 3500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future250 of priority 5");
            future250.cancel(true);
        }, 4000, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (signal == Signal.SIGNAL) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "thread with priority 9 changed to 1", // at 1600
                            "end thread with priority 1", // at 2600
                            "end thread with priority 8")); // at 3600
            // threads 3, 4, 5, 6 are not woken up as we called condition.signal
            // so we wait for 10 seconds then test ends
            
            assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
            assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,1,1,1,1,0,0,2,0], signalCount=0"));
            
        } else if (signal == Signal.SIGNAL_ALL) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "thread with priority 9 changed to 1", // at 1600
                            "end thread with priority 1", // at 2600
                            "thread with priority 9 changed to 1", // at 2600
                            "end thread with priority 1", // at 3600
                            "thread with priority 9 changed to 1", // at 3600
                            "end thread with priority 1", // at 4600
                            "end thread with priority 8", // at 5600
                            "end thread with priority 6", // at 6600
                            "InterruptedException in await of thread with priority 5", // at 6600
                            "end thread with priority 5",
                            "InterruptedException in await of thread with priority 4", // at 6600
                            "end thread with priority 4", // at 6600
                            "end thread with priority 3")); // at 7600

            assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
            assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
            
        } else {
            throw new UnsupportedOperationException();
        }
    }

    
    @ParameterizedTest
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class})
    void testSignalOnly(Class<?> clazz) throws InterruptedException {
        WaitArg unused = new WaitArgMillis(TimeUnit.SECONDS.toMillis(4));
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(11, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(2, null, unused), 50, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(3, null, unused), 100, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future200 =
        executor.schedule(() -> doThread.awaitAction(4, null, unused), 200, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future250 =
        executor.schedule(() -> doThread.awaitAction(5, null, unused), 250, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, unused), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(9, 1, unused), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future200 of priority 4");
            future200.cancel(true);
        }, 3500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future250 of priority 5");
            future250.cancel(true);
        }, 4000, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to signal condition 5 times");
            priorityLock.lock();
            try {
                doThread.condition.signal();
                doThread.condition.signal();
                doThread.condition.signal();
                doThread.condition.signal();
                doThread.condition.signal();
            } finally {
                priorityLock.unlock();
            }
        }, 5000, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "thread with priority 9 changed to 1", // at 1600
                            "end thread with priority 1", // at 2600
                            "end thread with priority 8", // at 3600
                            "end thread with priority 6", // at 6000
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted", // at 6000
                            "thread with priority 4 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted", // at 6000
                            "end thread with priority 3", // at 7000
                            "end thread with priority 2")); // at 8000
        } else if (DoThreadLockInterruptibly.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "thread with priority 9 changed to 1", // at 1600
                            "end thread with priority 1", // at 2600
                            "end thread with priority 8", // at 3600
                            "end thread with priority 6", // at 6000
                            "InterruptedException in await of thread with priority 5", // at 6000
                            "end thread with priority 5",
                            "InterruptedException in await of thread with priority 4", // at 6000
                            "end thread with priority 4", // at 6000
                            "end thread with priority 3", // at 7000
                            "end thread with priority 2")); // at 8000
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }

    
    /**
     * This test sets up 4 threads waiting on a condition.
     * A 5th threads has the lowest priority and calls signal.
     * The test verifies that the thread with the highest priority wins.
     */
    @ParameterizedTest
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class,
            DoThreadTryLockWithTimeoutNanos.class})
    void testAwait(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(4, null, new WaitArgMillis(4600)), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, new WaitArgMillis(3500)), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, new WaitArgMillis(3000)), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(9, 1, new WaitArgMillis(3000)), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "thread with priority 9 changed to 1", // at 1600
                            "end thread with priority 1", // at 2600
                            "end thread with priority 8", // at 3600
                            "end thread with priority 6", // at 4600
                            "end thread with priority 5", // at 5600
                            "end thread with priority 4")); // at 6600
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 4 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 5 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 6 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 9 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 8 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 3 changed to 2",
                            "end thread with priority 2"));
        } else if (DoThreadTryLockWithTimeoutMillis.class.equals(clazz) || DoThreadTryLockWithTimeoutNanos.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "thread with priority 9 changed to 1",
                            "end thread with priority 1", // will wait till 400+3000=3400, lock acquired at 1600, ends at 2600
                            "end thread with priority 8", // at 3600
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException", // will wait till 300+3000=3300
                            "end thread with priority 5", // will wait till 200+3500=3700, lock acquired at 3600, ends at 4600
                            "end thread with priority 4")); // will wait till 100+4600=4700, lock acquired at 4600, ends at 5600
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }


    /**
     * Test that acquired is false when thread gets hold of lock.
     * Here the internal call to await finishes before the elapsed time so acquired would be true,
     * but another thread of higher priority not tied to the condition is running, so acquired will be false after all.
     */
    @ParameterizedTest
    @ValueSource(classes = {
            DoThreadTryLockWithTimeoutMillis.class,
            DoThreadTryLockWithTimeoutNanos.class})
    void testAwaitReturnFalse(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(clazz, priorityLock);
        WaitArg waitArg = new WaitArgMillis(3500);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(8, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(2, null, waitArg), 100, TimeUnit.MILLISECONDS); // will wait till 3600
        executor.schedule(() -> doThread.awaitAction(3, null, waitArg), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(4, null, waitArg), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(9, null, waitArg), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7, null, 5000L, null), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8, null, 5000L, null), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(10, null, null, Signal.SIGNAL_ALL), 500, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "end thread with priority 5", // at 1300
                        "end thread with priority 10", // at 2300
                        "end thread with priority 9", // at 3300
                        "end thread with priority 8", // at 4300
                        "end thread with priority 7", // at 5300
                        "thread with priority 4 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException", // at 5300
                        "thread with priority 3 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException",
                        "thread with priority 2 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException"));

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }


    /**
     * Same as the above except that some threads are cancelled.
     */
    @ParameterizedTest
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class,
            DoThreadTryLockWithTimeoutNanos.class})
    @SuppressWarnings("checkstyle:LineLength")
    void testAwaitWithInterrupt(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(9, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(3, null, new WaitArgMillis(4600)), 100, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future200 = 
        executor.schedule(() -> doThread.awaitAction(4, null, new WaitArgMillis(3500)), 200, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future250 = 
        executor.schedule(() -> doThread.awaitAction(5, null, new WaitArgMillis(3500)), 250, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, new WaitArgMillis(4000)), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, 1, new WaitArgMillis(3000)), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future200 of priority 4");
            future200.cancel(true);
        }, 3500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future250 of priority 5");
            future250.cancel(true);
        }, 4500, TimeUnit.MILLISECONDS);

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
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted", // at 4600
                            "thread with priority 4 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted", // at 4600
                            "end thread with priority 3")); // at 5600
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
                            "InterruptedException in await of thread with priority 4",
                            "end thread with priority 4",
                            "end thread with priority 3")); // at 5600
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 4 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 5 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 6 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 7 encountered exception java.lang.UnsupportedOperationException: DoThreadTryLock.doAwait",
                            "thread with priority 8 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 3 changed to 2",
                            "end thread with priority 2"));
        } else if (DoThreadTryLockWithTimeoutMillis.class.equals(clazz) || DoThreadTryLockWithTimeoutNanos.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "end thread with priority 8", // at 2600
                            "thread with priority 7 changed to 1",
                            "end thread with priority 1", // at 3600 - will wait till 400+3000=3400, lock acquired at 2600, ends at 3600
                            "end thread with priority 6", // at 4600 - will wait till 300+4000=4300, locak acquired at 3600, ends at 4600
                            "InterruptedException in await of thread with priority 5", // at 4600
                            "end thread with priority 5",
                            "InterruptedException in await of thread with priority 4", // at 4600
                            "end thread with priority 4",
                            "end thread with priority 3")); // at 5600 - will wait till 100+4600=4700, lock acquired at 4500, ends at 5500
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }


    @Test
    void testAwaitUntil() throws InterruptedException {
        WaitArg deadline = new WaitArgDeadline(new Date(System.currentTimeMillis() + 4000));
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(DoThreadTryLockWithTimeoutMillis.class, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(9, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(3, null, deadline), 100, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future200 = 
        executor.schedule(() -> doThread.awaitAction(4, null, deadline), 200, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future250 = 
        executor.schedule(() -> doThread.awaitAction(5, null, deadline), 250, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, deadline), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, 1, deadline), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future200 of priority 4");
            future200.cancel(true);
        }, 3500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future250 of priority 5");
            future250.cancel(true);
        }, 4500, TimeUnit.MILLISECONDS);

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
                        "InterruptedException in await of thread with priority 5", // at 4600
                        "end thread with priority 5",
                        "InterruptedException in await of thread with priority 4",
                        "end thread with priority 4",
                        "thread with priority 3 encountered exception myutils.util.concurrent.PriorityLockTest$AwaitReturnsFalseException")); // at 4600

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }


    /**
     * This test starts 3 threads with different priorities, and await on one of them throws as soon as await is called.
     * The test verifies that the threads with lower priority still run.
     */
    @ParameterizedTest
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class})
    void testAwaitException1(Class<?> clazz) throws InterruptedException {
        WaitArg millis = new WaitArgMillis(5000);
        PriorityLock priorityLock = new PriorityLock(() -> new ThrowAtPrioritySevenAwait(true, false));

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(4, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(4, null, millis), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, null, millis), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, millis), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 500, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed",
                        "thread with priority 3 changed to 2", 
                        "end thread with priority 2",
                        "end thread with priority 5",
                        "end thread with priority 4"));

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }



    /**
     * This test starts 3 threads with different priorities, and await on one of them throws as soon at the end of await.
     * The test verifies that the threads with lower priority still run.
     */
    @ParameterizedTest
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class})
    void testAwaitException2(Class<?> clazz) throws InterruptedException {
        WaitArg millis = new WaitArgMillis(5000);
        PriorityLock priorityLock = new PriorityLock(() -> new ThrowAtPrioritySevenAwait(true, true));

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(4, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(4, null, millis), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, null, millis), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, millis), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 500, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10_000, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 3 changed to 2", 
                        "end thread with priority 2",
                        "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed",
                        "end thread with priority 5",
                        "end thread with priority 4"));

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }



    /**
     * This test starts 3 threads with different priorities, and lockAfterAwait throws an exception.
     * The test verifies that the threads with lower priority still run.
     */
    @ParameterizedTest
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class})
    void testAwaitException3(Class<?> clazz) throws InterruptedException {
        WaitArg millis = new WaitArgMillis(5000);
        ThrowAtPrioritySevenAwait internalLock = new ThrowAtPrioritySevenAwait(false, true);
        PriorityLock priorityLock = new PriorityLock(() -> internalLock);

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> doThread.awaitAction(4, null, millis), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, millis), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, null, millis), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(9, null, null, null), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            // thread with priority 7: await is called at 300
            // thread with priority 7: signaled at 1500, calls await again at 1500 (in order to wait for thread priority 9 to finish)
            // make this 2nd await throw in order to get code coverage in code in lockUninterruptiblyAfterAwait
           logString("about to make ThrowAtPrioritySevenAwait throw an exception when await is called");
           internalLock.setShouldThrow(true);
        }, 1000, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10_000, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 3 changed to 2", 
                        "end thread with priority 2", 
                        "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLock$FailedToReacquireLockError",
                        "end thread with priority 6",
                        "end thread with priority 4"));

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }


    private enum Signal {
        SIGNAL,
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
            action(priority, null, null, null);
        }
        
        /**
         * Call lock, followed by sleep(1000).
         * 
         * @param priority the initial priority of the thread, and lock is called right after setting the priority
         * @param newPriority if not null the new priority of the thread after lock returns. Used to verify that threads waiting on the original thread are signaled.
         * @param tryLockMillis the timeout parameter to tryLock - only applicable for DoThreadTryLockWith2000Timeout
         * @param signal if not null call signal or signal_all
         */
        void action(int priority, Integer newPriority, Long tryLockMillis, @Nullable Signal signal) {
            final Thread currentThread = Thread.currentThread();
            currentThread.setPriority(priority);
            logString("start " + priorityLock.toString());
            try {
                getLock(tryLockMillis);
                try {
                    logString("acquired lock");
                    sleep(1000);
                    if (newPriority != null) {
                        logString("* changing priority of thread from " + currentThread.getPriority() + " to " + newPriority);
                        messages.add("thread with priority " + currentThread.getPriority() + " changed to " + newPriority);
                        currentThread.setPriority(newPriority);
                    }
                    if (signal != null) {
                        switch (signal) {
                            case SIGNAL:
                                logString("about to call condition.signal");
                                condition.signal();
                                break;
                            case SIGNAL_ALL:
                                logString("about to call condition.signalAll");
                                condition.signalAll();
                                break;
                            default:
                                throw new UnsupportedOperationException();
                        }
                    }
                    logString("* end");
                    messages.add("end thread with priority " + currentThread.getPriority());
                } finally {
                    priorityLock.unlock();
                }
            } catch (InterruptedException | RuntimeException | Error e) {
                logString("* caught exception " + e.toString());
                if (shouldLogCallstack(e)) {
                    e.printStackTrace(System.out);
                }
                messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
            }
        }

        /**
         * Call await, followed by sleep(1000).
         *
         * @param priority the initial priority of the thread, and await is called right after setting the priority
         * @param newPriority if not null the new priority of the thread after await returns. Used to verify that threads waiting on the original thread are signaled.
         * @param waitArg the timeout parameter to await - only applicable for DoThreadTryLockWith2000Timeout
         */
        void awaitAction(int priority, Integer newPriority, WaitArg waitArg) {
            final Thread currentThread = Thread.currentThread();
            currentThread.setPriority(priority);
            logString("start " + priorityLock.toString() + " " + conditiontoString());
            try {
                boolean threadDeath = false;
                getLock(null);
                try {
                    logString("acquired lock");
                    logString("waiting on condition");
                    try {
                        waitArg.doWait(this);
                        logString("await finished");
                        if (newPriority != null) {
                            logString("* changing priority of thread from " + currentThread.getPriority() + " to " + newPriority);
                            messages.add("thread with priority " + currentThread.getPriority() + " changed to " + newPriority);
                            currentThread.setPriority(newPriority);
                        }
                        sleep(1000);
                    } catch (InterruptedException e) {
                        logString("await finished with InterruptedException");
                        messages.add("InterruptedException in await of thread with priority " + currentThread.getPriority());
                    }
                    logString("* end");
                    messages.add("end thread with priority " + currentThread.getPriority());
                } catch (RuntimeException | PriorityLock.FailedToReacquireLockError e) {
                    logString("* caught exception " + e.toString());
                    if (shouldLogCallstack(e)) {
                        e.printStackTrace(System.out);
                    }
                    messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
                    if (e instanceof ThreadDeath) {
                        threadDeath = true;
                        
                    }
                } finally {
                    if (!threadDeath) {
                        priorityLock.unlock();
                    }
                }
            } catch (InterruptedException | RuntimeException e) {
                logString("* caught exception " + e.toString());
                if (shouldLogCallstack(e)) {
                    e.printStackTrace(System.out);
                }
                messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
            }
        }
        
        abstract void getLock(Long tryLockMillis) throws InterruptedException;
        
        abstract void doAwait(long awaitMillis) throws InterruptedException;

        abstract void doAwaitUntil(Date deadline) throws InterruptedException;        
        
        abstract void doAwaitNanos(long nanos) throws InterruptedException;
        
        List<String> getMessages() {
            return messages;
        }

        final String conditiontoString() {
            return condition.toString();
        }
    }

    private boolean shouldLogCallstack(Throwable e) {
        while (true) {
            if ((e instanceof InterruptedException)
                    || (e instanceof DoNotLogCallStack)
                    || (e instanceof UnsupportedOperationException)
                    || (e instanceof PriorityLock.FailedToReacquireLockError)) {
                return false;
            }
            Throwable cause = e.getCause();
            if (cause == null) {
                break;
            }
            e = cause;
        }
        return true;
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
    
    private class DoThreadLock extends DoThread {
        private DoThreadLock(PriorityLock priorityLock) {
            super(priorityLock);
        }
        
        @Override
        void getLock(Long tryLockMillis) {
            priorityLock.lock();
        }

        @Override
        void doAwait(long awaitMillis) {
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
        void getLock(Long tryLockMillis) throws InterruptedException {
            priorityLock.lockInterruptibly();
        }
        
        @Override
        void doAwait(long awaitMillis) throws InterruptedException {
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
        void getLock(Long tryLockMillis) {
            boolean acquired = priorityLock.tryLock();
            if (!acquired) {
                throw new FailedToAcquireLockException();
            }
        }
        
        @Override
        void doAwait(long awaitMillis) {
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

    private abstract class AbstractDoThreadTryLockWithTimeout extends DoThread {
        private static final long DEFAULT_WAIT_TIME_MILLIS = 2000;

        private AbstractDoThreadTryLockWithTimeout(PriorityLock priorityLock) {
            super(priorityLock);
        }
        
        @Override
        void getLock(Long tryLockMillis) throws InterruptedException {
            tryLockMillis = fallbackToDefault(tryLockMillis);
            boolean acquired = priorityLock.tryLock(tryLockMillis, TimeUnit.MILLISECONDS);
            if (!acquired) {
                throw new FailedToAcquireLockException();
            }
        }
        
        @Override
        void doAwait(long awaitMillis) throws InterruptedException {
            awaitMillis = fallbackToDefault(awaitMillis);
            boolean acquiredWithTimeout = !invokeAwait(awaitMillis);
            if (acquiredWithTimeout) {
                throw new AwaitReturnsFalseException();
            }
        }
        
        @Override
        void doAwaitUntil(Date deadline) throws InterruptedException {
            boolean acquiredWithTimeout = !condition.awaitUntil(deadline);
            if (acquiredWithTimeout) {
                throw new AwaitReturnsFalseException();
            }
        }
        
        @Override
        void doAwaitNanos(long nanos) throws InterruptedException {
            long timeLeft = condition.awaitNanos(nanos);
            if (timeLeft <= 0) {
                throw new AwaitReturnsFalseException();
            }
        }

        private long fallbackToDefault(Long input) {
            if (input != null) {
                return input;
            }
            return DEFAULT_WAIT_TIME_MILLIS;
        }
        
        protected abstract boolean invokeAwait(long awaitMillis) throws InterruptedException;
    }
    
    private class DoThreadTryLockWithTimeoutMillis extends AbstractDoThreadTryLockWithTimeout {
        private DoThreadTryLockWithTimeoutMillis(PriorityLock priorityLock) {
            super(priorityLock);
        }

        @Override
        protected boolean invokeAwait(long awaitMillis) throws InterruptedException {
            return condition.await(awaitMillis, TimeUnit.MILLISECONDS);
        }
    }
    
    private class DoThreadTryLockWithTimeoutNanos extends AbstractDoThreadTryLockWithTimeout {
        private DoThreadTryLockWithTimeoutNanos(PriorityLock priorityLock) {
            super(priorityLock);
        }

        @Override
        protected boolean invokeAwait(long awaitMillis) throws InterruptedException {
            return condition.awaitNanos(awaitMillis * 1_000_000) > 0;
        }
    }
    
    private interface DoNotLogCallStack {
    }
    
    private static class FailedToAcquireLockException extends RuntimeException implements DoNotLogCallStack {
        private static final long serialVersionUID = 1L;
    }

    private static class AwaitReturnsFalseException extends RuntimeException implements DoNotLogCallStack {
        private static final long serialVersionUID = 1L;
    }
    
    
    private static class ThrowAtPrioritySevenException extends RuntimeException implements DoNotLogCallStack {        
        private static final long serialVersionUID = 1L;
        
        ThrowAtPrioritySevenException() {
            super("priority 7 not allowed");
        }
    }
    
    private static class ThrowAtPrioritySevenLock extends ReentrantLock {
        private static final long serialVersionUID = 1L;

        ThrowAtPrioritySevenLock() {
            super();
        }
        
        @Override
        public void lock() {
            sleep(300);
            if (Thread.currentThread().getPriority() == 7) {
                throw new ThrowAtPrioritySevenException();
            }
            super.lock();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            sleep(300);
            if (Thread.currentThread().getPriority() == 7) {
                throw new ThrowAtPrioritySevenException();
            }
            super.lockInterruptibly();
        }

        @Override
        public boolean tryLock() {
            sleep(300);
            if (Thread.currentThread().getPriority() == 7) {
                throw new ThrowAtPrioritySevenException();
            }
            return super.tryLock();
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            sleep(300);
            if (Thread.currentThread().getPriority() == 7) {
                throw new ThrowAtPrioritySevenException();
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

    private static class ThrowAtPrioritySevenAwait extends ReentrantLock implements DoNotLogCallStack {
        private static final long serialVersionUID = 1L;
        
        private boolean shouldThrow;
        private final boolean throwAfter;

        ThrowAtPrioritySevenAwait(boolean shouldThrow, boolean throwAfter) {
            super();
            setShouldThrow(shouldThrow);
            this.throwAfter = throwAfter;
        }
        
        public void setShouldThrow(boolean shouldThrow) {
            this.shouldThrow = shouldThrow;            
        }

        @Override
        public @Nonnull Condition newCondition() {
            return new InternalCondition(super.newCondition());
        }
        
        private class InternalCondition implements Condition {
            private final Condition condition;

            public InternalCondition(Condition condition) {
                this.condition = condition;
            }

            @Override
            public void await() throws InterruptedException {
                throwIfNecessaryBefore();
                condition.await();
                throwIfNecessaryAfter();
            }

            @Override
            public boolean await(long time, TimeUnit unit) throws InterruptedException {
                throwIfNecessaryBefore();
                boolean acquired = condition.await(time, unit);
                throwIfNecessaryAfter();
                return acquired;
            }

            @Override
            public void awaitUninterruptibly() {
                throwIfNecessaryBefore();
                condition.awaitUninterruptibly();
                throwIfNecessaryAfter();
            }

            @Override
            public long awaitNanos(long nanosTimeout) throws InterruptedException {
                throwIfNecessaryBefore();
                long timeLeft = condition.awaitNanos(nanosTimeout);
                throwIfNecessaryAfter();
                return timeLeft;
            }

            @Override
            public boolean awaitUntil(Date deadline) throws InterruptedException {
                throwIfNecessaryBefore();
                boolean acquired = condition.awaitUntil(deadline);
                throwIfNecessaryAfter();
                return acquired;
            }
            
            private void throwIfNecessaryBefore() {
                if (shouldThrow && !throwAfter && Thread.currentThread().getPriority() == 7) {
                    throw new ThrowAtPrioritySevenException();
                }
            }

            private void throwIfNecessaryAfter() {
                if (shouldThrow && throwAfter && Thread.currentThread().getPriority() == 7) {
                    throw new ThrowAtPrioritySevenException();
                }
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
        if (DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
            return new DoThreadTryLockWithTimeoutMillis(priorityLock);
        }
        if (DoThreadTryLockWithTimeoutNanos.class.equals(clazz)) {
            return new DoThreadTryLockWithTimeoutNanos(priorityLock);
        }
        throw new UnsupportedOperationException();
    }
    
    private void logString(String message) {
        Thread currentThread = Thread.currentThread();
        System.out.println(
                String.format("%4d", System.currentTimeMillis() - startOfTime)
                + " : " + currentThread.getName() + " at priority " + currentThread.getPriority()
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
            throw new SleepInterruptedException(e);
        }
    }
    
    private static class SleepInterruptedException extends RuntimeException implements DoNotLogCallStack {
        private static final long serialVersionUID = 1L;
        
        SleepInterruptedException(InterruptedException cause) {
            super(cause);
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
