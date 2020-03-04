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
        priorityLock.lock();
        Condition condition = priorityLock.newCondition();
        System.out.println(priorityLock.toString());
        System.out.println(condition.toString());
        assertThat(priorityLock.toString(), Matchers.matchesRegex("^java.util.concurrent.locks.ReentrantLock@[a-f0-9]+\\[Locked by thread main\\]\\[0,0,0,0,1,0,0,0,0,0\\]$"));
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
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLockAfterSleep50.class, DoThreadTryLockWith2000Timeout.class})
    void testLock(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7, 2, null); }, 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(1); }, 3800, TimeUnit.MILLISECONDS);

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
        } else if (DoThreadTryLockAfterSleep50.class.equals(clazz)) {
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
    }


    
    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * One thread with a lower priority that is waiting on a thread with higher priority is cancelled.
     * Verify that the threads waiting on the canceled thread finish.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLockAfterSleep50.class, DoThreadTryLockWith2000Timeout.class})
    void testLockWithCancellation1(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(7, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 400, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future500 =
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7, 2, null); }, 600, TimeUnit.MILLISECONDS);
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
                               "thread with priority 6 encountered exception java.util.concurrent.CancellationException",
                               "thread with priority 7 changed to 2",
                               "end thread with priority 2",
                               "end thread with priority 6",
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
        } else if (DoThreadTryLockAfterSleep50.class.equals(clazz)) {
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
    }

    
    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * There is another thread that cancels the thread with the highest priority --
     * i.e. it receives an InterruptedExcedption while in ReentrantLock::lockInterruptibly.
     * So it does not run, and all the other threads run in order.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLockAfterSleep50.class, DoThreadTryLockWith2000Timeout.class})
    void testLockWithCancellation2(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);

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
        } else if (DoThreadTryLockAfterSleep50.class.equals(clazz)) {
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
    }


    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * There is another thread that cancels all threads with less than the highest priority --
     * i.e. they receive an InterruptedExcedption while in Condition::await.
     * So only the first two threads run.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLockAfterSleep50.class, DoThreadTryLockWith2000Timeout.class})
    void testLockWithCancellation3(Class<?> clazz) throws InterruptedException {
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

        // the interrupted stuff occurs in different order each time
        Collections.sort(doThread.getMessages().subList(2, 6));
        
        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "end thread with priority 4",
                           "end thread with priority 7",
                           "thread with priority 5 encountered exception java.lang.InterruptedException",
                           "thread with priority 5 encountered exception java.lang.InterruptedException",
                           "thread with priority 6 encountered exception java.lang.InterruptedException",
                           "thread with priority 6 encountered exception java.lang.InterruptedException"));
    }

    
    /**
     * This test starts 3 threads with different priorities, and each thread acquires a priority lock.
     * A thread with a lower priority is waiting on a thread with higher priority.
     * The thread with the higher priority changes its priority and locks the thread again.
     * Verify that all threads finish.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLockAfterSleep50.class, DoThreadTryLockWith2000Timeout.class})
    void testLockThreadThatIsAlreadyLocked1(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(4, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
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
        executor.awaitTermination(10_000, TimeUnit.SECONDS);
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
        } else if (DoThreadTryLockAfterSleep50.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            // thread with priority 4 runs first, and while it is running all others get added to the queue
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 4"));
        } else {
            throw new UnsupportedOperationException();
        }
    }

    
    /**
     * This test starts 2 threads with different priorities, and each thread acquires a priority lock.
     * The first thread has lower priority and acquires the lock twice.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLockAfterSleep50.class, DoThreadTryLockWith2000Timeout.class})
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
        executor.schedule(() -> { doThread.action(7); }, 300, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10_000, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 6 changed to 3",
                            "release lock in thread with priority 3",
                            "end thread with priority 3",
                            "end thread with priority 7"));
        } else if (DoThreadTryLockAfterSleep50.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 6 changed to 3",
                            "release lock in thread with priority 3",
                            "end thread with priority 3"));
        } else {
            throw new UnsupportedOperationException();
        }
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
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * Of the 4 threads that start at time 1200ms, only one of them gets to run.
     * Long after there is a 6th thread that runs, but it's the only thread running at the time so it goes ahead. 
     */
    @Test
    void testTryLock() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(new ThrowAtPrioritySevenReentrantLock(true));

        DoThread doThread = new DoThreadTryLockAfterSleep50(priorityLock);

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
        
        // the FailedToAcquireLockException stuff occurs in different order each time
        Collections.sort(doThread.getMessages().subList(1, 4));

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
     * This test starts 3 threads with different priorities, and acquiring a lock on the second thread throws.
     * The test verifies that the third thread still runs.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class, DoThreadTryLockAfterSleep50.class, DoThreadTryLockWith2000Timeout.class})
    void testLockException(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(new ThrowAtPrioritySevenReentrantLock(false));

        DoThread doThread = createDoThread(clazz, priorityLock);

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
     * This test sets up 4 threads waiting on a condition.
     * A 5th threads has the lowest priority and calls signal.
     * The test verifies that the thread with the highest priority wins.
     */
    @ParameterizedTest
    @ValueSource(classes = {DoThreadLock.class, DoThreadLockInterruptibly.class})
    void testAwaitUninterruptibly(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(5, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.awaitAction(4, false); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.awaitAction(5, false); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.awaitAction(6, false); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.awaitAction(7, false); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6, 1, Signal.SIGNAL_ALL); }, 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           "thread with priority 6 changed to 1",
                           "end thread with priority 1",
                           "end thread with priority 7",
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
        DoThreadLockInterruptibly(PriorityLock priorityLock) {
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

    /**
     * Helper class to test tryLock.
     * getLock sleeps for 50ms so that if several threads call tryLock at the same time, we can be sure that the one with the highest priority goes first.
     *
     */
    private class DoThreadTryLockAfterSleep50 extends DoThread {
        private DoThreadTryLockAfterSleep50(PriorityLock priorityLock) {
            super(priorityLock);
        }
        
        @Override
        void getLock() {
            sleep(50);
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

    private class DoThreadTryLockWith2000Timeout extends DoThread {
        private final long waitTimeMillis;

        private DoThreadTryLockWith2000Timeout(PriorityLock priorityLock, long waitTime) {
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
    
    private DoThread createDoThread(Class<?> clazz, PriorityLock priorityLock) {
        if (DoThreadLock.class.equals(clazz)) {
            return new DoThreadLock(priorityLock);
        }
        if (DoThreadLockInterruptibly.class.equals(clazz)) {
            return new DoThreadLockInterruptibly(priorityLock);
        }
        if (DoThreadTryLockAfterSleep50.class.equals(clazz)) {
            return new DoThreadTryLockAfterSleep50(priorityLock);
        }
        if (DoThreadTryLockWith2000Timeout.class.equals(clazz)) {
            return new DoThreadTryLockWith2000Timeout(priorityLock, 2000);
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
}
