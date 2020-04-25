package myutils.util.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import myutils.TestUtil;
import myutils.LogFailureToConsoleTestWatcher;
import myutils.util.concurrent.PriorityLock.PriorityLockCondition;
import myutils.util.concurrent.PriorityLock.PriorityLockNamedParams;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;


@ExtendWith(LogFailureToConsoleTestWatcher.class)
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
    
    @BeforeAll
    static void onStartAllTests() {
        System.out.println("start all tests");
        System.out.println("--------------------------------------------------------------------------------");
    }
    
    @AfterAll
    static void printAllTestsFinished() {
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("all tests finished");
    }
    

    /**
     * This test shows that ReentrantLock does not run threads with the highest priority first.
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(booleans = {false, true})
    void testReentrantLock(boolean fair) throws InterruptedException {
        ReentrantLock reentrantLock = new ReentrantLock(fair);

        DoThread doThread = new DoThreadLock(reentrantLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(7, myThreadFactory());

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7, 2, null, null), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(9), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(1), 3800, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // thread with priority 4 runs first, and while it is running all others get added to the queue
                           "end thread with priority 4",
                           "end thread with priority 5",
                           "end thread with priority 6",
                           "thread with priority 7 changed to 2",
                           "end thread with priority 2",
                           "end thread with priority 8",
                           "end thread with priority 9",
                           "end thread with priority 1"));
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
        PriorityLock.PriorityLockCondition condition = priorityLock.newCondition();
        System.out.println(priorityLock.toString());
        System.out.println(condition.toString());
        assertThat(priorityLock.toString(), Matchers.matchesRegex("^myutils.util.concurrent.PriorityLock@[a-f0-9]+ levels=\\[0,0,0,0,0,0,0,0,0,1\\]$"));
        assertThat(condition.toString(), Matchers.matchesRegex("^myutils.util.concurrent.PriorityLock\\$PriorityLockCondition@[a-f0-9]+ levels=\\[0,0,0,0,0,0,0,0,0,0\\], signalCount=0$"));
        assertEquals(10, priorityLock.highestPriorityThread());
        assertNull(condition.highestPriorityThread());
    }
    
    

    /**
     * Test that we don't run into IndexOutOfBoundsException.
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(ints = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10})
    void testAllLevels(int level) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(DoThreadLock.class, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, myThreadFactory());

        executor.schedule(() -> doThread.awaitAction(level, null, new WaitArgMillis(0)), 0, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(level, null, null, Signal.SIGNAL_ALL), 100, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        assertThat(priorityLock.toString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }
    

    // ------------------------------------------------------------------------------------------------
    // lock tests

    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * The test verifies that the first thread starts right away because there is nothing else in the queue.
     * As for the rest, the thread with the highest priority runs first, and so on, till the thread with the lowest priority runs.
     * There is one catch: While the thread with the highest priority is running we change its priority.
     * This should not break other threads which are waiting on it to finish.
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    void testLock(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(7, myThreadFactory());

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
     * This test interrupts the current thread then calls lockInterruptibly,
     * verifying that an InterruptedException is thrown.
     * Here internalLock.lockInterruptibly throws an exception.
     */
    @Test
    void testLockInterruptedThread1() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, myThreadFactory());
        
        AtomicBoolean interruptedExceptionThrown = new AtomicBoolean();

        executor.schedule(() -> {
            logString("start thread");
            logString("interrupt thread");
            Thread.currentThread().interrupt();
            try {
                logString("about to lock");
                priorityLock.lockInterruptibly();
            } catch (InterruptedException ignored) {
                logString("caught InterruptedException");
                interruptedExceptionThrown.set(true);
            }
            logString("end thread");
        }, 100, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        
        assertTrue(interruptedExceptionThrown.get());
        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }
    

    /**
     * This test interrupts the current thread then calls lockInterruptibly,
     * verifying that an InterruptedException is thrown.
     * Here internalLock.lockInterruptibly locks the thread but then interrupts itself.
     */
    @Test
    void testLockInterruptedThread2() throws InterruptedException {
        class WeirdReentrantLock extends ReentrantLock {
            private static final long serialVersionUID = 1L;

            @Override
            public void lockInterruptibly() throws InterruptedException {
                super.lockInterruptibly();
                Thread.currentThread().interrupt();
            }
        }
        
        PriorityLock priorityLock = new PriorityLock(PriorityLockNamedParams.create().setInternalLockCreator(WeirdReentrantLock::new));
 
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, myThreadFactory());
        AtomicBoolean interruptedExceptionThrown = new AtomicBoolean();

        executor.schedule(() -> {
            logString("start thread");
            try {
                logString("about to lock (lock function locks internal thread then interrupts itself)");
                priorityLock.lockInterruptibly();
            } catch (InterruptedException ignored) {
                logString("caught InterruptedException");
                interruptedExceptionThrown.set(true);
            }
            logString("end thread");
        }, 100, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        
        assertTrue(interruptedExceptionThrown.get());
        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }
    

    /**
     * This test starts 6 threads with different priorities, and each thread acquires a priority lock.
     * One thread with a lower priority that is waiting on a thread with higher priority is cancelled.
     * Verify that the threads waiting on the canceled thread finish.
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    void testLockWithCancellation1(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(7, myThreadFactory());

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
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    void testLockWithCancellation2(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(7, myThreadFactory());

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
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    void testLockWithCancellation3(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(7, myThreadFactory());

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
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    void testLockThreadThatIsAlreadyLocked1(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4, myThreadFactory());

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
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    void testLockThreadThatIsAlreadyLocked2(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4, myThreadFactory());

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
            } catch (InterruptedException | RuntimeException | Error e) { // code-coverage: never hit
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
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3, myThreadFactory());

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
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(6, myThreadFactory());

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(9), 1200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(5), 1210, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 1210, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 1210, TimeUnit.MILLISECONDS);
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
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(6, myThreadFactory());

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


    private static Stream<Arguments> provideArgsFor_testLockExceptionOnLock() {
        return Stream.of(Arguments.of(DoThreadLock.class, false),
                         Arguments.of(DoThreadLockInterruptibly.class, false),
                         Arguments.of(DoThreadTryLock.class, false),
                         Arguments.of(DoThreadTryLockWithTimeoutMillis.class, false),
                         Arguments.of(DoThreadLock.class, true),
                         Arguments.of(DoThreadLockInterruptibly.class, true),
                         Arguments.of(DoThreadTryLock.class, true),
                         Arguments.of(DoThreadTryLockWithTimeoutMillis.class, true));
    }
    
    /**
     * This test starts 3 threads with different priorities, and acquiring a lock on one of them throws.
     * The test verifies that the threads with lower priority still run.
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @MethodSource("provideArgsFor_testLockExceptionOnLock")
    void testLockExceptionOnLock(Class<?> clazz, boolean throwAfter) throws InterruptedException {
        PriorityLock priorityLock
            = new PriorityLock(PriorityLockNamedParams.create()
                                  .setInternalLockCreator(() -> new ThrowAtPrioritySevenLock(/*fair*/ true, /*shouldThrow*/ true, throwAfter)));

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3, myThreadFactory());

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7), 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
            if (!throwAfter) {
                assertThat(doThread.getMessages(),
                        Matchers.contains(
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed", // at 700
                            "end thread with priority 4", // at 1100
                            "end thread with priority 6")); // at 2100
            } else {
                if (!DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
                    assertThat(doThread.getMessages(),
                            Matchers.contains(
                                "end thread with priority 4", // at 1100
                                "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed", // at 1100
                                // at 2100 background task signals that priority 7 is done
                                "end thread with priority 6")); // at 3100
                } else {
                    assertThat(doThread.getMessages(),
                            Matchers.contains(
                                "end thread with priority 4", // at 1100
                                "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed", // at 1100
                                // background tasks are not used for tryLock(time, unit)
                                "end thread with priority 6")); // at 3100
                }
            }
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
    
    /**
     * This test starts 3 threads with different priorities, and acquiring a lock on one of them throws.
     * Trying to signal the failing thread fails after 5 retries, so the thread with lower priority is never signaled, and never runs.
     */
    @Test
    void testLockExceptionOnLockTimeout() throws InterruptedException {
        assertEquals(0, PriorityLock.InitSignalWaitingThread.getNumberOfThreadsToSignal());
        
        PriorityLock priorityLock = new PriorityLock(PriorityLockNamedParams.create()
                                                                            .setInternalLockCreator(() -> new ThrowAtPrioritySevenLock(/*fair*/ true, /*shouldThrow*/ true, /*throwAfter*/ true)));

        DoThread doThread = createDoThread(DoThreadLock.class, priorityLock);
        
        PriorityLock.InitSignalWaitingThread.setThreadPriority(7);
        PriorityLock.InitSignalWaitingThread.setInitialDelayMillis(100);
        PriorityLock.InitSignalWaitingThread.setSecondDelayMillis(200);
        PriorityLock.InitSignalWaitingThread.setMaxDelayMillis(800);
        PriorityLock.InitSignalWaitingThread.setTimeToAcquireInternalLockMillis(10);
        PriorityLock.InitSignalWaitingThread.setMaxRetries(6);
        PriorityLock.InitSignalWaitingThread.setOnFailToSignalWaitingThread(e -> doThread.messages.add("failed to signal condition #7: " + e.toString()));        
        
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(5, myThreadFactory());

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(6), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("just before retry 6 to signal condition #7");
            doThread.messages.add("just before retry 6 to signal condition #7");
        }, 4400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("just after retry 6 to signal condition #7");
            doThread.messages.add("just after retry 6 to signal condition #7");
        }, 4800, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        PriorityLock.InitSignalWaitingThread.setThreadPriority(5);
        PriorityLock.InitSignalWaitingThread.setInitialDelayMillis(1_000);
        PriorityLock.InitSignalWaitingThread.setSecondDelayMillis(2_000);
        PriorityLock.InitSignalWaitingThread.setMaxDelayMillis(21_000);
        PriorityLock.InitSignalWaitingThread.setTimeToAcquireInternalLockMillis(1000);
        PriorityLock.InitSignalWaitingThread.setMaxRetries(5);
        PriorityLock.InitSignalWaitingThread.setOnFailToSignalWaitingThread(null);

        // snaran: test fail
        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "end thread with priority 4", // at 1100
                        "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed",
                        "just before retry 6 to signal condition #7",
                        "failed to signal condition #7: myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed",
                        "just after retry 6 to signal condition #7")); // at 1100
        // tries to signal condition #7 (so that #6 can wake up) at
        // 1200, 1400, 1700, 2200, 3000, 3800, 4600
        // condition #7 fails to be signaled, so thread #6 never wakes up and we don't see "end thread with priority 6"

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,1,0,0,0,0]"));
    }
    
    
    /**
     * This test starts 3 threads with different priorities, and acquiring a lock does not itself throw, but waiting for the higher priority thread to finish throws.
     * The test verifies that the threads with lower priority still run.
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class})
    void testLockExceptionOnAwait(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock
            = new PriorityLock(PriorityLockNamedParams.create()
                                   .setInternalLockCreator(() -> new ThrowAtPrioritySevenAwait(/*fair*/ true, /*shouldThrow*/ true, /*throwAfter*/ true)));

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4, myThreadFactory());

        executor.schedule(() -> doThread.action(4), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3), 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(7), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (DoThreadLock.class.equals(clazz) || DoThreadLockInterruptibly.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "end thread with priority 4", // at 1100
                            "end thread with priority 8", // at 2100
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed", // at 2100
                            "end thread with priority 3")); // at 3100
        } else if (DoThreadTryLockWithTimeoutMillis.class.equals(clazz)) {
            // PriorityLock.tryLock(time, unit) does not call await on the internal condition objects
            // so we never see ThrowAtPrioritySevenException
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "end thread with priority 4", // at 1100
                            "end thread with priority 8", // at 2100
                            "thread with priority 3 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException", // at 2500
                            "end thread with priority 7")); // at 3100
        } else if (DoThreadTryLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "thread with priority 8 encountered exception myutils.util.concurrent.PriorityLockTest$FailedToAcquireLockException",
                            "end thread with priority 4"));
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
    }
    
    
    // ------------------------------------------------------------------------------------------------
    // await tests


    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @EnumSource(Signal.class)
    void testSignalAndSignalAll(Signal signal) throws InterruptedException {
        WaitArg nanos = new WaitArgMillis(TimeUnit.SECONDS.toMillis(4));
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(DoThreadLockInterruptibly.class, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(11, myThreadFactory());

        executor.schedule(() -> doThread.awaitAction(3, null, nanos), 100, TimeUnit.MILLISECONDS); // will wait till 4300
        ScheduledFuture<?> future200 =
        executor.schedule(() -> doThread.awaitAction(4, null, nanos), 200, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future250 =
        executor.schedule(() -> doThread.awaitAction(5, null, nanos), 250, TimeUnit.MILLISECONDS); // will wait will 4250, but thread is interrupted before that time
        executor.schedule(() -> doThread.awaitAction(6, null, nanos), 300, TimeUnit.MILLISECONDS); // will wait will 4250, but thread is interrupted before that time
        executor.schedule(() -> doThread.awaitAction(9, null, nanos), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(9, null, nanos), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(9, null, nanos), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, signal), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(8), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
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
                            "about to signal in thread with priority 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "end thread with priority 9", // at 2600
                            "end thread with priority 8")); // at 3600
            // threads 3, 4, 5, 6, 9, 9 are not woken up as we called condition.signal
            // so we wait for 10 seconds then test ends
            
            assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
            assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,1,1,1,1,0,0,2,0], signalCount=0"));
            
        } else if (signal == Signal.SIGNAL_ALL) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "about to signalAll in thread with priority 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "end thread with priority 9", // at 2600
                            "end thread with priority 9", // at 3600
                            "end thread with priority 9", // at 4600
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

    
    private static Stream<Arguments> provideArgsFor_testSignalAllAndAddWaiters() {
        return Stream.of(Arguments.of(DoThreadLock.class, 2000L),
                         Arguments.of(DoThreadLock.class, 1000L),
                         Arguments.of(DoThreadLockInterruptibly.class, 2000L),
                         Arguments.of(DoThreadLockInterruptibly.class, 1000L));
    }
    
    /**
     * This thread sets up 3 waiting threads of priorities 4, 5, 6 and calls signalAll.
     * While priority 6 is running, add a new thread of priority 7.
     * One might expect that the 3 original threads would be signaled as they were the ones running when signalAll was called,
     * but this test shows that only 6, 5, 7 get woken up.  thread 4 is never signaled.
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @MethodSource("provideArgsFor_testSignalAllAndAddWaiters")
    void testSignalAllAndAddWaiters(Class<?> clazz, long timeAtWhichToAddThread7) throws InterruptedException {
        WaitArg unused = new WaitArgMillis(TimeUnit.SECONDS.toMillis(4));
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(5, myThreadFactory());

        executor.schedule(() -> doThread.awaitAction(4, null, unused), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, unused), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, unused), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, null, unused), timeAtWhichToAddThread7, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        if (timeAtWhichToAddThread7 == 2000) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "about to signalAll in thread with priority 2", // at 1600 - signals 6
                            "end thread with priority 2", // at 1600
                            "end thread with priority 6", // await finishes at 1600 and signals 5 - this message comes at 2600
                            "end thread with priority 5", // await finishes at 2600 and signals 7 - this message comes at 3600
                            "end thread with priority 7")); // at 4600
            // 4 is never signaled
        } else if (timeAtWhichToAddThread7 == 1000) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "about to signalAll in thread with priority 2", // at 1600 - signals 6
                            "end thread with priority 2", // at 1600
                            "end thread with priority 6", // await finishes at 1600 and signals 7 - this message comes at 2600
                            "end thread with priority 7", // await finishes at 2600 and signals 5 - this message comes at 3600
                            "end thread with priority 5")); // at 4600
            // 4 is never signaled
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,1,0,0,0,0,0,0], signalCount=0"));
    }

    
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class})
    void testSignalOnly(Class<?> clazz) throws InterruptedException {
        WaitArg unused = new WaitArgMillis(TimeUnit.SECONDS.toMillis(4));
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(11, myThreadFactory());

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
                            "about to signal in thread with priority 2",
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
                            "about to signal in thread with priority 2",
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

    
    @Test
    void testSignalWithNoAwait() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(DoThreadLockInterruptibly.class, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, myThreadFactory());

        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL), 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 3 changed to 2", // at 1600
                        "about to signal in thread with priority 2",
                        "end thread with priority 2")); // at 1600

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }

    
    @Test
    void testSignalTwiceWithOneAwait() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(DoThreadLockInterruptibly.class, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, myThreadFactory());
        WaitArg unused = new WaitArgMillis(TimeUnit.SECONDS.toMillis(4));

        executor.schedule(() -> doThread.awaitAction(8, null, unused), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to signal condition 2 times");
            doThread.getMessages().add("about to signal 2 times");
            priorityLock.lock();
            try {
                doThread.condition.signal();
                doThread.condition.signal();
            } finally {
                priorityLock.unlock();
            }
        }, 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "about to signal 2 times",
                        "end thread with priority 8")); // at 1600

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }

    
    /**
     * This test sets up 4 threads waiting on a condition.
     * A 5th threads has the lowest priority and calls signal.
     * The test verifies that the thread with the highest priority wins.
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class,
            DoThreadTryLockWithTimeoutNanos.class})
    void testAwait(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(6, myThreadFactory());

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
                            "about to signalAll in thread with priority 2", // at 1600
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
                            "about to signalAll in thread with priority 2", // at 1600
                            "end thread with priority 2"));
        } else if (DoThreadTryLockWithTimeoutMillis.class.equals(clazz) || DoThreadTryLockWithTimeoutNanos.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "about to signalAll in thread with priority 2", // at 1600
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
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadTryLockWithTimeoutMillis.class,
            DoThreadTryLockWithTimeoutNanos.class})
    void testAwaitReturnFalse(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(clazz, priorityLock);
        WaitArg waitArg = new WaitArgMillis(3500);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(8, myThreadFactory());

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
                        "about to signalAll in thread with priority 10",
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
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class,
            DoThreadTryLock.class,
            DoThreadTryLockWithTimeoutMillis.class,
            DoThreadTryLockWithTimeoutNanos.class})
    void testAwaitWithInterrupt(Class<?> clazz) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(true);

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(9, myThreadFactory());

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

        // Thread 4 and 5 encounter InterruptedException before they are signaled
        // and the line levelManager.waitUninterruptiblyForHigherPriorityTasksToFinishFromAwait() it hit, as happens always,
        // and conditions[nextHigherPriorityIndex].awaitUninterruptibly() is hit.
        //
        // In the normal case where the thread is not interrupted levelManager.waitUninterruptiblyForHigherPriorityTasksToFinishFromAwait() is still called,
        // but this thread is already the highest because of the call to levelManager.waitUninterruptiblyForSignal(priority)
        // and signal/signalAll and the finally block of await only signal the highest thread,
        // so level.waitUninterruptiblyForHigherPriorityTasksToFinishFromAwait() does no waiting --
        // i.e. conditions[nextHigherPriorityIndex].awaitUninterruptibly() is hit is not called.
        //
        // An awaiting thread can also wake up spuriously, so then the call conditions[nextHigherPriorityIndex].awaitUninterruptibly()
        // could happen in the normal case, although I have not seen it happen in these unit tests.
        
        if (DoThreadLock.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "about to signalAll in thread with priority 2", // at 1600
                            "end thread with priority 2", // at 1600
                            "end thread with priority 8", // at 2600
                            "thread with priority 7 changed to 1",
                            "end thread with priority 1",
                            "end thread with priority 6",
                            "thread with priority 5 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted", // at 4600
                            "thread with priority 4 encountered exception myutils.util.concurrent.PriorityLockTest$SleepInterruptedException: java.lang.InterruptedException: sleep interrupted", // at 4600
                            "end thread with priority 3")); // at 5600
        } else if (DoThreadLockInterruptibly.class.equals(clazz) || DoThreadTryLockWithTimeoutMillis.class.equals(clazz) || DoThreadTryLockWithTimeoutNanos.class.equals(clazz)) {
            assertThat(doThread.getMessages(),
                    Matchers.contains(
                            "thread with priority 3 changed to 2", // at 1600
                            "about to signalAll in thread with priority 2", // at 1600
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
                            "about to signalAll in thread with priority 2", // at 1600
                            "end thread with priority 2"));
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }


    /**
     * Same as the above except that some threads are cancelled and the cancelled threads throw InterruptedException as soon as possible.
     */
    @Test
    void testAwaitWithEarlyInterrupt() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(PriorityLockNamedParams.create()
                                                         .setInternalReentrantLockCreator(true)
                                                         .setAllowEarlyInterruptFromAwait(true));

        DoThread doThread = createDoThread(DoThreadLockInterruptibly.class, priorityLock);
        WaitArg unused = new WaitArgMillis(TimeUnit.SECONDS.toMillis(4));
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(8, myThreadFactory());

        executor.schedule(() -> doThread.awaitAction(3, null, unused), 100, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future200 = 
        executor.schedule(() -> doThread.awaitAction(4, null, unused), 200, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future250 = 
        executor.schedule(() -> doThread.awaitAction(5, null, unused), 250, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, unused), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(8, null, unused), 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(9), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future200 of priority 4");
            future200.cancel(true);
            logString("about to interrupt thread future250 of priority 5");
            future250.cancel(true);
        }, 1000, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 3 changed to 2", // at 1600
                        "about to signalAll in thread with priority 2", // at 1600
                        "end thread with priority 2", // at 1600
                        "end thread with priority 9", // at 2600
                        "InterruptedException in await of thread with priority 4", // at 2600
                        "end thread with priority 4",
                        "InterruptedException in await of thread with priority 5", // at 2600
                        "end thread with priority 5",
                        "end thread with priority 8", // at 3600
                        "end thread with priority 6", // at 4600
                        "end thread with priority 3")); // at 5600

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }


    /**
     * Test await with the caveat that one thread is coerced into waking up early, a phenomenon called spurious wakeup.
     * This test obtains code coverage on the lines around
     *     but due to the phenomenon of spurious wakeup, a lower priority thread may wake up before it is signaled
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(booleans = {false, true})
    void testAwaitWithSpuriousWakeup(boolean allowEarlyInterruptFromAwait) throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(PriorityLockNamedParams.create()
                                                         .setInternalReentrantLockCreator(true)
                                                         .setAllowEarlyInterruptFromAwait(allowEarlyInterruptFromAwait));

        DoThread doThread = createDoThread(DoThreadLockInterruptibly.class, priorityLock);
        WaitArg unused = new WaitArgMillis(TimeUnit.SECONDS.toMillis(4));
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(9, myThreadFactory());

        GetInternalConditionByReflection spuriousHelper = new GetInternalConditionByReflection(doThread.condition);

        executor.schedule(() -> doThread.awaitAction(3, null, unused), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(4, null, unused), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, unused), 250, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, unused), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(8, null, unused), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("schedule spurious wakeup: about to signal thread 3 which would normally be signaled at 6600");
            priorityLock.lock();
            try {
                spuriousHelper.simulateSpuriousSignal(true);
            } finally {
                priorityLock.unlock();
            }
            logString("end schedule spurious wakeup");
        }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("restore SignaledThreadPriority to zero");
            spuriousHelper.restoreSignaledThreadPriority();
        }, 550, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS); // this sets signaledThreadPriority to 8
        executor.schedule(() -> doThread.action(9), 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 3 changed to 2", // at 1600
                        "about to signalAll in thread with priority 2", // at 1600
                        "end thread with priority 2", // at 1600
                        "end thread with priority 9", // at 2600
                        "end thread with priority 8", // at 3600
                        "end thread with priority 6", // at 4600
                        "end thread with priority 5", // at 5600
                        "end thread with priority 4", // at 6600
                        "end thread with priority 3")); // at 7600

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }

    /**
     * Same as the above except that the thread that encountered spurious wakeup is interrupted.
     */
    @Test
    void testAwaitWithSpuriousWakeupAndInterrupt() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock(PriorityLockNamedParams.create()
                                                         .setInternalReentrantLockCreator(true)
                                                         .setAllowEarlyInterruptFromAwait(true));

        DoThread doThread = createDoThread(DoThreadLockInterruptibly.class, priorityLock);
        WaitArg unused = new WaitArgMillis(TimeUnit.SECONDS.toMillis(4));
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(10, myThreadFactory());

        GetInternalConditionByReflection spuriousHelper = new GetInternalConditionByReflection(doThread.condition);

        ScheduledFuture<?> future100 =
        executor.schedule(() -> doThread.awaitAction(3, null, unused), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(4, null, unused), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, unused), 250, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(6, null, unused), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(8, null, unused), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("schedule spurious wakeup: about to signal thread 3 which would normally be signaled at 6600");
            priorityLock.lock();
            try {
                spuriousHelper.simulateSpuriousSignal(true);
            } finally {
                priorityLock.unlock();
            }
            logString("end schedule spurious wakeup");
        }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("restore SignaledThreadPriority to zero");
            spuriousHelper.restoreSignaledThreadPriority();
        }, 550, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 600, TimeUnit.MILLISECONDS); // this sets signaledThreadPriority to 8
        executor.schedule(() -> doThread.action(9), 700, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString("about to interrupt thread future100 of priority 3");
            future100.cancel(true);
        }, 1000, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 3 changed to 2", // at 1600
                        "about to signalAll in thread with priority 2", // at 1600
                        "end thread with priority 2", // at 1600
                        "end thread with priority 9", // at 2600
                        "InterruptedException in await of thread with priority 3", // at 2600
                        "end thread with priority 3", // at 2600
                        "end thread with priority 8", // at 3600
                        "end thread with priority 6", // at 4600
                        "end thread with priority 5", // at 5600
                        "end thread with priority 4")); // at 6600
        
        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }

    private static class GetInternalConditionByReflection {
        private final Condition priorityLockCondition;
        private final Field signaledThreadPriority; // priorityLockCondition.signaledThreadPriority
        private final Condition internalCondition3; // priorityLockCondition.levelManager.conditions[2]
        private Integer originalSignaledThreadPriority;
        
        GetInternalConditionByReflection(Condition priorityLockCondition) {
            this.priorityLockCondition = priorityLockCondition;

            try {
                signaledThreadPriority = PriorityLockCondition.class.getDeclaredField("signaledThreadPriority");
                signaledThreadPriority.setAccessible(true);

                Class<?> classLevelManager = Class.forName("myutils.util.concurrent.PriorityLock$LevelManager");
                Field fieldLevelManager = PriorityLock.PriorityLockCondition.class.getDeclaredField("levelManager");
                Field fieldConditions = classLevelManager.getDeclaredField("conditions");
                fieldLevelManager.setAccessible(true);
                fieldConditions.setAccessible(true);
                Object levelManager = fieldLevelManager.get(priorityLockCondition);
                Condition[] conditions = (Condition[]) fieldConditions.get(levelManager);
                internalCondition3 = conditions[2];
            } catch (ClassNotFoundException | NoSuchFieldException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }
        }
        
        void simulateSpuriousSignal(boolean setSignaledThreadPriorityToBeHigher) {
            if (setSignaledThreadPriorityToBeHigher) {
                try {
                    originalSignaledThreadPriority = (int) signaledThreadPriority.get(priorityLockCondition);
                    signaledThreadPriority.set(priorityLockCondition, 11);
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
            internalCondition3.signal();
        }
        
        void restoreSignaledThreadPriority() {
            if (originalSignaledThreadPriority != null) {
                try {
                    signaledThreadPriority.set(priorityLockCondition, originalSignaledThreadPriority);
                    originalSignaledThreadPriority = null;
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    
    @Test
    void testAwaitUntil() throws InterruptedException {
        WaitArg deadline = new WaitArgDeadline(new Date(System.currentTimeMillis() + 4000));
        PriorityLock priorityLock = new PriorityLock();
                
        DoThread doThread = createDoThread(DoThreadTryLockWithTimeoutMillis.class, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(9, myThreadFactory());

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
                        "about to signalAll in thread with priority 2", // at 1600
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
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class})
    void testAwaitException1(Class<?> clazz) throws InterruptedException {
        WaitArg millis = new WaitArgMillis(5000);
        PriorityLock priorityLock
            = new PriorityLock(PriorityLockNamedParams.create()
                                   .setInternalLockCreator(() -> new ThrowAtPrioritySevenAwait(/*fair*/ false, /*shouldThrow*/ true, /*throwAfter*/ false)));

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4, myThreadFactory());

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
                        "about to signalAll in thread with priority 2",
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
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(classes = {
            DoThreadLock.class,
            DoThreadLockInterruptibly.class})
    void testAwaitException2(Class<?> clazz) throws InterruptedException {
        WaitArg millis = new WaitArgMillis(5000);
        PriorityLock priorityLock
            = new PriorityLock(PriorityLockNamedParams.create()
                                   .setInternalLockCreator(() -> new ThrowAtPrioritySevenAwait(/*fair*/ false, /*shouldThrow*/ true, /*throwAfter*/ true)));

        DoThread doThread = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(4, myThreadFactory());

        executor.schedule(() -> doThread.awaitAction(4, null, millis), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(7, null, millis), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.awaitAction(5, null, millis), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread.action(3, 2, null, Signal.SIGNAL_ALL), 500, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());

        assertThat(doThread.getMessages(),
                Matchers.contains(
                        "thread with priority 3 changed to 2", 
                        "about to signalAll in thread with priority 2",
                        "end thread with priority 2",
                        "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLockTest$ThrowAtPrioritySevenException: priority 7 not allowed",
                        "end thread with priority 5",
                        "end thread with priority 4"));

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }



    
    private static Stream<Arguments> provideArgsFor_testAwaitException3() {
        return Stream.of(Arguments.of(DoThreadLock.class, false),
                         Arguments.of(DoThreadLock.class, false),
                         Arguments.of(DoThreadLockInterruptibly.class, true),
                         Arguments.of(DoThreadLockInterruptibly.class, true));
    }
    
    /**
     * This test starts 3 threads with different priorities, and lockUninterruptiblyAfterAwait throws an exception.
     * The test verifies that the threads with lower priority still run.
     */
    @ParameterizedTest(name= TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @MethodSource("provideArgsFor_testAwaitException3")
    void testAwaitException3(Class<?> clazz, boolean allowEarlyInterruptFromAwait) throws InterruptedException {
        WaitArg millis = new WaitArgMillis(5000);
        ThrowAtPrioritySevenAwait internalLock = new ThrowAtPrioritySevenAwait(/*fair*/ true, /*shouldThrow*/ false, /*throwAfter*/ false);
        PriorityLock priorityLock = new PriorityLock(PriorityLockNamedParams.create()
                                                         .setInternalLockCreator(() -> internalLock)
                                                         .setAllowEarlyInterruptFromAwait(allowEarlyInterruptFromAwait));

        DoThread doThread1 = createDoThread(clazz, priorityLock);
        DoThread doThread2 = createDoThread(clazz, priorityLock);
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(7, myThreadFactory());

        executor.schedule(() -> doThread1.awaitAction(4, null, millis), 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread1.awaitAction(7, null, millis), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread2.awaitAction(9, null, millis), 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread1.awaitAction(5, null, millis), 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            Thread.currentThread().setPriority(3);
            priorityLock.lock();
            try {
                sleep(1000);
                logString("* about to signalAll both conditions");
                doThread1.messages.add("about to signalAll both conditions in thread with priority " + Thread.currentThread().getPriority());
                doThread1.condition.signalAll();
                doThread2.condition.signalAll();
                logString("signalled both conditions");
            } finally {
                priorityLock.unlock();
                logString("unlock after signal");
            }
            doThread1.messages.add("end thread with priority " + Thread.currentThread().getPriority());
        }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> doThread1.action(10, null, null, null), 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            // thread with priority 7: await is called at 200
            // thread with priority 7: signaled at 1500, lockUninterruptiblyAfterAwait awaits on priority 9 to finish
            // thread with priority 9: signaled at 1500 
            // make lockUninterruptiblyAfterAwait in priority 7 throw in order to get code coverage in code in lockUninterruptiblyAfterAwait
            logString("about to make ThrowAtPrioritySevenAwait throw an exception when await is called");
            internalLock.setShouldThrow(true);
        }, 1400, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages1", doThread1.getMessages());
        prettyPrintList("messages2", doThread2.getMessages());

        assertThat(doThread1.getMessages(),
                Matchers.contains(
                        "about to signalAll both conditions in thread with priority 3", 
                        "end thread with priority 3",
                        "end thread with priority 10",
                        "thread with priority 7 encountered exception myutils.util.concurrent.PriorityLock$MaybeNotHighestThreadAfterAwaitError",
                        "end thread with priority 5",
                        "end thread with priority 4"));

        assertThat(doThread2.getMessages(),
                Matchers.contains(
                        "end thread with priority 9"));

        assertThat(priorityLock.toString(), Matchers.endsWith("[0,0,0,0,0,0,0,0,0,0]"));
        assertThat(doThread1.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
        assertThat(doThread2.conditiontoString(), Matchers.endsWith("levels=[0,0,0,0,0,0,0,0,0,0], signalCount=0"));
    }


    private enum Signal {
        SIGNAL,
        SIGNAL_ALL
    }

    private abstract class DoThread {
        final Lock priorityLock;
        final Condition condition;
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

        private DoThread(Lock priorityLock) {
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
                                logString("* about to call condition.signal");
                                messages.add("about to signal in thread with priority " + currentThread.getPriority());
                                condition.signal();
                                break;
                            case SIGNAL_ALL:
                                logString("* about to call condition.signalAll");
                                messages.add("about to signalAll in thread with priority " + currentThread.getPriority());
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
        void awaitAction(int priority, Integer newPriority, @Nonnull WaitArg waitArg) {
            final Thread currentThread = Thread.currentThread();
            currentThread.setPriority(priority);
            logString("start " + priorityLock.toString() + " " + conditiontoString());
            try {
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
                } catch (RuntimeException | PriorityLock.MaybeNotHighestThreadAfterAwaitError e) {
                    logString("* caught exception " + e.toString());
                    if (shouldLogCallstack(e)) {
                        e.printStackTrace(System.out);
                    }
                    messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
                } finally {
                    priorityLock.unlock();
                }
            } catch (InterruptedException | RuntimeException e) { // CodeCoverage: never hit
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
                    || (e instanceof PriorityLock.MaybeNotHighestThreadAfterAwaitError)) {
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
        private DoThreadLock(Lock priorityLock) {
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
    }

    private class DoThreadLockInterruptibly extends DoThread {
        DoThreadLockInterruptibly(Lock priorityLock) {
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
    }

    /**
     * Helper class to test tryLock.
     * getLock sleeps for 50ms so that if several threads call tryLock at the same time, we can be sure that the one with the highest priority goes first.
     *
     */
    private class DoThreadTryLock extends DoThread {
        private DoThreadTryLock(Lock priorityLock) {
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
    }

    private abstract class AbstractDoThreadTryLockWithTimeout extends DoThread {
        private static final long DEFAULT_WAIT_TIME_MILLIS = 2000;

        private AbstractDoThreadTryLockWithTimeout(Lock priorityLock) {
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

        private long fallbackToDefault(Long input) {
            if (input != null) {
                return input;
            }
            return DEFAULT_WAIT_TIME_MILLIS;
        }
        
        protected abstract boolean invokeAwait(long awaitMillis) throws InterruptedException;
    }
    
    private class DoThreadTryLockWithTimeoutMillis extends AbstractDoThreadTryLockWithTimeout {
        private DoThreadTryLockWithTimeoutMillis(Lock priorityLock) {
            super(priorityLock);
        }

        @Override
        protected boolean invokeAwait(long awaitMillis) throws InterruptedException {
            return condition.await(awaitMillis, TimeUnit.MILLISECONDS);
        }
    }
    
    private class DoThreadTryLockWithTimeoutNanos extends AbstractDoThreadTryLockWithTimeout {
        private DoThreadTryLockWithTimeoutNanos(Lock priorityLock) {
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

        private boolean shouldThrow;
        private final boolean throwAfter;

        ThrowAtPrioritySevenLock(boolean fair, boolean shouldThrow, boolean throwAfter) {
            super(fair);
            this.shouldThrow = shouldThrow;
            this.throwAfter = throwAfter;
        }
        
        @Override
        public void lock() {
            throwIfNecessaryBefore();
            super.lock();
            throwIfNecessaryAfter();
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            throwIfNecessaryBefore();
            super.lockInterruptibly();
            throwIfNecessaryAfter();
        }

        @Override
        public boolean tryLock() {
            throwIfNecessaryBefore();
            boolean acquired = super.tryLock();
            throwIfNecessaryAfter();
            return acquired;
        }

        @Override
        public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
            throwIfNecessaryBefore();
            boolean acquired = super.tryLock(time, unit);
            throwIfNecessaryAfter();
            return acquired;
        }

        @Override
        public void unlock() {
            super.unlock();
        }

        @Override
        public @Nonnull Condition newCondition() {
            return super.newCondition();
        }        
        
        private void throwIfNecessaryBefore() {
            if (shouldThrow && !throwAfter && Thread.currentThread().getPriority() == 7) {
                throw new ThrowAtPrioritySevenException();
            }
        }

        private void throwIfNecessaryAfter() {
            if (shouldThrow && throwAfter && Thread.currentThread().getPriority() == 7) {
                if (isHeldByCurrentThread()) {
                    super.unlock();
                }
                throw new ThrowAtPrioritySevenException();
            }
        }
    }

    private static class ThrowAtPrioritySevenAwait extends ReentrantLock implements DoNotLogCallStack {
        private static final long serialVersionUID = 1L;
        
        private boolean shouldThrow;
        private final boolean throwAfter;

        ThrowAtPrioritySevenAwait(boolean fair, boolean shouldThrow, boolean throwAfter) {
            super(fair);
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
            public boolean awaitUntil(@Nonnull Date deadline) throws InterruptedException {
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

    private DoThread createDoThread(Class<?> clazz, Lock priorityLock) {
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
    
    private static ThreadFactory myThreadFactory() {
        AtomicInteger threadNumber = new AtomicInteger();
        return runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A'));
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
    
}
