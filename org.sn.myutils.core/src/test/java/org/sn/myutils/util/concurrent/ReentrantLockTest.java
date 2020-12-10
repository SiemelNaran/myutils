package org.sn.myutils.util.concurrent;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.sn.myutils.testutils.TestUtil.myThreadFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


/**
 * The purpose of these tests are to show how ReentrantLock works in special circumstances,
 * as the behavior of PriorityLock is based on ReentrantLock.
 */
public class ReentrantLockTest {
    private long startOfTime;
    
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
     * This test interrupts the current thread then calls lockInterruptibly,
     * verifying that an InterruptedException is thrown.
     */
    @ParameterizedTest
    @ValueSource(strings = { "lockInterruptibly", "lock" })
    void testLockInterruptedThread(String lockFunction) throws InterruptedException {
        ReentrantLock reentrantLock = new ReentrantLock();
        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, myThreadFactory());
        AtomicBoolean interruptedExceptionThrown = new AtomicBoolean();
        AtomicBoolean threadStillInterrupted = new AtomicBoolean();

        executor.schedule(() -> {
            logStringPlus(reentrantLock, "start thread");
            logStringPlus(reentrantLock, "interrupt thread");
            Thread.currentThread().interrupt();
            try {
                logStringPlus(reentrantLock, "about to lock");
                assertTrue(Thread.currentThread().isInterrupted());
                switch (lockFunction) {
                    case "lockInterruptibly":
                        reentrantLock.lockInterruptibly();
                        break;
                    case "lock":
                        reentrantLock.lock();
                        break;

                    default:
                        throw new UnsupportedOperationException();
                }
                if ("lock".equals(lockFunction)) {
                    threadStillInterrupted.set(Thread.currentThread().isInterrupted());
                }
            } catch (InterruptedException ignored) {
                assertFalse(Thread.currentThread().isInterrupted());
                logStringPlus(reentrantLock, "caught InterruptedException");
                interruptedExceptionThrown.set(true);
                threadStillInterrupted.set(Thread.currentThread().isInterrupted());
            }
            logStringPlus(reentrantLock, "end thread");
        }, 100, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        switch (lockFunction) {
            case "lockInterruptibly":
                assertTrue(interruptedExceptionThrown.get());
                assertFalse(threadStillInterrupted.get());
                break;
            case "lock":
                assertTrue(threadStillInterrupted.get());
                break;
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * The purpose of this test is to demonstrate how await with timeout works.
     * await(time, unit) can wait for more than time specified.
     * After await(time, unit) finishes and more than 'time' has passed,
     * the current thread still holds the lock, but await returns false,
     * indicating that the calling function can release the lock and abort further processing.
     * No exception is thrown.
     */
    @Test
    void testReentrantLockAwait() throws InterruptedException {
        ReentrantLock reentrantLock = new ReentrantLock();
        Condition condition = reentrantLock.newCondition();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(2, myThreadFactory());
        
        AtomicBoolean assertionsRun = new AtomicBoolean();

        executor.schedule(() -> {
            Boolean acquired = null;
            logStringPlus(reentrantLock, "start thread 1");
            reentrantLock.lock();
            try {
                logStringPlus(reentrantLock, "acquired lock in thread 1");
                logStringPlus(reentrantLock, "about to await for 500ms in thread 1");
                acquired = condition.await(500, TimeUnit.MILLISECONDS);
                logStringPlus(reentrantLock, "right after await of 500ms in thread 1: acquired=" + acquired);
                sleep(1000);
                logStringPlus(reentrantLock, "end thread 1");
            } catch (InterruptedException e) {
                logStringPlus(reentrantLock, "caught " + e.toString());
            } finally {
                logStringPlus(reentrantLock, "running assertions in thread 1");
                assertTrue(reentrantLock.isLocked());
                assertTrue(reentrantLock.isHeldByCurrentThread());
                assertEquals(Boolean.FALSE, acquired);
                assertionsRun.set(true);
                reentrantLock.unlock();
                logStringPlus(reentrantLock, "unlock in thread 1");
            }
        }, 100, TimeUnit.MILLISECONDS);

        executor.schedule(() -> {
            logStringPlus(reentrantLock, "start thread 2");
            reentrantLock.lock();
            try {
                logStringPlus(reentrantLock, "acquired lock in thread 1");
                sleep(1000);
                logStringPlus(reentrantLock, "end thread 2");
            } catch (InterruptedException e) {
                logStringPlus(reentrantLock, "caught " + e.toString());
            } finally {
                reentrantLock.unlock();
                logStringPlus(reentrantLock, "unlock in thread 2");
            }
        }, 200, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        
        assertTrue(assertionsRun.get());
    }

    /**
     * The purpose of this thread is to demonstrate how await works when the thread is interrupted.
     * The awaiting thread is interrupted.
     * Upon returning from await with exception the thread still holds the lock.
     */
    @Test
    void testReentrantLockAwaitInterruptedException() throws InterruptedException {
        ReentrantLock reentrantLock = new ReentrantLock();
        Condition condition = reentrantLock.newCondition();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3, myThreadFactory());

        AtomicBoolean assertionsRun = new AtomicBoolean();

        ScheduledFuture<?> future1 =
        executor.schedule(() -> {
            logStringPlus(reentrantLock, "start thread 1");
            reentrantLock.lock();
            try {
                logStringPlus(reentrantLock, "acquired lock in thread 1");
                logStringPlus(reentrantLock, "about to await for 500ms in thread 1");
                condition.await();
                logStringPlus(reentrantLock, "right after await of 500ms in thread 1");
                sleep(1000);
                logStringPlus(reentrantLock, "end thread 1");
            } catch (InterruptedException e) {
                logStringPlus(reentrantLock, "caught " + e.toString());
            } finally {
                logStringPlus(reentrantLock, "running assertions in thread 1");
                assertTrue(reentrantLock.isLocked());
                assertTrue(reentrantLock.isHeldByCurrentThread());
                assertFalse(Thread.currentThread().isInterrupted());
                assertionsRun.set(true);
                reentrantLock.unlock();
                logStringPlus(reentrantLock, "unlock in thread 1");
            }
        }, 100, TimeUnit.MILLISECONDS);

        executor.schedule(() -> {
            logStringPlus(reentrantLock, "start thread 2");
            reentrantLock.lock();
            try {
                logStringPlus(reentrantLock, "acquired lock in thread 2");
                sleep(1000);
                logStringPlus(reentrantLock, "no signal sent in thread 2");
                logStringPlus(reentrantLock, "end thread 2");
            } catch (InterruptedException e) {
                logStringPlus(reentrantLock, "caught " + e.toString());
            } finally {
                reentrantLock.unlock();
                logStringPlus(reentrantLock, "unlock in thread 2");
            }
        }, 200, TimeUnit.MILLISECONDS);

        executor.schedule(() -> {
            logStringPlus(reentrantLock, "about to interrupt thread 1");
            future1.cancel(true);
        }, 400, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        
        assertTrue(assertionsRun.get());
    }

    /**
     * The purpose of this thread is to demonstrate how await works when the thread is interrupted.
     * The awaiting thread is interrupted but awaitUninterruptibly was called.
     * Upon returning from await with exception the thread still holds the lock.
     */
    @Test
    void testReentrantLockAwaitUninterruptiblyInterruptedException() throws InterruptedException {
        ReentrantLock reentrantLock = new ReentrantLock();
        Condition condition = reentrantLock.newCondition();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3, myThreadFactory());

        AtomicBoolean assertionsRun = new AtomicBoolean();

        ScheduledFuture<?> future1 =
        executor.schedule(() -> {
            logStringPlus(reentrantLock, "start thread 1");
            reentrantLock.lock();
            try {
                logStringPlus(reentrantLock, "acquired lock in thread 1");
                logStringPlus(reentrantLock, "about to await in thread 1");
                condition.awaitUninterruptibly();
                logStringPlus(reentrantLock, "right after await in thread 1");
                sleep(1000);
                logStringPlus(reentrantLock, "end thread 1");
            } catch (InterruptedException e) {
                logStringPlus(reentrantLock, "caught " + e.toString());
            } finally {
                logStringPlus(reentrantLock, "running assertions in thread 1");
                assertTrue(reentrantLock.isLocked());
                assertTrue(reentrantLock.isHeldByCurrentThread());
                assertFalse(Thread.currentThread().isInterrupted());
                assertionsRun.set(true);
                reentrantLock.unlock();
                logStringPlus(reentrantLock, "unlock in thread 1");
            }
        }, 100, TimeUnit.MILLISECONDS);

        executor.schedule(() -> {
            logStringPlus(reentrantLock, "start thread 2");
            reentrantLock.lock();
            try {
                logStringPlus(reentrantLock, "acquired lock in thread 2");
                sleep(1000);
                logStringPlus(reentrantLock, "about to signal in thread 2");
                condition.signal();
                logStringPlus(reentrantLock, "end thread 2");
            } catch (InterruptedException e) {
                logStringPlus(reentrantLock, "caught " + e.toString());
            } finally {
                reentrantLock.unlock();
                logStringPlus(reentrantLock, "unlock in thread 2");
            }
        }, 200, TimeUnit.MILLISECONDS);

        executor.schedule(() -> {
            logStringPlus(reentrantLock, "about to interrupt thread 1");
            future1.cancel(true);
        }, 400, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10_00, TimeUnit.SECONDS);
        
        assertTrue(assertionsRun.get());
    }
    
    private void logStringPlus(ReentrantLock reentrantLock, String message) {
        message += " :";
        message += " isLocked=" + reentrantLock.isLocked();
        message += " isHeldByCurrentThread=" + reentrantLock.isHeldByCurrentThread();
        logString(message);
    }
    
    private void logString(String message) {
        Thread currentThread = Thread.currentThread();
        boolean isInterrupted = Thread.currentThread().isInterrupted();
        System.out.println(
                String.format("%4d", System.currentTimeMillis() - startOfTime)
                + " : " + currentThread.getName() + " at priority " + currentThread.getPriority()
                + " : currentThread.isInterrupted=" + currentThread.isInterrupted()
                + " : " + message);
        boolean isInterruptedAfter = Thread.currentThread().isInterrupted();
        if (isInterrupted && !isInterruptedAfter) {
            // this code block hit when running mvn test
            System.out.println("[WARNING] System.out.println cleared interrupted flag ... re-interrupting thread");
            Thread.currentThread().interrupt();
        }
    }
    
}
