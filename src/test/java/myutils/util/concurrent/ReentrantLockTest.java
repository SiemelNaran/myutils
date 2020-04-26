package myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;


/**
 * The purpose of these tests are to show how ReentrantLock works in special circumstances,
 * as the behavior of PriorityLock is based on ReentrantLock.
 */
public class ReentrantLockTest {
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
     * This test interrupts the current thread then calls lockInterruptibly,
     * verifying that an InterruptedException is thrown.
     */
    @Test
    void testLockInterruptedThread() throws InterruptedException {
        ReentrantLock reentrantLock = new ReentrantLock();

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(1, myThreadFactory());
        
        AtomicBoolean interruptedExceptionThrown = new AtomicBoolean();

        executor.schedule(() -> {
            logStringPlus(reentrantLock, "start thread");
            logStringPlus(reentrantLock, "interrupt thread");
            Thread.currentThread().interrupt();
            try {
                logStringPlus(reentrantLock, "about to lock");
                reentrantLock.lockInterruptibly();
            } catch (InterruptedException ignored) {
                logStringPlus(reentrantLock, "caught InterruptedException");
                interruptedExceptionThrown.set(true);
            }
            logStringPlus(reentrantLock, "end thread");
        }, 100, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        
        assertTrue(interruptedExceptionThrown.get());
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
                assertEquals(false, Thread.currentThread().isInterrupted()); 
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
            } finally {
                logStringPlus(reentrantLock, "running assertions in thread 1");
                assertTrue(reentrantLock.isLocked());
                assertTrue(reentrantLock.isHeldByCurrentThread());
                assertEquals(false, Thread.currentThread().isInterrupted()); 
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

    private static class SleepInterruptedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        
        SleepInterruptedException(InterruptedException cause) {
            super(cause);
        }
    }

    private void logStringPlus(ReentrantLock reentrantLock, String message) {
        message += " :";
        message += " isLocked=" + reentrantLock.isLocked() + " isHeldByCurrentThread=" + reentrantLock.isHeldByCurrentThread();
        logString(message);
    }
    
    private void logString(String message) {
        Thread currentThread = Thread.currentThread();
        System.out.println(
                String.format("%4d", System.currentTimeMillis() - startOfTime)
                + " : " + currentThread.getName() + " at priority " + currentThread.getPriority()
                + " : " + message);
    }
    
}
