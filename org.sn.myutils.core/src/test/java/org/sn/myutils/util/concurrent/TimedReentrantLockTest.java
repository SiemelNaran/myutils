package org.sn.myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.sn.myutils.testutils.TestUtil.myThreadFactory;
import static org.sn.myutils.testutils.TestUtil.sleep;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


@SuppressWarnings("ResultOfMethodCallIgnored")
public class TimedReentrantLockTest {
    private static long startOfAllTests;
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

    @BeforeAll
    static void onStartAllTests() {
        startOfAllTests = System.currentTimeMillis();
        System.out.println("start all tests");
        System.out.println("--------------------------------------------------------------------------------");
    }

    @AfterAll
    static void printAllTestsFinished() {
        Duration timeTaken = Duration.ofMillis(System.currentTimeMillis() - startOfAllTests);
        System.out.println("--------------------------------------------------------------------------------");
        System.out.println("all tests finished in " + timeTaken);
    }
    
    @Test
    public void testLock() throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(2, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        service.schedule(() -> {
            logString("start thread 1");
            lock.lock(); // sets lockStartTime to 300
            try {
                logString("acquired lock in thread 1");
                sleep(500);
                lock.lock(); // does not set lockStartTime to 800 as lock is already locked
                try {
                    logString("reacquired lock in thread 1");
                    sleep(500);
                } finally {
                    try {
                        sleep(500);
                    } finally {
                        logString("about to unlock in thread 1");
                        lock.unlock(); // runningTime unchanged since locked is locked twice
                    }
                }
            } finally {
                try {
                    sleep(500);
                } finally {
                    logString("about to unlock in thread 1");
                    lock.unlock(); // runningTime += 2300-300=2000
                }
            }
            logString("end thread 1");
        }, 300, TimeUnit.MILLISECONDS);
        
        service.schedule(() -> {
            logString("start thread 2");
            lock.lock();
            try {
                logString("acquired lock in thread 2");
                sleep(1000);
            } finally {
                lock.unlock();
            }
            logString("end thread 2");
        }, 500, TimeUnit.MILLISECONDS);
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        assertEquals(1800, lock.getTotalWaitTime().toMillis(), 50.0); // thread 1 ends at 2300, thread 2 waiting from 500, so waitTime=2300-500=1800
        assertEquals(3000, lock.getTotalLockRunningTime().toMillis(), 50.0); // thread 1 runs for 2000ms, thread 2 runs for 1000ms
        assertEquals(300, lock.getTotalIdleTime().toMillis(), 50.0); // as lock waiting from time 0ms to 300ms when thread 1 starts
    }
    

    @Test
    public void testLockInterruptibly() throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(3, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        service.schedule(() -> {
            logString("start thread 1");
            try {
                lock.lockInterruptibly(); // sets lockStartTime to 300
                try {
                    logString("acquired lock in thread 1");
                    sleep(500);
                    lock.lockInterruptibly(); // does not set lockStartTime to 800 as lock is already locked
                    try {
                        logString("reacquired lock in thread 1");
                        sleep(500);
                    } finally {
                        try {
                            sleep(500);
                        } finally {
                            logString("about to unlock in thread 1");
                            lock.unlock(); // runningTime += 2300-300=2000
                        }
                    }
                } finally {
                    try {
                        sleep(500);
                    } finally {
                        logString("about to unlock in thread 1");
                        lock.unlock(); // runningTime += 2300-300=2000
                    }
                }
            } catch (InterruptedException e) {
                logString("exception in thread 1 = " + e);
            }
            logString("end thread 1");
        }, 300, TimeUnit.MILLISECONDS);
        
        ScheduledFuture<?> future = service.schedule(() -> {
            logString("start thread 2");
            try {
                lock.lockInterruptibly();
                try {
                    logString("acquired lock in thread 2");
                    sleep(500);
                    lock.lock();
                    try {
                        logString("reacquired lock in thread 2");
                        sleep(500);
                    } finally {
                        lock.unlock();
                    }
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                logString("exception in thread 2 = " + e);
            }
            logString("end thread 2");
        }, 500, TimeUnit.MILLISECONDS);
        
        service.schedule(() -> {
            logString("about to cancel thread 2");
            future.cancel(true);
        }, 700, TimeUnit.MILLISECONDS);
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        assertEquals(200, lock.getTotalWaitTime().toMillis(), 50.0); // thread 2 cancelled at 700 so never runs, and waited only from time 500ms to time 700ms
        assertEquals(2000, lock.getTotalLockRunningTime().toMillis(), 50.0); // only thread 1 runs
        assertEquals(300, lock.getTotalIdleTime().toMillis(), 50.0); // as lock waiting from time 0ms to 300ms when thread 1 starts
    }
    

    @Test
    public void testTryLock() throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(2, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        service.schedule(() -> {
            logString("start thread 1");
            boolean acquired = lock.tryLock(); // sets lockStartTime to 300
            if (acquired) {
                logString("acquired lock in thread 1");
                try {
                    sleep(500);
                    assertTrue(lock.tryLock()); // does not set lockStartTime to 800 as lock is already locked
                    try {
                        logString("reacquired lock in thread 1");
                        sleep(500);
                    } finally {
                        try {
                            sleep(500);
                        } finally {
                            logString("about to unlock in thread 1");
                            lock.unlock(); // runningTime += 2300-300=2000
                        }
                    }
                } finally {
                    try {
                        sleep(500);
                    } finally {
                        logString("about to unlock in thread 1");
                        lock.unlock(); // runningTime += 2300-300=2000
                    }
                }
            }
            logString("end thread 1");
        }, 300, TimeUnit.MILLISECONDS);
        
        service.schedule(() -> {
            logString("start thread 2");
            boolean acquired = lock.tryLock();
            if (acquired) {
                logString("acquired lock in thread 2");
                try {
                    sleep(1000);
                } finally {
                    lock.unlock();
                }
            }
            logString("end thread 2");
        }, 500, TimeUnit.MILLISECONDS);
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        assertEquals(0, lock.getTotalWaitTime().toMillis(), 10.0); // because tryLock returns right away it never increments waitTime
        assertEquals(2000, lock.getTotalLockRunningTime().toMillis(), 50.0); // because only thread 1 runs
        assertEquals(300, lock.getTotalIdleTime().toMillis(), 50.0); // as lock waiting from time 0ms to 300ms when thread 1 starts
    }


    @Test
    public void testTryLockWithArgs() throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(3, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        service.schedule(() -> {
            logString("start thread 1");
            try {
                boolean acquired = lock.tryLock(400, TimeUnit.MILLISECONDS); // sets lockStartTime to 300
                if (acquired) {
                    logString("acquired lock in thread 1");
                    try {
                        sleep(500);
                        assertTrue(lock.tryLock(400, TimeUnit.MILLISECONDS)); // does not set lockStartTime to 800 as lock is already locked
                        try {
                            logString("reacquired lock in thread 1");
                            sleep(500);
                        } finally {
                            try {
                                sleep(500);
                            } finally {
                                logString("about to unlock in thread 1");
                                lock.unlock(); // runningTime += 2300-300=2000
                            }
                        }
                    } finally {
                        try {
                            sleep(500);
                        } finally {
                            logString("about to unlock in thread 1");
                            lock.unlock(); // runningTime += 2300-300=2000
                        }
                    }
                }
            } catch (InterruptedException e) {
                logString("exception in thread 1 = " + e);
            }
            logString("end thread 1");
        }, 300, TimeUnit.MILLISECONDS);
        
        service.schedule(() -> {
            logString("start thread 2");
            try {
                boolean acquired = lock.tryLock(400, TimeUnit.MILLISECONDS);
                if (acquired) {
                    try {
                        sleep(1000);
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                logString("exception in thread 2 = " + e);
            }
            logString("end thread 2");
        }, 500, TimeUnit.MILLISECONDS);
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        assertEquals(400, lock.getTotalWaitTime().toMillis(), 50.0); // as lock2 starts waiting at 300ms and waits till 700ms before throwing, so wait time is 400ms 
        assertEquals(2000, lock.getTotalLockRunningTime().toMillis(), 50.0);
        assertEquals(300, lock.getTotalIdleTime().toMillis(), 50.0); // as lock waiting from time 0ms to 300ms when thread 1 starts
    }
    
    @FunctionalInterface
    private interface AwaitFunction {
        void accept(Condition condition) throws InterruptedException;
    }
    
    @SuppressWarnings("checkstyle:indentation")
    private static final AwaitFunction[] AWAIT_FUNCTIONS = {
        Condition::awaitUninterruptibly,
        Condition::await,
        condition -> condition.await(250, TimeUnit.MILLISECONDS),
        condition -> condition.awaitNanos(TimeUnit.MILLISECONDS.toNanos(250)),
        condition -> condition.awaitUntil(new Date(System.currentTimeMillis() + 250))
    };
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4})
    public void testAllAwaitFunctions(int awaitFunctionIndex) throws InterruptedException {
        AwaitFunction awaitFunction = AWAIT_FUNCTIONS[awaitFunctionIndex];
        
        ScheduledExecutorService service = Executors.newScheduledThreadPool(3, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        Condition condition = lock.newCondition();
        
        
        ScheduledFuture<?> thread1 = service.schedule(() -> {
            logString("start thread 1");
            lock.lock();
            try {
                logString("acquired lock in thread 1");
                sleep(500);
                logString("about to await in thread 1");
                try {
                    awaitFunction.accept(condition);
                } catch (InterruptedException e) {
                    logString("caught InterruptedException in thread 1");
                }
                logString("await done in thread 1");
                if (Thread.interrupted()) {
                    logString("thread 1 is interrupted, clearing interrupted flag");
                }
                sleep(500);
            } finally {
                lock.unlock();
            }
            logString("end thread 1");
        }, 300, TimeUnit.MILLISECONDS);
        
        service.schedule(() -> {
            logString("start thread 2");
            lock.lock();
            try {
                logString("acquired lock in thread 2");
                sleep(1000);
                condition.signal();
            } finally {
                lock.unlock();
            }
            logString("end thread 2");
        }, 1000, TimeUnit.MILLISECONDS);
        
        service.schedule(() -> {
            logString("start thread 3");
            logString("about to interrupt thread 1");
            thread1.cancel(true);
            logString("end thread 3");
        }, 1500, TimeUnit.MILLISECONDS);
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        assertEquals(1200, lock.getTotalWaitTime().toMillis(), 50.0); // thread 1 starts waiting at 300+500=800ms and acquires lock at 2000ms
        assertEquals(2000, lock.getTotalLockRunningTime().toMillis(), 50.0); // each thread runs for 1000, so net 2000ms
        assertEquals(500, lock.getTotalIdleTime().toMillis(), 50.0); // as lock waiting from time 0ms to 300ms, and 800ms to 1000ms
    }
    
    /**
     * Same as above test except that we only test condition.await().
     * In the previous test thread1 calls await and is interrupted while waiting for a signal.
     * Here the thread is interrupted after it is signaled.
     * We also call signalAll to get code coverage in signalAll.
     */
    @Test
    public void testAwait() throws InterruptedException {
        AwaitFunction awaitFunction = AWAIT_FUNCTIONS[1];
        
        ScheduledExecutorService service = Executors.newScheduledThreadPool(3, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        Condition condition = lock.newCondition();
        
        
        ScheduledFuture<?> thread1 = service.schedule(() -> {
            logString("start thread 1");
            lock.lock();
            try {
                logString("acquired lock in thread 1");
                sleep(500);
                logString("about to await in thread 1");
                try {
                    awaitFunction.accept(condition);
                } catch (InterruptedException e) {
                    logString("caught InterruptedException in thread 1");
                }
                logString("await done in thread 1");
                if (Thread.interrupted()) {
                    logString("thread 1 is interrupted, clearing interrupted flag");
                }
                sleep(500);
            } finally {
                lock.unlock();
            }
            logString("end thread 1");
        }, 300, TimeUnit.MILLISECONDS);
        
        service.schedule(() -> {
            logString("start thread 2");
            lock.lock();
            try {
                logString("acquired lock in thread 2");
                sleep(1000);
                condition.signalAll();
            } finally {
                lock.unlock();
            }
            logString("end thread 2");
        }, 1000, TimeUnit.MILLISECONDS);
        
        service.schedule(() -> {
            logString("start thread 3");
            logString("about to interrupt thread 1");
            thread1.cancel(true);
            logString("end thread 3");
        }, 2500, TimeUnit.MILLISECONDS);
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        assertEquals(1200, lock.getTotalWaitTime().toMillis(), 50.0); // thread 1 starts waiting at 300+500=800ms and acquires lock at 2000ms
        assertEquals(2000, lock.getTotalLockRunningTime().toMillis(), 50.0); // each thread runs for 1000, so net 2000ms
        assertEquals(500, lock.getTotalIdleTime().toMillis(), 50.0); // as lock waiting from time 0ms to 300ms, and 800ms to 1000ms
    }
    
    private void logString(String message) {
        Thread currentThread = Thread.currentThread();
        System.out.println((System.currentTimeMillis() - startOfTime) + " : " + currentThread.getName() + " : " + message);
    }
}
