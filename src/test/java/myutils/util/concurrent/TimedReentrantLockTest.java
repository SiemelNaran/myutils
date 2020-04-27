package myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.Date;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


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
        System.out.println("all tests finished in " + timeTaken);
    }
    
    @Test
    public void testLock() throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(2, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        service.schedule(() -> {
            logString("start thread 1");
            lock.lock();
            try {
                logString("acquired lock in thread 1");
                sleep(500);
                lock.lock();
                try {
                    logString("reacquired lock in thread 1");
                    sleep(500);
                } finally {
                    try {
                        sleep(500);
                    } finally {
                        lock.unlock();
                    }
                }
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
            } finally {
                lock.unlock();
            }
            logString("end thread 2");
        }, 500, TimeUnit.MILLISECONDS);
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        assertEquals(1300, lock.getTotalWaitTime().toMillis(), 40.0); // thread 1 ends at 1800, thread 2 waiting from 500, so waitTime=1800-500=1300
        assertEquals(2500, lock.getTotalLockRunningTime().toMillis(), 40.0); // each thread runs for 1000, set net 2000ms
        assertEquals(300, lock.getTotalIdleTime().toMillis(), 40.0); // as lock waiting from time 0ms to 300ms when thread 1 starts
    }
    

    @Test
    public void testLockInterruptibly() throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(3, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        service.schedule(() -> {
            logString("start thread 1");
            try {
                lock.lockInterruptibly();
                try {
                    logString("acquired lock in thread 1");
                    sleep(500);
                    lock.lock();
                    try {
                        logString("reacquired lock in thread 1");
                        sleep(500);
                    } finally {
                        lock.unlock();
                    }
                } finally {
                    try {
                        sleep(500);
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                logString("exception in thread 1 = " + e.toString());
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
                        logString("reacquired lock in thread 1");
                        sleep(500);
                    } finally {
                        lock.unlock();
                    }
                } finally {
                    lock.unlock();
                }
            } catch (InterruptedException e) {
                logString("exception in thread 2 = " + e.toString());
            }
            logString("end thread 2");
        }, 500, TimeUnit.MILLISECONDS);
        
        service.schedule(() -> {
            logString("about to cancel thread 2");
            future.cancel(true);
        }, 700, TimeUnit.MILLISECONDS);
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        assertEquals(200, lock.getTotalWaitTime().toMillis(), 40.0); // thread 2 cancelled at 700 so never runs, and waited only from time 500ms to time 700ms
        assertEquals(1000, lock.getTotalLockRunningTime().toMillis(), 40.0); // only thread1 runs
        assertEquals(300, lock.getTotalIdleTime().toMillis(), 40.0); // as lock waiting from time 0ms to 300ms when thread 1 starts
    }
    

    @Test
    public void testTryLock() throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(2, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        service.schedule(() -> {
            logString("start thread 1");
            boolean acquired = lock.tryLock();
            if (acquired) {
                logString("acquired lock in thread 1");
                try {
                    sleep(500);
                    assertTrue(lock.tryLock());
                    try {
                        logString("reacquired lock in thread 1");
                        sleep(500);
                    } finally {
                        lock.unlock();
                    }
                } finally {
                    lock.unlock();
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
        assertEquals(1000, lock.getTotalLockRunningTime().toMillis(), 40.0); // because only thread 1 runs for 500ms 
        assertEquals(300, lock.getTotalIdleTime().toMillis(), 40.0); // as lock waiting from time 0ms to 300ms when thread 1 starts
    }


    @Test
    public void testTryLockWithArgs() throws InterruptedException {
        ScheduledExecutorService service = Executors.newScheduledThreadPool(3, myThreadFactory());
        
        TimedReentrantLock lock = new TimedReentrantLock();
        service.schedule(() -> {
            logString("start thread 1");
            try {
                boolean acquired = lock.tryLock(400, TimeUnit.MILLISECONDS);
                if (acquired) {
                    logString("acquired lock in thread 1");
                    try {
                        sleep(500);
                        assertTrue(lock.tryLock());
                        try {
                            logString("reacquired lock in thread 1");
                            sleep(500);
                        } finally {
                            lock.unlock();
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException e) {
                logString("exception in thread 1 = " + e.toString());
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
                logString("exception in thread 2 = " + e.toString());
            }
            logString("end thread 2");
        }, 500, TimeUnit.MILLISECONDS);
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        assertEquals(400, lock.getTotalWaitTime().toMillis(), 40.0); // as lock2 starts waiting at 300ms and waits till 700ms before throwing, so wait time is 400ms 
        assertEquals(1000, lock.getTotalLockRunningTime().toMillis(), 40.0);
        assertEquals(300, lock.getTotalIdleTime().toMillis(), 40.0); // as lock waiting from time 0ms to 300ms when thread 1 starts
    }
    
    @FunctionalInterface
    private interface AwaitFunction {
        void accept(Condition condition) throws InterruptedException;
    }
    
    @SuppressWarnings("checkstyle:indentation")
    private static final AwaitFunction[] AWAIT_FUNCTIONS = {
        condition -> condition.awaitUninterruptibly(),
        condition -> condition.await(),
        condition -> condition.await(250, TimeUnit.MILLISECONDS),
        condition -> condition.awaitNanos(TimeUnit.MILLISECONDS.toNanos(250)),
        condition -> condition.awaitUntil(new Date(System.currentTimeMillis() + 250))
    };
    
    @ParameterizedTest
    @ValueSource(ints = {0, 1, 2, 3, 4})
    public void testAwait(int awaitFunctionIndex) throws InterruptedException {
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
        
        assertEquals(1200, lock.getTotalWaitTime().toMillis(), 40.0); // thread 1 starts waiting at 300+500=800ms and acquires lock at 2000ms
        assertEquals(2000, lock.getTotalLockRunningTime().toMillis(), 40.0); // each thread runs for 1000, set net 2000ms
        assertEquals(500, lock.getTotalIdleTime().toMillis(), 40.0); // as lock waiting from time 0ms to 300ms, and 800ms to 1000ms
    }
    

    private static ThreadFactory myThreadFactory() {
        AtomicInteger threadNumber = new AtomicInteger();
        return runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A'));
    }
    
    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    private void logString(String message) {
        Thread currentThread = Thread.currentThread();
        System.out.println((System.currentTimeMillis() - startOfTime) + " : " + currentThread.getName() + " : " + message);
    }
}
