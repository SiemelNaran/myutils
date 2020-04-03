package myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;


public class TimedReentrantLockTest {
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
        System.out.println("all tests finished");
    }
    
    @Test
    public void testLock() throws InterruptedException {
        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService service
            = Executors.newScheduledThreadPool(2, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));
        
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
                    lock.unlock();
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
        
        assertEquals(800, lock.getTotalWaitTime().toMillis(), 40.0); // thread 1 ends at 1300, thread 2 waiting from 500, so waitTime=1300-500=800
        assertEquals(2000, lock.getTotalLockRunningTime().toMillis(), 40.0); // each thread runs for 1000, set net 2000ms
        assertEquals(300, lock.getTotalIdleTime().toMillis(), 40.0); // as lock waiting from time 0ms to 300ms when thread 1 starts
    }
    

    @Test
    public void testLockInterruptibly() throws InterruptedException {
        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService service
            = Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));
        
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
                    lock.unlock();
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
        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService service
            = Executors.newScheduledThreadPool(2, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));
        
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
        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService service
            = Executors.newScheduledThreadPool(3, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));
        
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
