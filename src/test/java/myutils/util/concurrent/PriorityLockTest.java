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
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class PriorityLockTest {
    long startOfTime;
    
    @BeforeEach
    void setStartOfTime() {
        startOfTime = System.currentTimeMillis();
    }
    
    @AfterEach
    void printTestFinished() {
        System.out.println("test finished");
    }
    
    @AfterAll
    static void printAllTestsFinished() {
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
    void testEnoughThreads() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThread(priorityLock, false);

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


    @Test
    @SuppressWarnings("checkstyle:LineLength")
    void testReuseThreads() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThread(priorityLock, false);

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
                           "end thread with priority 7", // queue is [priority 5, priority 7] so thread with highest priority in queue goes next, and when priority 7 done we signal priority 5 to wake up
                           "end thread with priority 5"));
    }


    @Test
    void testExceptionWhileAcquireLockInterruptibly() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThread(priorityLock, true);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future7 =
        executor.schedule(() -> { doThread.action(7); }, 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> {
            logString(Thread.currentThread(), "interrupt thread of priority 7");
            future7.cancel(true);
        }, 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        assertThat(doThread.getMessages(),
                   Matchers.contains("end thread with priority 4", // thread with priority 4 runs first, and while it is running all others get added to the queue
                                     // thread 7 is interrupted while waiting to acquire lock
                                     "end thread with priority 6",
                                     "end thread with priority 6",
                                     "end thread with priority 5",
                                     "end thread with priority 5"));
    }


    @Test
    @SuppressWarnings("checkstyle:ParenPad")
    void testChangingThreadPriority() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThread(priorityLock, true);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4   ); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5   ); }, 200, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6   ); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5   ); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6   ); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7, 8); }, 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        assertThat(doThread.getMessages(),
                   Matchers.contains("end thread with priority 4", // thread with priority 4 runs first, and while it is running all others get added to the queue
                                     "end thread with priority 8",
                                     "end thread with priority 6",
                                     "end thread with priority 6",
                                     "end thread with priority 5",
                                     "end thread with priority 5"));
    }


    private class DoThread {
        private final PriorityLock priorityLock;
        private final boolean allowInterrupt;
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

        private DoThread(PriorityLock priorityLock, boolean allowInterrupt) {
            this.priorityLock = priorityLock;
            this.allowInterrupt = allowInterrupt;
        }

        void action(int priority) {
            action(priority, null);
        }
        
        void action(int priority, Integer newPriority) {
            final Thread currentThread = Thread.currentThread();
            currentThread.setPriority(priority);
            logString(currentThread, "start");
            if (allowInterrupt) {
                try {
                    priorityLock.lockInterruptibly();
                } catch (InterruptedException e) {
                    logString(currentThread, "caught InterruptedException");
                    throw new RuntimeException(e);
                }
            } else {
                priorityLock.lock();
            }
            try {
                logString(currentThread, "acquired lock");
                sleep(1000);
                if (newPriority != null) {
                    Thread.currentThread().setPriority(newPriority);
                }
                logString(currentThread, "end");
                messages.add("end thread with priority " + currentThread.getPriority());
            } finally {
                priorityLock.unlock();
            }
        }
        
        List<String> getMessages() {
            return messages;
        }
    }

    private void logString(Thread currentThread, String message) {
        System.out.println(
                Long.toString(System.currentTimeMillis() - startOfTime)
                + " : " + currentThread.getName() + "@" + currentThread.hashCode() + " at priority " + currentThread.getPriority()
                + " : " + message);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
