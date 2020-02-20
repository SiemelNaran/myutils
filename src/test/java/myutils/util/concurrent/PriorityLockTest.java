package myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class PriorityLockTest {
    long startOfTime;
    
    @BeforeEach
    void setStartOfTime() {
        startOfTime = System.currentTimeMillis();
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
        executor.schedule(() -> { doThread.action(6); }, 400, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 300, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(6); }, 500, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(7); }, 600, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        assertEquals(Arrays.asList("end thread with priority 4", // thread with priority 4 runs first, and while it is running all others get added to the queue
                                   "end thread with priority 7", // thread with highest priority goes first
                                   "end thread with priority 6", // thread with next highest priority goes next
                                   "end thread with priority 6", // thread with next highest priority goes next
                                   "end thread with priority 5", // thread with next highest priority goes next
                                   "end thread with priority 5"), // thread with lowest priority goes last
                     doThread.getMessages());
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

        assertEquals(Arrays.asList("end thread with priority 4", // thread with priority 4 runs first, and while it is running 2 runnables get added to the queue (priority 5 at time 200ms, priority 6 at time 300s)
                                   "end thread with priority 6", // queue is [priority 5, priority 6] so thread with highest priority in queue goes next, and we add priority 5 at 400ms to the queue, and when priority 6 done we signal priority 5 to wake up
                                   "end thread with priority 5", // queue is [priority 5, priority 5] so thread with highest priority in queue goes next, and we add priority 6 at 500ms to the queue, and when priority 5 done we signal threads waiting on priority 5 to wake up though there are no threads with this priority
                                   "end thread with priority 6", // queue is [priority 5, priority 6] so thread with highest priority in queue goes next, and we add priority 7 at 600ms to the queue, and when priority 6 done we signal priority 5 to wake up
                                   "end thread with priority 7", // queue is [priority 5, priority 7] so thread with highest priority in queue goes next, and when priority 7 done we signal priority 5 to wake up
                                   "end thread with priority 5"),
                     doThread.getMessages());
    }


    @Test
    @SuppressWarnings("checkstyle:LineLength")
    void testExceptionWhileAcquireLock() throws InterruptedException {
        PriorityLock priorityLock = new PriorityLock();

        DoThread doThread = new DoThread(priorityLock, true);

        AtomicInteger threadNumber = new AtomicInteger();
        ScheduledExecutorService executor =
                Executors.newScheduledThreadPool(6, runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A')));

        executor.schedule(() -> { doThread.action(4); }, 100, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { doThread.action(5); }, 200, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future600 =
        executor.schedule(() -> { doThread.action(7); }, 600, TimeUnit.MILLISECONDS);
        executor.schedule(() -> { System.out.println((System.currentTimeMillis() - startOfTime) + " : interrupt thread priority 7"); future600.cancel(true); }, 700, TimeUnit.MILLISECONDS);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);

        assertEquals(Arrays.asList("end thread with priority 4", // thread with priority 4 runs first, and while it is running all others get added to the queue
                                   "end thread with priority 5"), // thread with lowest priority goes last
                     doThread.getMessages());
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
                logString(currentThread, "end");
                messages.add("end thread with priority " + currentThread.getPriority());
            } finally {
                priorityLock.unlock();
            }
        }
        
        private void logString(Thread currentThread, String message) {
            System.out.println(
                    Long.toString(System.currentTimeMillis() - startOfTime)
                    + " : " + currentThread.getName() + "@" + currentThread.hashCode() + " at priority " + currentThread.getPriority()
                    + " : " + message);
        }

        List<String> getMessages() {
            return messages;
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
