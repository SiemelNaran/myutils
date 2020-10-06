package org.sn.myutils.util.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;


public class PriorityThreadPoolExecutorTest {
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

    @Test
    void testNotEnoughThreads1() throws InterruptedException {
        PriorityExecutorService executor = MoreExecutors.newFixedPriorityThreadPool(3);
        DoThread doThread = new DoThread();

        sleep(30); executor.submit(4, () -> doThread.run());
        sleep(30); executor.submit(() -> doThread.run());
        sleep(30); executor.submit(6, () -> doThread.run());
        sleep(30); executor.submit(7, () -> doThread.run());
        sleep(30); executor.submit(8, () -> doThread.run());
        sleep(30); executor.submit(9, () -> doThread.run());

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());
        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // threads 4, 5, 6 start right away as the executor can run 3 threads at a time
                           "end thread with priority 4",
                           "end thread with priority 5",
                           "end thread with priority 6",
                           // threads 7, 8, 9 get added while threads 4, 5, 6 are running, and the highest priority thread in the queue runs first
                           "end thread with priority 9",
                           "end thread with priority 8",
                           "end thread with priority 7"));
    }
    
    @Test
    void testNotEnoughThreads2() throws InterruptedException {
        PriorityExecutorService executor = MoreExecutors.newFixedPriorityThreadPool(3);
        DoThread doThread = new DoThread();
        List<Future<String>> futures = new ArrayList<>();

        sleep(30); futures.add(executor.submit(4, () -> doThread.run(), "44"));
        sleep(30); futures.add(executor.submit(() -> doThread.run(), "55"));
        sleep(30); futures.add(executor.submit(6, () -> doThread.run(), "66"));
        sleep(30); futures.add(executor.submit(7, () -> doThread.run(), "77"));
        sleep(30); futures.add(executor.submit(8, () -> doThread.run(), "88"));
        sleep(30); futures.add(executor.submit(9, () -> doThread.run(), "99"));

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());
        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // threads 4, 5, 6 start right away as the executor can run 3 threads at a time
                           "end thread with priority 4",
                           "end thread with priority 5",
                           "end thread with priority 6",
                           // threads 7, 8, 9 get added while threads 4, 5, 6 are running, and the highest priority thread in the queue runs first
                           "end thread with priority 9",
                           "end thread with priority 8",
                           "end thread with priority 7"));
        
        assertThat(futures.stream().map(PriorityThreadPoolExecutorTest::getFromFuture).collect(Collectors.toList()),
                   Matchers.contains("44", "55", "66", "77", "88", "99"));
    }
    
    @Test
    void testNotEnoughThreads3() throws InterruptedException {
        PriorityExecutorService executor = MoreExecutors.newFixedPriorityThreadPool(3);
        DoThread doThread = new DoThread();
        List<Future<String>> futures = new ArrayList<>();

        sleep(30); futures.add(executor.submit(4, () -> doThread.call()));
        sleep(30); futures.add(executor.submit(() -> doThread.call()));
        sleep(30); futures.add(executor.submit(6, () -> doThread.call()));
        sleep(30); futures.add(executor.submit(7, () -> doThread.call()));
        sleep(30); futures.add(executor.submit(8, () -> doThread.call()));
        sleep(30); futures.add(executor.submit(9, () -> doThread.call()));

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
        prettyPrintList("messages", doThread.getMessages());
        assertThat(doThread.getMessages(),
                   Matchers.contains(
                           // threads 4, 5, 6 start right away as the executor can run 3 threads at a time
                           "end thread with priority 4",
                           "end thread with priority 5",
                           "end thread with priority 6",
                           // threads 7, 8, 9 get added while threads 4, 5, 6 are running, and the highest priority thread in the queue runs first
                           "end thread with priority 9",
                           "end thread with priority 8",
                           "end thread with priority 7"));
        
        assertThat(futures.stream().map(PriorityThreadPoolExecutorTest::getFromFuture).collect(Collectors.toList()),
                   Matchers.contains("44", "55", "66", "77", "88", "99"));
    }
    
    private class DoThread {
        private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

        void run() {
            Thread currentThread = Thread.currentThread();
            logString("start");
            try {
                sleep(500);
                logString("end");
                messages.add("end thread with priority " + currentThread.getPriority());
            } catch (RuntimeException | Error e) {
                logString("caught exception " + e.toString());
                messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
            }
        }
        
        String call() {
            Thread currentThread = Thread.currentThread();
            logString("start");
            try {
                sleep(250);
                logString("end");
                messages.add("end thread with priority " + currentThread.getPriority());
            } catch (RuntimeException | Error e) {
                logString("caught exception " + e.toString());
                messages.add("thread with priority " + currentThread.getPriority() + " encountered exception " + e.toString());
            }
            return Integer.toString(Thread.currentThread().getPriority()) + Integer.toString(Thread.currentThread().getPriority());
        }
        
        List<String> getMessages() {
            return messages;
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

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static <T> T getFromFuture(Future<T> future) {
        try {
            assertTrue(future.isDone());
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
