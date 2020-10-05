package org.sn.myutils.util.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.sn.myutils.LogFailureToConsoleTestWatcher;
import org.sn.myutils.TestUtil;


@ExtendWith(LogFailureToConsoleTestWatcher.class)
public class TestScheduledExecutorServiceTest {
    long startOfTime;

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

    
    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(strings = { "millis", "nanos" })
    void testScheduleRealExecutor(String waitUnit) throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = Executors.newScheduledThreadPool(1, myThreadFactory());
        service.schedule(() -> addWord(service, words, "apple"), 300, TimeUnit.MILLISECONDS);
        service.schedule(() -> addWord(service, words, "antelope"), 300, TimeUnit.MILLISECONDS);
        service.schedule(() -> { addWord(service, words, "banana"); return "callable"; }, 1, TimeUnit.SECONDS);
        service.schedule(() -> addWord(service, words, "carrot"), 2500, TimeUnit.MILLISECONDS);

        switch (waitUnit) {
            case "millis":
                MoreExecutors.advanceTime(service, 1_000, TimeUnit.MILLISECONDS);
                break;
                
            case "nanos":
                MoreExecutors.advanceTime(service, 1_002_000_000, TimeUnit.NANOSECONDS);
                // if argument to advanceTime is 1_000_100_000 then "1000:banana" does not run
                // even for 1_000_100_000 "1000:banana" does not always run
                break;
                
            default:
                throw new UnsupportedOperationException(waitUnit);
        }

        System.out.println("actual: " + words);
        var roundedWords = roundToNearestHundred(words);
        System.out.println("actual: " + roundedWords);
        assertThat(roundedWords, Matchers.contains("300:apple", "300:antelope", "1000:banana"));

        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        service.shutdown(); // second call to shutdown has no effect
        assertFalse(service.awaitTermination(0, TimeUnit.MILLISECONDS)); // because "2500:carrot" has not yet run
        assertTrue(service.isShutdown());
        assertFalse(service.isTerminated());
    }
    
    @Test
    void testEmptyExecutor() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        
        MoreExecutors.advanceTime(service, 1, TimeUnit.SECONDS);
        System.out.println("actual: " + words);
        assertThat(words, Matchers.empty());
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        service.shutdown(); // second call to shutdown has no effect
        assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }
    
    @Test
    void testSchedule() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        service.schedule(() -> addWord(service, words, "apple"), 300, TimeUnit.MILLISECONDS);
        service.schedule(() -> addWord(service, words, "antelope"), 300, TimeUnit.MILLISECONDS);
        service.schedule(() -> { addWord(service, words, "banana"); return "callable"; }, 1, TimeUnit.SECONDS);
        service.schedule(() -> addWord(service, words, "carrot"), 2500, TimeUnit.MILLISECONDS);
        ScheduledFuture<?> future2700 = service.schedule(() -> addWord(service, words, "dragon fruit"), 2700, TimeUnit.MILLISECONDS);
        
        assertThat(words, Matchers.empty());
        
        future2700.cancel(true);
        
        MoreExecutors.advanceTime(service, 1, TimeUnit.SECONDS);
        System.out.println("actual: " + words);
        assertThat(words, Matchers.contains("300:apple", "300:antelope", "1000:banana"));
        
        MoreExecutors.advanceTime(service, 500, TimeUnit.MILLISECONDS);
        System.out.println("actual: " + words);
        assertThat(words, Matchers.contains("300:apple", "300:antelope", "1000:banana"));
        
        MoreExecutors.advanceTime(service, 3, TimeUnit.MINUTES);
        System.out.println("actual: " + words);
        assertThat(words, Matchers.contains("300:apple", "300:antelope", "1000:banana", "2500:carrot"));
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        service.shutdown(); // second call to shutdown has no effect
        assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }
    
    @Test
    void testInterruptedException1() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        ScheduledFuture<?> futureToCancel = service.scheduleAtFixedRate(() -> {
            System.out.println("thread started");
            TestUtil.sleep(2000);
            addWord(service, words, "apple");
            System.out.println("thread finished");
        }, 300, 200, TimeUnit.MILLISECONDS);

        ScheduledExecutorService anotherService = Executors.newScheduledThreadPool(2);
        anotherService.schedule(() -> {
            try {
                MoreExecutors.advanceTime(service, 300, TimeUnit.MILLISECONDS);
            } catch (CompletionException e) {
                words.add(e.getClass().getSimpleName() + ":" + e.getCause().getClass().getSimpleName());
            }
        }, 0, TimeUnit.MILLISECONDS);
        anotherService.schedule(() -> {
            System.out.println("cancelling thread");
            futureToCancel.cancel(true);
            futureToCancel.cancel(true); // second call to cancel has no effect
        }, 1000, TimeUnit.MILLISECONDS);
        
        anotherService.shutdown();
        anotherService.awaitTermination(1100, TimeUnit.MILLISECONDS);
        
        System.out.println("actual: " + words);
        assertThat(words, Matchers.empty());
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MILLISECONDS);
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }
    
    @Test
    void testInterruptedException2() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        service.schedule(() -> {
            System.out.println("thread started");
            TestUtil.sleep(2000);
            addWord(service, words, "apple");
            System.out.println("thread finished");
        }, 300, TimeUnit.MILLISECONDS);

        ScheduledExecutorService anotherService = Executors.newScheduledThreadPool(2);
        ScheduledFuture<?> futureToCancel = anotherService.schedule(() -> {
            try {
                MoreExecutors.advanceTime(service, 300, TimeUnit.MILLISECONDS);
            } catch (CompletionException e) {
                // this thread starts at time 0ms and would normally finish at time 2000ms
                // but is interrupted at 1000ms
                words.add(e.getClass().getSimpleName() + ":" + e.getCause().getClass().getSimpleName());
            }
        }, 0, TimeUnit.MILLISECONDS);
        anotherService.schedule(() -> {
            System.out.println("cancelling thread that called advanceTime");
            futureToCancel.cancel(true);
        }, 1000, TimeUnit.MILLISECONDS);
        
        anotherService.shutdown();
        anotherService.awaitTermination(1100, TimeUnit.MILLISECONDS);
        
        System.out.println("actual: " + words);
        assertThat(words, Matchers.contains("CompletionException:InterruptedException"));
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MILLISECONDS);
        assertTrue(service.isShutdown());
        assertFalse(service.isTerminated(), "TestScheduledThreadPoolExecutor is not terminated is because it is still running the addWord text");
    }
    
    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(longs = { 1000, 900 })
    void testScheduleAtFixedRate(long millis) throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        service.scheduleAtFixedRate(() -> addWord(service, words, "fixedRate", 60), 300, 200, TimeUnit.MILLISECONDS);
        MoreExecutors.advanceTime(service, millis, TimeUnit.MILLISECONDS);
        System.out.println("actual: " + words);
        assertThat(words, Matchers.contains("360:fixedRate", "560:fixedRate", "760:fixedRate", "960:fixedRate"));
        // explanation: advanceTime waits for thread that starts at 900ms to finish (effectively waiting till 960ms)
        // even though we only advance time by 900ms
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }
    
    @Test
    void testScheduleWithFixedDelay() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        service.scheduleWithFixedDelay(() -> addWord(service, words, "fixedDelay", 60), 300, 200, TimeUnit.MILLISECONDS);
        MoreExecutors.advanceTime(service, 1, TimeUnit.SECONDS);
        System.out.println("actual: " + words);
        var roundedWords = roundDownToNearestTwenty(words);
        System.out.println("actual: " + roundedWords);
        assertThat(roundedWords, Matchers.contains("360:fixedDelay", "620:fixedDelay", "880:fixedDelay"));
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }
    
    @Test
    void testMixed() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        service.schedule(() -> addWord(service, words, "apple"), 300, TimeUnit.MILLISECONDS);
        service.scheduleAtFixedRate(() -> addWord(service, words, "fixedRate", 60), 300, 200, TimeUnit.MILLISECONDS);
        service.schedule(() -> { addWord(service, words, "banana"); return "callable"; }, 800, TimeUnit.MILLISECONDS);
        service.schedule(() -> addWord(service, words, "carrot"), 960, TimeUnit.MILLISECONDS);
        
        assertThat(words, Matchers.empty());
        
        MoreExecutors.advanceTime(service, 960, TimeUnit.MILLISECONDS);
        System.out.println("actual: " + words);
        assertThat(words, Matchers.contains("300:apple", "360:fixedRate", "560:fixedRate", "760:fixedRate", "800:banana", "960:fixedRate", "960:carrot"));
        // "960:fixedRate" starts at 900ms and ends at 960ms, whereas "960:carrot" starts and ends at 960ms
        // so "960:fixedRate" is guaranteed to come first
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }

    @Test
    void testScheduleAtFixedRate_DifferentTimeToExecute() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        service.scheduleAtFixedRate(() -> addWord(service, words, "fixedRateFast", 10), 300, 200, TimeUnit.MILLISECONDS);
        service.scheduleAtFixedRate(() -> addWord(service, words, "fixedRateSlow", 180), 300, 200, TimeUnit.MILLISECONDS);
        MoreExecutors.advanceTime(service, 1, TimeUnit.SECONDS);
        System.out.println("actual: " + words);
        assertThat(words, Matchers.contains(
                "310:fixedRateFast", "480:fixedRateSlow", "510:fixedRateFast", "680:fixedRateSlow",
                "710:fixedRateFast", "880:fixedRateSlow", "910:fixedRateFast", "1080:fixedRateSlow"));
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }
    
    @Test
    void testScheduleAtFixedRate_DifferentFrequency() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        service.scheduleAtFixedRate(() -> addWord(service, words, "fixedRate", 60), 300, 200, TimeUnit.MILLISECONDS);
        service.scheduleAtFixedRate(() -> addWord(service, words, "fixedRateLessFrequent", 60), 300, 650, TimeUnit.MILLISECONDS);
        MoreExecutors.advanceTime(service, 1, TimeUnit.SECONDS);
        System.out.println("actual: " + words);
        assertThat(words, Matchers.contains("360:fixedRate", "360:fixedRateLessFrequent", "560:fixedRate", "760:fixedRate", "960:fixedRate", "1010:fixedRateLessFrequent"));
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }

    @Test
    void testScheduleAtFixedRate_TaskTakesLongerThanPeriod_TwoThreads() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(2, myThreadFactory(), startOfTime);
        service.scheduleAtFixedRate(() -> addWord(service, words, "fixedRateSlow", 400), 300, 200, TimeUnit.MILLISECONDS);
        service.scheduleAtFixedRate(() -> addWord(service, words, "fixedRate", 60), 400, 100, TimeUnit.MILLISECONDS);
        MoreExecutors.advanceTime(service, 1, TimeUnit.SECONDS);
        System.out.println("actual: " + words);
        var roundedWords = roundDownToNearestTwenty(words);
        System.out.println("actual: " + roundedWords);
        assertThat(roundedWords, Matchers.contains(
                "460:fixedRate", // realTime = 60ms
                "560:fixedRate", // realTime = 120ms
                "660:fixedRate", // realTime = 180ms
                "760:fixedRate", // realTime = 240ms
                "860:fixedRate", // realTime = 300ms
                "960:fixedRate", // realTime = 360ms
                "700:fixedRateSlow", // realTime = 400ms
                "1060:fixedRate", // realTime = 420ms
                "1100:fixedRateSlow" // realTime = 800ms (task would have started at 500ms, but started at 700ms as fixedRateSlow takes longer than the period
        ));

        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());

        service.shutdown();
        assertTrue(service.awaitTermination(10, TimeUnit.MILLISECONDS));
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }

    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(strings = { "awaitTermination", "shutdown", "shutdownAll" })
    void testEndingExecutorService(String methodSequence) throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, myThreadFactory(), startOfTime);
        service.schedule(() -> addWord(service, words, "apple"), 300, TimeUnit.MILLISECONDS);
        service.schedule(() -> addWord(service, words, "antelope"), 300, TimeUnit.MILLISECONDS);
        service.scheduleAtFixedRate(() -> addWord(service, words, "fixedRate", 60), 300, 200, TimeUnit.MILLISECONDS);
        service.schedule(() -> { addWord(service, words, "banana"); return "callable"; }, 1, TimeUnit.SECONDS);
        service.schedule(() -> addWord(service, words, "carrot"), 2500, TimeUnit.MILLISECONDS);
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        assertThat(words, Matchers.empty());

        switch (methodSequence) {
            case "awaitTermination":
                assertFalse(service.awaitTermination(1110, TimeUnit.MILLISECONDS));
                assertFalse(service.isShutdown());
                assertFalse(service.isTerminated());
                System.out.println("actual: " + words);
                assertThat(words, Matchers.contains(
                        "300:apple",
                        "300:antelope",
                        "360:fixedRate",
                        "560:fixedRate",
                        "760:fixedRate",
                        "960:fixedRate",
                        "1000:banana"));
                // "1160:fixedRate" starts at 1100 but does not finish by 1110 so is not printed
                // also even if you add a Thread.sleep(1000) it does not finish because the thread is interrupted at 1110
                break;
                
            case "shutdown":
                service.shutdown(); // cancels future periodic tasks (default behavior of java.util.concurrent.ScheduledThreadPoolExecutor)
                assertFalse(service.awaitTermination(1050, TimeUnit.MILLISECONDS));
                assertTrue(service.isShutdown());
                assertFalse(service.isTerminated());
                System.out.println("actual: " + words);
                assertThat(words, Matchers.contains("300:apple", "300:antelope",  "1000:banana"));
                // if we await for 1000ms then "1000:banana" runs with invokeAll("1000:banana", time=0ms)
                // and because we wait for 0ms the task does not run
                break;
                
            case "shutdownAll":
                List<Runnable> runnables = service.shutdownNow();
                assertThat(runnables, Matchers.hasSize(5));        
                assertThat(words, Matchers.empty());
                assertTrue(service.isShutdown());
                assertTrue(service.isTerminated());
                
                assertTrue(service.awaitTermination(300, TimeUnit.MILLISECONDS));
                assertThat(words, Matchers.empty()); // proves that none of the terminated runnables ran
                
                runnables.forEach(Runnable::run);
                System.out.println("actual: " + words);
                assertThat(words, Matchers.contains("300:apple", "300:antelope",  "360:fixedRate", "300:banana", "300:carrot"));
                
                assertTrue(service.isTerminated()); // proves that periodic runnable did not get added to executor service after completion
                
                break;
                
            default:
                throw new UnsupportedOperationException(methodSequence);
        }
    }
    
    @Test
    void testImmediateFunctions() throws InterruptedException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(1, startOfTime);
        service.submit(() -> addWord(service, words, "apple"));
        service.submit(() -> addWord(service, words, "banana"), "runnable");
        service.submit(() -> { addWord(service, words, "carrot"); return "callable"; });
        service.execute(() -> addWord(service, words, "dragon fruit"));
        Thread.sleep(100); // wait for tasks to finish
        System.out.println("actual: " + words);
        
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MILLISECONDS);
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
        
        assertThat(words, Matchers.contains("0:apple", "0:banana", "0:carrot", "0:dragon fruit"));
    }
    
    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(strings = { "invokeAll", "invokeAllWithTimeout", "invokeAny", "invokeAnyWithTimeout" })
    void testBulkFunctions(String method) throws InterruptedException, ExecutionException, TimeoutException {
        List<String> words = Collections.synchronizedList(new ArrayList<>());
        ScheduledExecutorService service = MoreExecutors.newTestScheduledThreadPool(3, startOfTime);
        List<Callable<Integer>> callables = Arrays.asList(
            () -> { addWord(service, words, "apple", 300); return 1; },
            () -> { addWord(service, words, "banana", 0); return 2; },
            () -> { addWord(service, words, "carrot", 500); return 3; });
        
        switch (method) {
            case "invokeAll":
            case "invokeAllWithTimeout":
                List<Future<Integer>> futures;
                switch (method) {
                    case "invokeAll":
                        futures = service.invokeAll(callables);
                        break;
                    case "invokeAllWithTimeout":
                        futures = service.invokeAll(callables, 1, TimeUnit.SECONDS);
                        break;
                    default:
                        throw new UnsupportedOperationException(method);
                }
                System.out.println("actual: " + words);
                assertThat(words, Matchers.contains("0:banana", "300:apple", "500:carrot"));
                assertThat(TestUtil.toList(futures), Matchers.contains(1, 2, 3));
                break;
                
            case "invokeAny":
            case "invokeAnyWithTimeout":
                int futureValue;
                switch (method) {
                    case "invokeAny":
                        futureValue = service.invokeAny(callables);
                        break;
                    case "invokeAnyWithTimeout":
                        futureValue = service.invokeAny(callables, 1, TimeUnit.SECONDS);
                        break;
                    default:
                        throw new UnsupportedOperationException(method);
                }
                System.out.println("actual: " + words);
                assertThat(words, Matchers.contains("0:banana"));
                assertEquals(2, futureValue);
                break;
                
            default:
                throw new UnsupportedOperationException(method);

        }
    
        assertFalse(service.isShutdown());
        assertFalse(service.isTerminated());
        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.MILLISECONDS);
        assertTrue(service.isShutdown());
        assertTrue(service.isTerminated());
    }
    

    private static ThreadFactory myThreadFactory() {
        AtomicInteger threadNumber = new AtomicInteger();
        return runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A'));
    }

    private void addWord(ScheduledExecutorService baseService, List<String> list, String word) {
        addWord(baseService, list, word, 0);
    }
    
    private void addWord(ScheduledExecutorService service, List<String> list, String word, long realSleepMillis) {
        long startTimeMillis = System.currentTimeMillis();
        long timeBeforeSleep = MoreExecutors.currentTimeMillis(service) - startOfTime;
        try {
            System.out.println(timeBeforeSleep + ": started " + word + " (realSleepMillis=" + realSleepMillis + ')');
            TestUtil.sleep(realSleepMillis);
            long timeAfterSleep = timeBeforeSleep + realSleepMillis;
            list.add(timeAfterSleep + ":" + word);
            System.out.println(timeAfterSleep + ": added " + word);
        } catch (RuntimeException e) {
            long deltaMillis = System.currentTimeMillis() - startTimeMillis;
            long timeException = deltaMillis - startTimeMillis;
            System.out.println(timeException + ": caught " + e.getClass().getSimpleName());
            throw e;
        }
    }

    /**
     * Return an array like [323:apple, 324:antelope, 1021:banana] to [300:apple, 300:antelope, 1000:banana].
     */
    private List<String> roundToNearestHundred(List<String> words) {
        List<String> results = new ArrayList<>(words.size());
        for (String word : words) {
            String[] parts = word.split(":");
            long time = Long.parseLong(parts[0]);
            long rounded = ((time + 50) / 100) * 100;
            String result = rounded + ":" + parts[1];
            results.add(result);
        }
        return results;
    }

    /**
     * Return an array like [360:fixedDelay, 624:fixedDelay, 889:fixedDelay] to [360:fixedDelay, 620:fixedDelay, 880:fixedDelay].
     */
    private List<String> roundDownToNearestTwenty(List<String> words) {
        List<String> results = new ArrayList<>(words.size());
        for (String word : words) {
            String[] parts = word.split(":");
            long time = Long.parseLong(parts[0]);
            long rounded = (time / 20) * 20;
            String result = rounded + ":" + parts[1];
            results.add(result);
        }
        return results;
    }
}
