package org.sn.myutils.util.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.sn.myutils.testutils.TestUtil.assertException;
import static org.sn.myutils.testutils.TestUtil.assertExceptionFromCallable;
import static org.sn.myutils.testutils.TestUtil.myThreadFactory;
import static org.sn.myutils.testutils.TestUtil.sleep;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableScheduledFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.sn.myutils.testutils.TestBase;
import org.sn.myutils.testutils.TestUtil;


/**
 * Test time bucket scheduled thread pool executor.
 *
 * <p>In Eclipse run with the following VM arguments to get full logs:
 * <code>
 -ea
 -Djava.util.logging.config.file=../org.sn.myutils.testutils/target/classes/logging.properties
 * </code>
 */
class TimeBucketScheduledThreadPoolExecutorTest extends TestBase {
    private static final List<String> words = Collections.synchronizedList(new ArrayList<>());

    private Path folder;

    private static class MyRunnable implements SerializableRunnable {
        private final String value;
        private final transient int transientInteger;

        MyRunnable(String value) {
            this.value = value;
            this.transientInteger = 1;
        }

        @Override
        public void run() {
            if (Thread.currentThread().isInterrupted()) {
                System.out.println("Interrupted " + toString());
                return;
            }
            System.out.println("Running " + toString());
            words.add(value);
        }

        @Override
        public String toString() {
            return "<adding " + value + " transientInteger=" + transientInteger + ">";
        }
    }

    private static class MyCallable implements SerializableCallable<String> {
        private final String value;
        private final transient int transientInteger;

        MyCallable(String value) {
            this.value = value;
            this.transientInteger = 1;
        }

        @Override
        public String call() {
            if (Thread.currentThread().isInterrupted()) {
                System.out.println("Interrupted " + toString());
                return null;
            }
            System.out.println("Running " + toString());
            words.add(value);
            return value;
        }

        @Override
        public String toString() {
            return "<adding and returning " + value + "transientInteger=" + transientInteger + ">";
        }
    }

    @BeforeEach
    void setup() throws IOException {
        folder = Files.createTempDirectory("TimeBucketScheduledThreadPoolExecutorTest");
        System.out.println("folder=\n" + folder);

        words.clear();
    }

    @Test
    void basicTestRunnable() throws IOException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            assertEquals(0, countFiles());

            sleepTillNextSecond();
            Instant realStart = Instant.now();
            long[] timeBuckets;

            service.schedule(new MyRunnable("3800"), 3800, TimeUnit.MILLISECONDS); // bucket [3000, 4000)
            assertEquals(1, getNumTimeBuckets(service));
            service.schedule(new MyRunnable("3900"), 3900, TimeUnit.MILLISECONDS); // bucket [3000, 4000)
            assertEquals(1, getNumTimeBuckets(service));
            service.schedule(new MyRunnable("800"), 800, TimeUnit.MILLISECONDS); // bucket [0000, 1000)
            assertEquals(2, getNumTimeBuckets(service));
            timeBuckets = getTimeBuckets(service);
            assertEquals(timeBuckets[0] + 3000, timeBuckets[1]);
            service.schedule(new MyRunnable("1700"), 1700, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
            assertEquals(3, getNumTimeBuckets(service));
            service.schedule(new MyRunnable("1500"), 1500, TimeUnit.MILLISECONDS); // bucket [1000, 1000)
            assertEquals(3, getNumTimeBuckets(service));
            service.schedule(() -> words.add("2100"), 2100, TimeUnit.MILLISECONDS); // no bucket as not serializable
            System.out.println("Time to schedule 6 futures: " + Duration.between(realStart, Instant.now()).toMillis() + "ms"); // typical output: Time to schedule 6 futures: 33ms
            assertEquals(3, getNumTimeBuckets(service));
            assertEquals(3, countFiles());
            timeBuckets = getTimeBuckets(service);
            System.out.println("timeBuckets=" + Arrays.stream(timeBuckets).mapToObj(Long::toString).collect(Collectors.joining(",")));
            assertEquals(timeBuckets[0] + 1000, timeBuckets[1]);
            assertEquals(timeBuckets[0] + 3000, timeBuckets[2]);
            assertThat(words, Matchers.empty());

            sleep(1200); // advance time to 1200ms
            System.out.println("words=" + words);
            assertThat(words, Matchers.contains("800"));
            assertEquals(2, getNumTimeBuckets(service));
            assertEquals(2, countFiles());

            sleep(1200); // advance time to 2400ms
            System.out.println("words=" + words);
            assertThat(words, Matchers.contains("800", "1500", "1700", "2100"));
            assertEquals(1, countFiles());

            sleep(1200); // advance time to 3600ms
            System.out.println("words=" + words);
            assertThat(words, Matchers.contains("800", "1500", "1700", "2100"));
            assertEquals(1, getNumTimeBuckets(service));
            assertEquals(1, countFiles());

            sleep(1200); // advance time to 4800ms
            System.out.println("words=" + words);
            assertThat(words, Matchers.contains("800", "1500", "1700", "2100", "3800", "3900"));
            assertEquals(0, getNumTimeBuckets(service));
            assertEquals(0, countFiles());
        }
    }

    @Test
    void basicTestCallable() throws IOException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            assertEquals(0, countFiles());

            sleepTillNextSecond();
            Instant realStart = Instant.now();
            long[] timeBuckets;

            service.schedule(new MyCallable("800"), 800, TimeUnit.MILLISECONDS); // bucket [0000, 1000)
            assertEquals(1, getNumTimeBuckets(service));
            service.schedule(new MyCallable("1700"), 1700, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
            assertEquals(2, getNumTimeBuckets(service));
            service.schedule(new MyCallable("1500"), 1500, TimeUnit.MILLISECONDS); // bucket [1000, 1000)
            assertEquals(2, getNumTimeBuckets(service));
            service.schedule(
                    () -> {
                        words.add("2100");
                        return "2100";
                    },
                    2100,
                    TimeUnit.MILLISECONDS); // no bucket as not serializable
            System.out.println("Time to schedule 3 futures: " + Duration.between(realStart, Instant.now()).toMillis() + "ms"); // typical output: Time to schedule 6 futures: 33ms
            assertEquals(2, getNumTimeBuckets(service));
            assertEquals(2, countFiles());
            timeBuckets = getTimeBuckets(service);
            System.out.println("timeBuckets=" + Arrays.stream(timeBuckets).mapToObj(Long::toString).collect(Collectors.joining(",")));
            assertEquals(timeBuckets[0] + 1000, timeBuckets[1]);
            assertThat(words, Matchers.empty());

            sleep(1200); // advance time to 1200ms
            System.out.println("words=" + words);
            assertThat(words, Matchers.contains("800"));
            assertEquals(1, getNumTimeBuckets(service));
            assertEquals(1, countFiles());

            sleep(1200); // advance time to 2400ms
            System.out.println("words=" + words);
            assertThat(words, Matchers.contains("800", "1500", "1700", "2100"));
            assertEquals(0, countFiles());
        }
    }

    @Test
    void testNanos() throws IOException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            assertEquals(0, countFiles());

            sleepTillNextSecond();
            long[] timeBuckets;

            service.schedule(new MyCallable("800"), 800, TimeUnit.MILLISECONDS); // bucket [0000, 1000)
            assertEquals(1, getNumTimeBuckets(service));
            service.schedule(new MyCallable("1500+"), 1508 * 1_000_000 + 111_222, TimeUnit.NANOSECONDS); // bucket [1000, 2000)
            assertEquals(2, getNumTimeBuckets(service));
            service.schedule(new MyCallable("1500"), 1500, TimeUnit.MILLISECONDS); // bucket [1000, 1000)
            assertEquals(2, getNumTimeBuckets(service));
            assertEquals(2, countFiles());
            timeBuckets = getTimeBuckets(service);
            System.out.println("timeBuckets=" + Arrays.stream(timeBuckets).mapToObj(Long::toString).collect(Collectors.joining(",")));
            assertEquals(timeBuckets[0] + 1000, timeBuckets[1]);
            assertThat(words, Matchers.empty());

            sleep(1200); // advance time to 1200ms
            System.out.println("words=" + words);
            assertThat(words, Matchers.contains("800"));
            assertEquals(1, getNumTimeBuckets(service));
            assertEquals(1, countFiles());

            sleep(1200); // advance time to 2400ms
            System.out.println("words=" + words);
            assertThat(words, Matchers.contains("800", "1500", "1500+"));
            assertEquals(0, countFiles());
        }
    }

    @Test
    void multipleThreadsWriteToTimeBucketAtSameTime() throws IOException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            sleepTillNextSecond();

            ScheduledExecutorService runner = Executors.newScheduledThreadPool(4, myThreadFactory());
            runner.schedule(() -> service.schedule(new MyRunnable("1400"), 1400, TimeUnit.MILLISECONDS), 100, TimeUnit.MILLISECONDS);
            runner.schedule(() -> service.schedule(new MyRunnable("1300"), 1300, TimeUnit.MILLISECONDS), 100, TimeUnit.MILLISECONDS);
            runner.schedule(() -> service.schedule(new MyRunnable("1200"), 1200, TimeUnit.MILLISECONDS), 100, TimeUnit.MILLISECONDS);
            runner.schedule(() -> service.schedule(new MyRunnable("1100"), 1100, TimeUnit.MILLISECONDS), 100, TimeUnit.MILLISECONDS);
            runner.shutdown();
            assertTrue(runner.awaitTermination(250, TimeUnit.MILLISECONDS));

            assertEquals(1, getNumTimeBuckets(service));
            assertThat(words, Matchers.empty());

            service.shutdown();
            assertTrue(service.awaitTermination(2, TimeUnit.SECONDS));
            assertEquals(0, getNumTimeBuckets(service));
            assertThat(words, Matchers.contains("1100", "1200", "1300", "1400"));
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void cancelAndGet() throws IOException, ExecutionException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            assertEquals(0, countFiles());

            sleepTillNextSecond();

            ScheduledFuture<String> future500 = service.schedule(() -> "500", 500, TimeUnit.MILLISECONDS); // no bucket as lambda is not serializable
            ScheduledFuture<String> future600 = service.schedule(new MyCallable("600"), 600, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
            ScheduledFuture<String> future1400 = service.schedule(new MyCallable("1400"), 1400, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
            service.schedule(new MyRunnable("1300"), 1300, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
            ScheduledFuture<?> future1200 = service.schedule(new MyRunnable("1200"), 1200, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
            assertEquals(2, getNumTimeBuckets(service));
            assertEquals(2, countFiles());
            assertThat(words, Matchers.empty());

            assertTrue(future500.cancel(true));
            assertTrue(future600.cancel(true));
            assertTrue(future1200.cancel(true));
            assertTrue(future1200.cancel(true)); // cancel twice has no effect
            assertEquals("1400", future1400.get());
            assertThat(words, Matchers.contains("1300", "1400"));

            assertTrue(future1200.isCancelled());
            assertTrue(future1200.isDone());
            assertFalse(future1400.isCancelled());
            assertTrue(future1400.isDone());

            assertThat(future1200.compareTo(future1400), Matchers.lessThan(0));
            assertThat(future1200.compareTo(future600), Matchers.greaterThan(0));

            assertFalse(((RunnableScheduledFuture<Integer>) future1200).isPeriodic()); // @SuppressWarnings("unchecked")
        }
    }

    @Test
    void getTimeout1() throws IOException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            assertEquals(0, countFiles());

            sleepTillNextSecond();

            ScheduledFuture<?> future1300 = service.schedule(new MyRunnable("1300"), 1300, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
            assertEquals(1, getNumTimeBuckets(service));
            assertThat(words, Matchers.empty());

            Instant start = Instant.now();
            assertExceptionFromCallable(() -> future1300.get(400, TimeUnit.MILLISECONDS), TimeoutException.class);
            assertThat(Duration.between(start, Instant.now()).toMillis(), TestUtil.between(380L, 420L));
        }
    }

    @Test
    void getTimeout2() throws IOException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            assertEquals(0, countFiles());

            sleepTillNextSecond();

            ScheduledFuture<?> future1300 = service.schedule(new MyRunnable("1300"), 1300, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
            assertEquals(1, getNumTimeBuckets(service));
            assertThat(words, Matchers.empty());

            Instant start = Instant.now();
            assertExceptionFromCallable(() -> future1300.get(1100, TimeUnit.MILLISECONDS), TimeoutException.class);
            assertThat(Duration.between(start, Instant.now()).toMillis(), TestUtil.between(1080L, 1120L));
        }
    }

    @Test
    void testForwardingFunctions() throws IOException, ExecutionException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            service.execute(new MyRunnable("execute"));
            var future1 = service.submit(new MyRunnable("apple"));
            var future2 = service.submit(new MyRunnable("banana"), "banana");
            var future3 = service.submit(new MyCallable("carrot"));
            assertNull(future1.get());
            assertThat(future2.get(), Matchers.equalTo("banana"));
            assertThat(future3.get(), Matchers.equalTo("carrot"));
            assertThat(words, Matchers.contains("execute", "apple", "banana", "carrot"));

            words.clear();
            service.scheduleAtFixedRate(new MyRunnable("hello"), 0, 100, TimeUnit.MILLISECONDS);
            service.scheduleWithFixedDelay(new MyRunnable("world"), 40, 100, TimeUnit.MILLISECONDS);
            sleep(280);
            assertThat(words, Matchers.contains("hello", "world", "hello", "world", "hello", "world"));
        }
    }

    @Test
    void testRejectWhenShutdown() throws IOException, InterruptedException, ExecutionException, TimeoutException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            assertEquals(0, countFiles());

            service.shutdown();
            assertException(() -> service.execute(new MyRunnable("apple")), RejectedExecutionException.class);
            assertExceptionFromCallable(() -> service.submit(new MyRunnable("apple")), RejectedExecutionException.class);
            assertExceptionFromCallable(() -> service.submit(new MyRunnable("banana"), "banana"), RejectedExecutionException.class);
            assertExceptionFromCallable(() -> service.submit(new MyCallable("carrot")), RejectedExecutionException.class);
            assertExceptionFromCallable(() -> service.schedule(new MyRunnable("600"), 600, TimeUnit.MILLISECONDS), RejectedExecutionException.class);
            assertExceptionFromCallable(() -> service.schedule(new MyCallable("600"), 600, TimeUnit.MILLISECONDS), RejectedExecutionException.class);
            assertExceptionFromCallable(() -> service.scheduleAtFixedRate(new MyRunnable("hello"), 0, 100, TimeUnit.MILLISECONDS), RejectedExecutionException.class);
            assertExceptionFromCallable(() -> service.scheduleWithFixedDelay(new MyRunnable("world"), 40, 100, TimeUnit.MILLISECONDS), RejectedExecutionException.class);

            // invokeAll and invokeAny continue to work even when executor is shut down:
            List<Future<String>> result = service.invokeAll(List.of(new MyCallable("hello"), new MyCallable("world")));
            assertThat(result.stream().map(Future::isDone).collect(Collectors.toList()), Matchers.contains(true, true));
            assertThat(result.stream().map(TestUtil::join).collect(Collectors.toList()), Matchers.contains("hello", "world"));
            result = service.invokeAll(List.of(new MyCallable("hello"), new MyCallable("world")), 100, TimeUnit.MILLISECONDS);
            assertThat(result.stream().map(Future::isDone).collect(Collectors.toList()), Matchers.contains(true, true));
            assertThat(result.stream().map(TestUtil::join).collect(Collectors.toList()), Matchers.contains("hello", "world"));
            String anyString = service.invokeAny(List.of(new MyCallable("hello"), new MyCallable("world")));
            assertThat(anyString, Matchers.oneOf("hello", "world"));
            anyString = service.invokeAny(List.of(new MyCallable("hello"), new MyCallable("world")), 100, TimeUnit.MILLISECONDS);
            assertThat(anyString, Matchers.oneOf("hello", "world"));
        }
    }

    @Test
    void changeBucketLength() throws IOException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            TimeBucketScheduledThreadPoolExecutor serviceImpl = (TimeBucketScheduledThreadPoolExecutor) service;
            assertEquals(0, countFiles());

            sleepTillNextSecond();

            service.schedule(new MyRunnable("300"), 300, TimeUnit.MILLISECONDS); // bucket [0000, 1000)
            service.schedule(new MyRunnable("900"), 900, TimeUnit.MILLISECONDS); // bucket [0000, 1000)
            assertEquals(1, countFiles());
            serviceImpl.setTimeBucketLength(Duration.ofMillis(500));
            service.schedule(new MyRunnable("1300"), 1300, TimeUnit.MILLISECONDS); // bucket [1000, 1500)
            service.schedule(new MyRunnable("1900"), 1900, TimeUnit.MILLISECONDS); // bucket [1500, 2000)
            assertEquals(3, getNumTimeBuckets(service));
            assertEquals(3, countFiles());
            assertThat(words, Matchers.empty());
        }
    }


    @Test
    void tooManyTimeBuckets() throws IOException {
        Duration timeBucketLength = Duration.ofMillis(250);
        try (AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory())) {
            sleepTillNextSecond();

            int numBuckets = 0;
            for (int time = 250; time <= 4500; time += 250) {
                numBuckets++;
                service.schedule(new MyRunnable(Integer.toString(time)), time, TimeUnit.MILLISECONDS);
            }
            System.out.println("Number of time buckets: " + numBuckets);
            assertEquals(18, numBuckets);
            assertEquals(numBuckets, getNumTimeBuckets(service));
            assertEquals(16, getNumOpenFiles(service));
            assertEquals(numBuckets, countFiles());

            service.schedule(new MyRunnable(Integer.toString(260)), 260, TimeUnit.MILLISECONDS);
            assertEquals(numBuckets, getNumTimeBuckets(service));
            assertEquals(16, getNumOpenFiles(service));

            sleep(4600);
            System.out.println("words=" + words);
            assertThat(words, Matchers.contains("250", "260", "500", "750", "1000",
                                                "1250", "1500", "1750", "2000",
                                                "2250", "2500", "2750", "3000",
                                                "3250", "3500", "3750", "4000",
                                                "4250", "4500"));
        }
    }

    @Test
    void testAwaitTermination1() throws IOException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory());
        sleepTillNextSecond();

        service.schedule(new MyRunnable("800"), 800, TimeUnit.MILLISECONDS); // bucket [0, 1000)
        service.schedule(new MyRunnable("1100"), 1100, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        service.schedule(new MyRunnable("1200"), 1200, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        assertEquals(2, getNumTimeBuckets(service));
        assertThat(words, Matchers.empty());

        service.shutdown();
        assertFalse(service.awaitTermination(500, TimeUnit.MILLISECONDS)); // as there are tasks to run in time bucket [1000, 2000)
        assertThat(words, Matchers.empty());
        assertEquals(0, getNumTimeBuckets(service)); // awaitTermination removes all time buckets from memory
        assertEquals(2, countFiles());

        // even though executor is shutdown future tasks still run
        // this is the behavior of java.util.concurrent.ScheduledThreadPoolExecutor
        // but future tasks in the next time bucket do not run
        sleep(1700);
        assertThat(words, Matchers.contains("800"));
        assertEquals(2, countFiles()); // we're now at time 500+1700=2300, but time bucket file is neither loaded nor deleted
        assertFalse(service.isTerminated());
    }

    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(booleans = {false, true})
    void testAwaitTermination2(boolean addExtra) throws IOException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        AutoCloseableScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory());
        sleepTillNextSecond();

        service.schedule(new MyRunnable("1100"), 1100, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        service.schedule(new MyRunnable("1200"), 1200, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        if (addExtra) {
            service.schedule(new MyRunnable("1800"), 1800, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        }
        assertEquals(1, countFiles());
        assertThat(words, Matchers.empty());

        service.shutdown();
        boolean terminated = service.awaitTermination(1400, TimeUnit.MILLISECONDS);
        assertEquals(!addExtra, terminated);
        assertEquals(!addExtra, service.isTerminated());
        System.out.println("words=" + words);
        assertThat(words, Matchers.contains("1100", "1200"));
        assertEquals(1, countFiles());

        var remainingTasks = service.shutdownNow();
        assertEquals(addExtra ? 1 : 0, remainingTasks.size());
    }

    @Test
    void testShutdownNow() throws IOException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        ScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory());

        sleepTillNextSecond();

        service.schedule(new MyRunnable("1100"), 1100, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        service.schedule(new MyRunnable("1200"), 1200, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        assertEquals(1, countFiles());
        assertThat(words, Matchers.empty());

        var tasksNotStarted = service.shutdownNow();
        assertThat(tasksNotStarted, Matchers.empty());
        boolean terminated = service.awaitTermination(1400, TimeUnit.MILLISECONDS);
        assertTrue(terminated);
        assertThat(words, Matchers.empty());
        assertEquals(1, countFiles());
    }

    @Test
    void testRestartExecutor() throws IOException, InterruptedException {
        AutoCloseableScheduledExecutorService service = null;

        try {
            Duration timeBucketLength = Duration.ofSeconds(1);
            service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory());

            sleepTillNextSecond();

            service.schedule(new MyRunnable("200"), 400, TimeUnit.MILLISECONDS);
            service.schedule(new MyRunnable("800"), 800, TimeUnit.MILLISECONDS);
            service.schedule(new MyRunnable("1000"), 1, TimeUnit.SECONDS);
            service.schedule(new MyRunnable("1800"), 1600, TimeUnit.MILLISECONDS);
            service.schedule(new MyRunnable("2200"), 2200, TimeUnit.MILLISECONDS);
            service.schedule(new MyRunnable("2800"), 2800, TimeUnit.MILLISECONDS);
            service.schedule(new MyRunnable("3600"), 3600, TimeUnit.MILLISECONDS);
            service.schedule(new MyRunnable("3800"), 3800, TimeUnit.MILLISECONDS);
            service.schedule(new MyRunnable("4200"), 4200, TimeUnit.MILLISECONDS);
            service.schedule(new MyRunnable("4800"), 4800, TimeUnit.MILLISECONDS);
            assertEquals(5, countFiles());

            sleep(1490); // at 1490ms mark
            System.out.println("words1=" + words);
            assertThat(words, Matchers.contains("200", "800", "1000"));

            service.shutdownNow();
            service.awaitTermination(10, TimeUnit.MILLISECONDS); // at 1500ms mark
            service = null;

            assertEquals(4, countFiles()); // file for [0, 1000) was removed
        } finally {
            if (service != null) {
                service.close();
            }
        }

        sleep(1500); // at 3000ms mark.  Normally 1800, 2200, 2800 would run, but the executor is shut down
        System.out.println("Restarting executor");

        try {
            Duration timeBucketLength = Duration.ofSeconds(2);
            service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory());

            sleep(100); // give executor time to load jobs till the 3000ms mark and run them, now at 3100ms mark
            System.out.println("words2=" + words);
            assertThat(words, Matchers.contains("200", "800", "1000", "1800", "2200", "2800"));

            Instant startInstant = Instant.now();
            service.shutdown();
            boolean terminated = service.awaitTermination(2, TimeUnit.SECONDS);
            System.out.println("words3=" + words);
            assertThat(words, Matchers.contains("200", "800", "1000", "1800", "2200", "2800", "3600", "3800", "4200", "4800"));
            assertThat(Duration.between(startInstant, Instant.now()).toMillis(), TestUtil.between(1500L, 1900L));
            assertTrue(terminated);
        } finally {
            if (service != null) {
                service.close();
            }
        }
    }

    /**
     * If now is 1606087891647, then sleep for 353ms so that the test starts at the whole second.
     * This is because if the time bucket length is 1000ms, the time buckets range from [0, 1000), [1000, 2000), ..., [1606087891000, 1606087892000), ...
     */
    private static void sleepTillNextSecond() {
        long now = System.currentTimeMillis();
        long part = now % 1000;
        if (part == 0) {
            return;
        }
        long remaining = 1000 - part;
        sleep(remaining);
    }


    private static long getNumTimeBuckets(ScheduledExecutorService service) {
        TimeBucketScheduledThreadPoolExecutor serviceImpl = (TimeBucketScheduledThreadPoolExecutor) service;
        return serviceImpl.getTimeBuckets().count();
    }

    private static long getNumOpenFiles(ScheduledExecutorService service) {
        TimeBucketScheduledThreadPoolExecutor serviceImpl = (TimeBucketScheduledThreadPoolExecutor) service;
        return serviceImpl.getTimeBucketOpenFiles().count();
    }

    private static long[] getTimeBuckets(ScheduledExecutorService service) {
        TimeBucketScheduledThreadPoolExecutor serviceImpl = (TimeBucketScheduledThreadPoolExecutor) service;
        return serviceImpl.getTimeBuckets().toArray();
    }

    /**
     * Count the number of time buckets, or the number of files in 'folder'.
     */
    private long countFiles() throws IOException {
        return Files.list(folder).filter(path -> !path.endsWith("index.txt")).count();
    }
}