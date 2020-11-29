package org.sn.myutils.util.concurrent;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.sn.myutils.testutils.TestBase;


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
        ScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
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
        assertEquals(3, getNumTimeBuckets(service));
        assertEquals(3, countFiles());
        System.out.println("Time to schedule 6 futures: " + Duration.between(realStart, Instant.now()).toMillis() + "ms"); // typical output: Time to schedule 6 futures: 33ms
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

    @Test
    void cancelAndGet() throws IOException, ExecutionException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        ScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
        assertEquals(0, countFiles());

        sleepTillNextSecond();

        ScheduledFuture<String> future1400 = service.schedule(new MyCallable("1400"), 1400, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        service.schedule(new MyRunnable("1300"), 1300, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        ScheduledFuture<?> future1200 = service.schedule(new MyRunnable("1200"), 1200, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        assertEquals(1, countFiles());
        assertThat(words, Matchers.empty());

        future1200.cancel(true);
        assertEquals("1400", future1400.get());
        assertThat(words, Matchers.contains("1300", "1400"));
    }

    @Test
    void getTimeout() throws IOException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        ScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
        assertEquals(0, countFiles());

        sleepTillNextSecond();

        ScheduledFuture<?> future1300 = service.schedule(new MyRunnable("1300"), 1300, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        assertEquals(1, countFiles());
        assertThat(words, Matchers.empty());

        assertExceptionFromCallable(() -> future1300.get(1, TimeUnit.SECONDS), TimeoutException.class);
    }

    @Test
    void testRejectWhenShutdown() throws IOException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        ScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
        assertEquals(0, countFiles());

        service.shutdown();
        assertExceptionFromCallable(() -> service.schedule(new MyRunnable("600"), 600, TimeUnit.MILLISECONDS), RejectedExecutionException.class);
        assertExceptionFromCallable(() -> service.schedule(new MyCallable("600"), 600, TimeUnit.MILLISECONDS), RejectedExecutionException.class);
    }

    @Test
    void changeBucketLength() throws IOException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        ScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
        TimeBucketScheduledThreadPoolExecutor serviceImpl = (TimeBucketScheduledThreadPoolExecutor) service;
        assertEquals(0, countFiles());

        sleepTillNextSecond();

        service.schedule(new MyRunnable("300"), 300, TimeUnit.MILLISECONDS); // bucket [0000, 1000)
        service.schedule(new MyRunnable("900"), 900, TimeUnit.MILLISECONDS); // bucket [0000, 1000)
        assertEquals(1, countFiles());
        serviceImpl.setTimeBucketLength(Duration.ofMillis(500));
        service.schedule(new MyRunnable("1300"), 1300, TimeUnit.MILLISECONDS); // bucket [1000, 1500)
        service.schedule(new MyRunnable("1900"), 1900, TimeUnit.MILLISECONDS); // bucket [1500, 2000)
        assertEquals(3, countFiles());
        assertThat(words, Matchers.empty());

        service.shutdown();
        service.awaitTermination(1, TimeUnit.MILLISECONDS);
    }

    @Test
    void testAwaitTermination() throws IOException, InterruptedException {
        Duration timeBucketLength = Duration.ofSeconds(1);
        ScheduledExecutorService service = MoreExecutors.newTimeBucketScheduledThreadPool(folder, timeBucketLength, 1, myThreadFactory(), new ThreadPoolExecutor.AbortPolicy());
        TimeBucketScheduledThreadPoolExecutor serviceImpl = (TimeBucketScheduledThreadPoolExecutor) service;
        assertEquals(0, countFiles());

        sleepTillNextSecond();

        service.schedule(new MyRunnable("1800"), 1800, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        service.schedule(new MyRunnable("1900"), 1900, TimeUnit.MILLISECONDS); // bucket [1000, 2000)
        assertEquals(1, countFiles());
        assertThat(words, Matchers.empty());

        service.shutdown();
        assertTrue(service.awaitTermination(2, TimeUnit.SECONDS));
        assertThat(words, Matchers.contains("800", "1900"));
        assertEquals(0, countFiles());
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