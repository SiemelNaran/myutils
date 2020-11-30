package org.sn.myutils.util.concurrent;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;
import org.sn.myutils.testutils.TestUtil;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.sn.myutils.testutils.TestUtil.assertExceptionFromCallable;


class BlockingValueTest {
    @Test
    void testGetAndSet() throws InterruptedException {
        BlockingValue<Integer> number = new BlockingValue<>();

        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.schedule(() -> number.setValue(5), 300, TimeUnit.MILLISECONDS);

        long beforeGet = System.nanoTime();
        assertEquals(5, number.get());
        long afterGet = System.nanoTime();
        assertThat((afterGet - beforeGet) / 1_000_000, TestUtil.between(280L, 320L));

        assertEquals(5, number.get());
        long afterGet2 = System.nanoTime();
        assertThat((afterGet2 - afterGet) / 1_000_000, TestUtil.between(0L, 10L));

        assertExceptionFromCallable(() -> number.setValue(6), IllegalStateException.class);

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MILLISECONDS);
    }

    @Test
    void testGetTimeout() {
        BlockingValue<Integer> number = new BlockingValue<>();

        long beforeGet = System.nanoTime();
        assertExceptionFromCallable(() -> number.get(300, TimeUnit.MILLISECONDS), TimeoutException.class);
        long afterGet = System.nanoTime();
        assertThat((afterGet - beforeGet) / 1_000_000, TestUtil.between(280L, 320L));
    }
}