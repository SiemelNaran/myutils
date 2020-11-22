package org.sn.myutils.util.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class BlockingValue<T> {
    private final CountDownLatch latch = new CountDownLatch(1);
    private T value;

    public T get() throws InterruptedException {
        latch.await();
        return value;
    }

    public T get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        boolean success = latch.await(timeout, unit);
        if (!success) {
            throw new TimeoutException();
        }
        return value;
    }

    public BlockingValue<T> setValue(T value) {
        if (latch.getCount() < 1) {
            throw new IllegalStateException("BlockingValue cannot be modified");
        }
        this.value = value;
        latch.countDown();
        return this;
    }
}

