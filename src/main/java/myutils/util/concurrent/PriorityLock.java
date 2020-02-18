package myutils.util.concurrent;

import javax.annotation.Nonnull;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A lock which builds upon reentrant lock by giving preference to the queue with the highest priority.
 * This implementation uses Condition objects to wait on threads with a higher priority to finish.
 *
 * @see Thread#MIN_PRIORITY
 * @see Thread#MAX_PRIORITY
 */
public class PriorityLock implements Lock {
    private static class Level {
        private final AtomicInteger count = new AtomicInteger();
        private final Condition condition;

        private Level(Lock lock) {
            condition = lock.newCondition();
        }
    }

    private final ReentrantLock lock ;
    private final Level[] levels;

    public PriorityLock() {
        lock = new ReentrantLock();
        levels = new Level[Thread.MAX_PRIORITY];
        for (int i = 0; i < Thread.MAX_PRIORITY; i++) {
            levels[i] = new Level(lock);
        }
    }

    @Override
    public void lock() {
        Thread currentThread = Thread.currentThread();
        addThread(currentThread);
        lock.lock();
        try {
            waitForHigherPriorityTasksToFinish(currentThread);
        } catch (InterruptedException e) {
            throw new CancellationException(e.getMessage());
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        Thread currentThread = Thread.currentThread();
        addThread(currentThread);
        lock.lockInterruptibly();
        waitForHigherPriorityTasksToFinish(currentThread);
    }

    @Override
    public boolean tryLock() {
        Thread currentThread = Thread.currentThread();
        addThread(currentThread);
        boolean acquired = lock.tryLock();
        if (acquired) {
            int priority = currentThread.getPriority();
            Integer nextHigherPriority = computeNextHigherPriority(priority);
            if (nextHigherPriority != null) {
                lock.unlock();
                acquired = false;
            }
        }
        return acquired;
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        Thread currentThread = Thread.currentThread();
        addThread(currentThread);
        boolean acquired = lock.tryLock(time, unit);
        if (acquired) {
            int priority = currentThread.getPriority();
            Integer nextHigherPriority = computeNextHigherPriority(priority);
            if (nextHigherPriority != null) {
                lock.unlock();
                acquired = false;
            }
        }
        return acquired;
    }

    @Override
    public void unlock() {
        Thread currentThread = Thread.currentThread();
        Condition condition = removeThread(currentThread);
        condition.signalAll();
        lock.unlock();
    }

    @Override
    public @Nonnull Condition newCondition() {
        return lock.newCondition();
    }

    private void addThread(Thread currentThread) {
        int index = currentThread.getPriority() - 1;
        levels[index].count.incrementAndGet();
    }

    private void waitForHigherPriorityTasksToFinish(Thread currentThread) throws InterruptedException {
        int priority = currentThread.getPriority();
        while (true) {
            Integer nextHigherPriority = computeNextHigherPriority(priority);
            if (nextHigherPriority == null) {
                break;
            }
            levels[nextHigherPriority].condition.await();
        }
    }

    private Integer computeNextHigherPriority(int priority) {
        for (int i = priority; i < Thread.MAX_PRIORITY; i++) {
            if (levels[i].count.get() > 0) {
                return i;
            }
        }
        return null;
    }

    private @Nonnull Condition removeThread(Thread currentThread) {
        int index = currentThread.getPriority() - 1;
        levels[index].count.decrementAndGet();
        return levels[index].condition;
    }
}
