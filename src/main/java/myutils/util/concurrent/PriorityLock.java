package myutils.util.concurrent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;


/**
 * A lock which builds upon reentrant lock by giving preference to the queue with the highest priority.
 * 
 * <ul>Here is how this class implements it:
 *   <li>This class has an internal ReentrantLock.
 *   <li>Threads have priorities between 1 and 10 inclusive, with 5 being the default priority.</li>
 *   <li>Thus we have 10 counters and 10 conditions based on the internal ReentrantLock.</li>
 *   <li>When a thread of priority N requests a lock,
 *           the lock makes a note that a thread of priority N is waiting for a lock by incrementing the counter for this priority.</li>
 *   <li>When a thread gets the internal lock, we check if there are any threads with higher priority in the queue (simply by checking if the count > 0</li>
 *   <li>If there is, then we await on that priority's condition</li>
 *   <li>When unlock is called we decrement the count for the thread's priority,
 *           and signal the condition object so that any threads waiting on this condition wake up and start running.</li>
 * </ul>
 * 
 * @see Thread#MIN_PRIORITY
 * @see Thread#NORM_PRIORITY
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

    private final ReentrantLock lock;
    private final Level[] levels;

    public PriorityLock() {
        lock = new ReentrantLock(true);
        levels = new Level[Thread.MAX_PRIORITY];
        for (int i = 0; i < Thread.MAX_PRIORITY; i++) {
            levels[i] = new Level(lock);
        }
    }

    @Override
    public void lock() {
        Thread currentThread = Thread.currentThread();
        addThread(currentThread);
        try {
            lock.lock();
            try {
                waitForHigherPriorityTasksToFinish(currentThread);
            } catch (InterruptedException e) {
                throw new CancellationException(e.getMessage());
            }
        } catch (RuntimeException | Error e) {
            Condition condition = removeThread(currentThread);
            condition.signalAll();
        }
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
        Thread currentThread = Thread.currentThread();
        addThread(currentThread);
        try {
            lock.lockInterruptibly();
            waitForHigherPriorityTasksToFinish(currentThread);
        } catch (InterruptedException | RuntimeException | Error e) {
            Condition condition = removeThread(currentThread);
            condition.signalAll();
        }
    }

    @Override
    public boolean tryLock() {
        Thread currentThread = Thread.currentThread();
        addThread(currentThread);
        try {
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
        } catch (RuntimeException | Error e) {
            Condition condition = removeThread(currentThread);
            condition.signalAll();
            return false;
        }
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
        Thread currentThread = Thread.currentThread();
        addThread(currentThread);
        try {
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
        } catch (InterruptedException | RuntimeException | Error e) {
            Condition condition = removeThread(currentThread);
            condition.signalAll();
            return false;
        }
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
