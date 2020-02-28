package myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;


public class HashLocksTest {
    @Test
    void testLocksWithCollisionTracking() {
        var locks = HashLocks.create(3,
                                     () -> new TimedReentrantLock(false), HashLocks::toStatistics,
                                     HashLocks.CollisionTracking.newBuilder().setNumStringsPerHashCode(3).build());
        Lock lock0 = locks.getLock(0);
        Lock lock1 = locks.getLock(1);
        Lock lock2 = locks.getLock(2);
        assertNotSame(lock0, lock1);
        assertNotSame(lock0, lock2);
        assertNotSame(lock0, lock2);

        assertEquals(Arrays.asList(false, false, false), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::isLocked).collect(Collectors.toList()));
        assertEquals(Arrays.asList(1, 1, 1), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::getUsage).collect(Collectors.toList()));

        Lock lock3 = locks.getLock(3);
        Lock lock4 = locks.getLock(4);
        Lock lock5 = locks.getLock(5);
        assertSame(lock0, lock3);
        assertSame(lock1, lock4);
        assertSame(lock2, lock5);

        Lock lockNegative1 = locks.getLock(-1);
        Lock lockNegative2 = locks.getLock(-2);
        Lock lockNegative3 = locks.getLock(-3);
        assertSame(lock2, lockNegative1);
        assertSame(lock1, lockNegative2);
        assertSame(lock0, lockNegative3);

        assertEquals(Arrays.asList(false, false, false), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::isLocked).collect(Collectors.toList()));
        assertEquals(Arrays.asList(3, 3, 3), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::getUsage).collect(Collectors.toList()));
        // explanation: 3 since 0, 3, -3 have different values for toString() but all hash to lock0

        Lock lock6 = locks.getLock(3);
        Lock lock7 = locks.getLock(4);
        Lock lock8 = locks.getLock(5);
        assertSame(lock0, lock6);
        assertSame(lock1, lock7);
        assertSame(lock2, lock8);

        assertEquals(Arrays.asList(false, false, false), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::isLocked).collect(Collectors.toList()));
        assertEquals(Arrays.asList(3, 3, 3), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::getUsage).collect(Collectors.toList()));
        // explanation: 3 since we track only 3 strings per lock
    }
    
    @Test
    void testLocksWithoutCollisionTracking() {
        var locks = HashLocks.create(3, () -> new TimedReentrantLock(false), HashLocks::toStatistics);
        Lock lock0 = locks.getLock(0);
        Lock lock1 = locks.getLock(1);
        Lock lock2 = locks.getLock(2);
        assertNotSame(lock0, lock1);
        assertNotSame(lock0, lock2);
        assertNotSame(lock0, lock2);

        assertEquals(Arrays.asList(false, false, false), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::isLocked).collect(Collectors.toList()));
        assertEquals(Arrays.asList(-1, -1, -1), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::getUsage).collect(Collectors.toList()));

        Lock lock3 = locks.getLock(3);
        Lock lock4 = locks.getLock(4);
        Lock lock5 = locks.getLock(5);
        assertSame(lock0, lock3);
        assertSame(lock1, lock4);
        assertSame(lock2, lock5);

        Lock lockNegative1 = locks.getLock(-1);
        Lock lockNegative2 = locks.getLock(-2);
        Lock lockNegative3 = locks.getLock(-3);
        assertSame(lock2, lockNegative1);
        assertSame(lock1, lockNegative2);
        assertSame(lock0, lockNegative3);
        
        assertEquals(Arrays.asList(false, false, false), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::isLocked).collect(Collectors.toList()));
        assertEquals(Arrays.asList(-1, -1, -1), locks.statistics().map(HashLocks.TimedReentrantLockStatistics::getUsage).collect(Collectors.toList()));
    }
    
    @Test
    void testStatistics() throws InterruptedException {
        var locks = HashLocks.create(3, () -> new TimedReentrantLock(true), HashLocks::toStatistics, HashLocks.CollisionTracking.newBuilder().build());        
        Lock lock0 = locks.getLock(0);

        final long startOfTime = System.currentTimeMillis();
        
        ScheduledExecutorService service = Executors.newScheduledThreadPool(3);
        service.schedule(() -> {
            print("start 1st at " + (System.currentTimeMillis() - startOfTime));
            lock0.lock();
            try {
                sleep(1000);
            } finally {
                lock0.unlock();
            }
            print("end 1st after 1 second at " + (System.currentTimeMillis() - startOfTime));
        }, 300, TimeUnit.MILLISECONDS);
        service.schedule(() -> {
            print("start 2nd at " + (System.currentTimeMillis() - startOfTime));
            lock0.lock();
            try {
                sleep(2000);
            } finally {
                lock0.unlock();
            }
            print("end 2nd after 2 seconds at " + (System.currentTimeMillis() - startOfTime));
        }, 500, TimeUnit.MILLISECONDS);
        service.schedule(() -> {
            print("start 3rd at " + (System.currentTimeMillis() - startOfTime));
            lock0.lock();
            try {
                sleep(4000);
            } finally {
                lock0.unlock();
            }
            print("end 3rd after 4 seconds at " + (System.currentTimeMillis() - startOfTime));
        }, 700, TimeUnit.MILLISECONDS);

        
        service.shutdown();
        service.awaitTermination(10, TimeUnit.SECONDS);
        
        /* Ideal output:
            start 1st at 300
            start 2nd at 500
            start 3rd at 700
            end 1st after 1 second at 1300
            end 2nd after 2 seconds at 3300
            end 3rd after 4 seconds at 7300
         */
        
        List<HashLocks.TimedReentrantLockStatistics> statistics = locks.statistics().collect(Collectors.toList());
        
        assertEquals(Arrays.asList(1, 0, 0), statistics.stream().map(HashLocks.TimedReentrantLockStatistics::getUsage).collect(Collectors.toList()));
        
        Duration[] waitTimes = statistics.stream().map(HashLocks.TimedReentrantLockStatistics::getTotalWaitTime).toArray(Duration[]::new);
        Duration[] lockRunningTimes = statistics.stream().map(HashLocks.TimedReentrantLockStatistics::getTotalLockRunningTime).toArray(Duration[]::new);
        Duration[] approximateTotalIdleTimes = statistics.stream().map(HashLocks.TimedReentrantLockStatistics::getTotalIdleTime).toArray(Duration[]::new);
        System.out.println("waitTimes: " + toString(waitTimes));
        System.out.println("lockRunningTimes: " + toString(lockRunningTimes));
        System.out.println("approximateTotalIdleTimes: " + toString(approximateTotalIdleTimes));

        assertEquals(3400, waitTimes[0].toMillis(), 40.0); // runnable2 waited 1300-500=800, runnable2 waited 3300-700=2600. sum is 3400 
        assertEquals(0, waitTimes[1].toMillis());
        assertEquals(0, waitTimes[2].toMillis());

        assertEquals(7000, lockRunningTimes[0].toMillis(), 40.0);
        assertEquals(0, lockRunningTimes[1].toMillis());
        assertEquals(0, lockRunningTimes[2].toMillis());

        assertEquals(300, approximateTotalIdleTimes[0].toMillis(), 40.0); 
        assertEquals(7300, approximateTotalIdleTimes[1].toMillis(), 40.0);
        assertEquals(7300, approximateTotalIdleTimes[2].toMillis(), 40.0);

        int[] queueLengths = statistics.stream().mapToInt(HashLocks.TimedReentrantLockStatistics::getQueueLength).toArray();
        assertEquals(0, queueLengths[0]);
        assertEquals(0, queueLengths[1]);
        assertEquals(0, queueLengths[2]);
    }
    
    
    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static void print(String line) {
        System.out.println(line);
    }
    
    private static String toString(Duration[] array) {
        return Arrays.stream(array).mapToLong(Duration::toMillis).mapToObj(Long::toString).collect(Collectors.joining(", "));
    }
}
