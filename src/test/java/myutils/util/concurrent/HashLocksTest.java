package myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.junit.jupiter.api.Test;


public class HashLocksTest {
    @Test
    void testLock1() {
        HashLocks<ReentrantLock> locks = new HashLocks<>(ReentrantLock.class, 3);
        Lock lock0 = locks.getLock(0);
        Lock lock1 = locks.getLock(1);
        Lock lock2 = locks.getLock(2);
        assertNotSame(lock0, lock1);
        assertNotSame(lock0, lock2);
        assertNotSame(lock0, lock2);

        Lock lock3 = locks.getLock(3);
        Lock lock4 = locks.getLock(4);
        Lock lock5 = locks.getLock(5);
        assertSame(lock0, lock3);
        assertSame(lock1, lock4);
        assertSame(lock2, lock5);

        Lock lock_1 = locks.getLock(-1);
        Lock lock_2 = locks.getLock(-2);
        Lock lock_3 = locks.getLock(-3);
        assertSame(lock2, lock_1);
        assertSame(lock1, lock_2);
        assertSame(lock0, lock_3);
    }
    
    @Test
    void testLock2() {
        HashLocks<ReentrantLock> locks = new HashLocks<>(() -> new ReentrantLock(true), 3);
        Lock lock0 = locks.getLock(0);
        Lock lock1 = locks.getLock(1);
        Lock lock2 = locks.getLock(2);
        assertNotSame(lock0, lock1);
        assertNotSame(lock0, lock2);
        assertNotSame(lock0, lock2);

        Lock lock3 = locks.getLock(3);
        Lock lock4 = locks.getLock(4);
        Lock lock5 = locks.getLock(5);
        assertSame(lock0, lock3);
        assertSame(lock1, lock4);
        assertSame(lock2, lock5);

        Lock lock_1 = locks.getLock(-1);
        Lock lock_2 = locks.getLock(-2);
        Lock lock_3 = locks.getLock(-3);
        assertSame(lock2, lock_1);
        assertSame(lock1, lock_2);
        assertSame(lock0, lock_3);
    }
}

