package myutils.pubsub;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

import java.math.BigInteger;
import myutils.TestBase;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;


public class ServerIndexTest extends TestBase {
    @Test
    void test() {
        BigInteger value = BigInteger.valueOf(1595660400000L);
        System.out.println(value.toString(16));
        value = value.shiftLeft(Long.SIZE);
        System.out.println(value.toString(16));

        ServerIndex serverIndex = new ServerIndex(1595660400000L);
        System.out.println(serverIndex.toString());
        assertThat(serverIndex.toString(), Matchers.endsWith("0000000000000000")); // 16 0's
        // timestamps in 2020 are 13 digits decimal, or 11 digits in hex.
        // the largest 11 digit hex number is 0xfffffffffff or 2527-06-23-06:20:44 TZ
        // so the below check should be valid till then
        assertEquals(27, serverIndex.toString().length());
        
        ServerIndex nextServerIndex = serverIndex.increment();
        assertThat(nextServerIndex.toString(), Matchers.endsWith("0000000000000001"));
        
        assertEquals(1, nextServerIndex.intValue());
        assertEquals(1L, nextServerIndex.longValue());
        assertThat(nextServerIndex.floatValue(), Matchers.greaterThan(1.0f));
        assertThat(nextServerIndex.doubleValue(), Matchers.greaterThan(1.0));
        
        assertNotEquals(serverIndex, nextServerIndex);
        assertNotEquals(serverIndex.hashCode(), nextServerIndex.hashCode());
        
        ServerIndex otherServerIndex = serverIndex.increment();
        assertNotSame(nextServerIndex, otherServerIndex);
        assertEquals(nextServerIndex, otherServerIndex);
        assertEquals(nextServerIndex.hashCode(), otherServerIndex.hashCode());
    }
    
    @Test
    void testMinAndMax() {
        System.out.println("min=" + ServerIndex.MIN_VALUE);
        System.out.println("max=" + ServerIndex.MAX_VALUE);
        assertThat(ServerIndex.compare(ServerIndex.MIN_VALUE, ServerIndex.MAX_VALUE), Matchers.lessThan(0));
        assertThat(ServerIndex.MIN_VALUE, Matchers.lessThan(ServerIndex.MAX_VALUE));
        
        ServerIndex overflow = ServerIndex.MAX_VALUE.increment();
        System.out.println("overflow=" + overflow);
        assertThat(overflow, Matchers.greaterThan(ServerIndex.MAX_VALUE));
    }

    @Test
    void testDefaultFromNow() throws InterruptedException {
        ServerIndex serverIndex1 = ServerIndex.createDefaultFromNow();
        System.out.println(serverIndex1.toString());
        
        Thread.sleep(1);
        ServerIndex serverIndex2 = ServerIndex.createDefaultFromNow();
        System.out.println(serverIndex2.toString());
        
        assertThat(serverIndex2, Matchers.greaterThan(serverIndex1));
    }
}