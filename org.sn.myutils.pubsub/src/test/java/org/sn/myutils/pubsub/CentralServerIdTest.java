package org.sn.myutils.pubsub;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;


public class CentralServerIdTest {
    @Test
    void test() {
        CentralServerId centralServerId = new CentralServerId(1595660400000L);
        System.out.println(centralServerId);
        assertEquals("17384c57580", centralServerId.toString());
        
        assertEquals(-2067434112, centralServerId.intValue());
        assertEquals(1595660400000L, centralServerId.longValue());
        assertThat(centralServerId.floatValue(), Matchers.greaterThan(1.59566e12f));
        assertThat(centralServerId.doubleValue(), Matchers.greaterThan(1.59566e12));
        
        CentralServerId centralServerId1 = new CentralServerId(1595660400000L);
        CentralServerId centralServerId2 = new CentralServerId(1595660400001L);
        assertNotEquals(centralServerId, null);
        assertEquals(centralServerId, centralServerId1);
        assertNotSame(centralServerId, centralServerId1);
        assertNotEquals(centralServerId1, centralServerId2);
        assertEquals(centralServerId.hashCode(), centralServerId1.hashCode());
        assertNotEquals(centralServerId1.hashCode(), centralServerId2.hashCode());
        assertTrue(centralServerId1.compareTo(centralServerId2) < 0);
        assertTrue(centralServerId2.compareTo(centralServerId1) > 0);
        assertEquals(centralServerId1.compareTo(centralServerId), 0);
    }
    
    @Test
    void testDefaultFromNow() throws InterruptedException {
        CentralServerId centralServerId1 = CentralServerId.createDefaultFromNow();
        System.out.println(centralServerId1);
        
        Thread.sleep(1);
        CentralServerId centralServerId2 = CentralServerId.createDefaultFromNow();
        System.out.println(centralServerId2);
        
        assertThat(centralServerId2, Matchers.greaterThan(centralServerId1));
    }
}
