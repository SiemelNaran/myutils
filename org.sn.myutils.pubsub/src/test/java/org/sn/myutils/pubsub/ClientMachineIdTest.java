package org.sn.myutils.pubsub;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.sn.myutils.testutils.LogFailureToConsoleTestWatcher;


@ExtendWith(LogFailureToConsoleTestWatcher.class)
public class ClientMachineIdTest {
    @Test
    public void test() {
        ClientMachineId c2 = new ClientMachineId("two");
        ClientMachineId c1 = new ClientMachineId("one");
        ClientMachineId c1b = new ClientMachineId("one");
        
        assertEquals(c1, c1); // verifies equals() function
        assertEquals(c1, c1b);
        assertEquals(c1.hashCode(), c1b.hashCode());
        assertEquals(0, c1.compareTo(c1b));
        assertNotEquals(c1, c2);
        assertNotEquals(c1.hashCode(), c2.hashCode());
        assertNotEquals(null, c1);
        
        assertEquals("one", c1.toString());
        assertEquals(3, c1.length());
        assertEquals('o', c1.charAt(0));
        assertEquals('n', c1.charAt(1));
        assertEquals('e', c1.charAt(2));
        
        assertEquals("ne", c1.subSequence(1, 3));
        
        assertThat(c1, Matchers.lessThan(c2));
    }
}
