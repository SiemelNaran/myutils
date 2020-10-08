package org.sn.myutils.pubsub;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;


public class RetentionPriorityTest {
    @Test
    void testCompare() {
        assertTrue(RetentionPriority.MEDIUM.compareTo(RetentionPriority.HIGH) < 0);
    }

}
