package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import org.junit.jupiter.api.Test;


public class MultimapUtilsTest {
    @Test
    void test() {
        var multimapUtils = new MultimapUtils<>(new HashMap<String, Collection<Integer>>(), ArrayList<Integer>::new);
        multimapUtils.put("hello", 1);
        multimapUtils.put("hello", 3);
        multimapUtils.put("hello", 2);
        multimapUtils.put("world", 5);
        multimapUtils.put("world", 4);
        assertEquals(List.of(1, 3, 2), multimapUtils.get("hello"));
        assertEquals(List.of(5, 4), multimapUtils.get("world"));
        assertTrue(multimapUtils.containsKey("hello"));
        assertTrue(multimapUtils.containsKey("world"));

        assertFalse(multimapUtils.remove("world", 6));
        assertTrue(multimapUtils.remove("world", 5));
        assertEquals(List.of(4), multimapUtils.get("world"));
        assertTrue(multimapUtils.remove("world", 4));
        assertNull(multimapUtils.get("world"));
        assertFalse(multimapUtils.containsKey("world"));
        assertEquals(List.of(1, 3, 2), multimapUtils.get("hello"));

        assertTrue(multimapUtils.removeIf("hello", val -> val >= 2));
        assertEquals(List.of(1), multimapUtils.get("hello"));
        assertTrue(multimapUtils.removeIf("hello", val -> val >= 1));
        assertNull(multimapUtils.get("hello"));
        assertFalse(multimapUtils.containsKey("hello"));

        assertFalse(multimapUtils.remove("carrot", 6));
        assertFalse(multimapUtils.removeIf("carrot", val -> val >= 5));
    }
}
