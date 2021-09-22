package org.sn.myutils.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Serial;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;


/**
 * The purpose of these tests are to show how LinkedHashMap works in detail.
 */
public class LinkedHashMapTest {
    @Test
    void testBasic() {
        Map<String, String> cache = new LinkedHashMap<>(15, 0.75f, true) {
            @Serial
            private static final long serialVersionUID = 1L;

            @Override
            protected boolean removeEldestEntry(Map.Entry<String, String> oldest) {
                return size() > 3;
            }
        };
        assertNull(cache.put("one", "1"));
        assertNull(cache.put("two", "2"));
        assertNull(cache.put("three", "3"));
        assertThat(getListFromIterator(cache), Matchers.containsInAnyOrder("three=3", "two=2", "one=1"));

        assertEquals("1", cache.get("one"));
        assertThat(getListFromIterator(cache), Matchers.containsInAnyOrder("one=1", "three=3", "two=2"));

        assertNull(cache.put("four", "4"));
        assertThat(getListFromIterator(cache), Matchers.containsInAnyOrder("four=4", "one=1", "three=3"));

        assertTrue(cache.containsKey("three"));
        assertNull(cache.put("five", "5"));
        assertThat(getListFromIterator(cache), Matchers.containsInAnyOrder("five=5", "four=4", "one=1"));

        assertTrue(cache.containsValue("1"));
        assertNull(cache.put("six", "6"));
        assertThat(getListFromIterator(cache), Matchers.containsInAnyOrder("six=6", "five=5", "four=4"));
    }

    private static List<String> getListFromIterator(Map<String, String> map) {
        return map.entrySet().stream().map(Map.Entry::toString).toList();
    }
}
