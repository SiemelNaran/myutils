package myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;


public class LruCacheTest {
    
    @Test
    public void testAddTooMany() {
        LruCache<String, String> cache = new LruCache<>(3);
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        assertNull(cache.put("one", "1"));
        assertFalse(cache.isEmpty());
        assertEquals(1, cache.size());
        assertEquals(Collections.singletonList("one=1"), getListForTesting(cache));
        assertNull(cache.put("two", "2"));
        assertEquals(Arrays.asList("two=2", "one=1"), getListForTesting(cache));
        assertNull(cache.put("three", "3"));
        assertEquals(Arrays.asList("three=3", "two=2", "one=1"), getListForTesting(cache));
        assertNull(cache.put("four", "4"));
        assertEquals(Arrays.asList("four=4", "three=3", "two=2"), getListForTesting(cache));
    }
    
    @Test
    public void testPutExisting() {
        LruCache<String, String> cache = new LruCache<>(3);
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        assertNull(cache.put("one", "1"));
        assertEquals(Collections.singletonList("one=1"), getListForTesting(cache));
        assertNull(cache.put("two", "2"));
        assertEquals(Arrays.asList("two=2", "one=1"), getListForTesting(cache));
        assertNull(cache.put("three", "3"));
        assertEquals(Arrays.asList("three=3", "two=2", "one=1"), getListForTesting(cache));
        assertEquals("2", cache.put("two", "22"));
        assertEquals(Arrays.asList("two=22", "three=3", "one=1"), getListForTesting(cache));
    }

    @Test
    public void testPutSame() {
        LruCache<String, String> cache = new LruCache<>(3);
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        assertNull(cache.put("one", "1"));
        assertEquals(Collections.singletonList("one=1"), getListForTesting(cache));
        assertNull(cache.put("two", "2"));
        assertEquals(Arrays.asList("two=2", "one=1"), getListForTesting(cache));
        assertNull(cache.put("three", "3"));
        assertEquals(Arrays.asList("three=3", "two=2", "one=1"), getListForTesting(cache));
        assertEquals("3", cache.put("three", "33"));
        assertEquals(Arrays.asList("three=33", "two=2", "one=1"), getListForTesting(cache));
    }

    @Test
    public void testGetExisting() {
        LruCache<String, String> cache = new LruCache<>(3);
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        assertNull(cache.put("one", "1"));
        assertEquals(Collections.singletonList("one=1"), getListForTesting(cache));
        assertNull(cache.put("two", "2"));
        assertEquals(Arrays.asList("two=2", "one=1"), getListForTesting(cache));
        assertNull(cache.put("three", "3"));
        assertEquals(Arrays.asList("three=3", "two=2", "one=1"), getListForTesting(cache));
        
        assertEquals("2", cache.get("two")); // get element in middle of linked list
        assertEquals(Arrays.asList("two=2", "three=3", "one=1"), getListForTesting(cache));

        assertEquals("2", cache.get("two")); // get element at head of linked list
        assertEquals(Arrays.asList("two=2", "three=3", "one=1"), getListForTesting(cache));
        
        assertEquals("1", cache.get("one")); // get element at tail of linked list
        assertEquals(Arrays.asList("one=1", "two=2", "three=3"), getListForTesting(cache));
        
        assertNull(cache.get("nine"));
    }
    
    @Test
    public void testRemove() {
        LruCache<String, String> cache = new LruCache<>(3);
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        assertNull(cache.put("one", "1"));
        assertEquals(Collections.singletonList("one=1"), getListForTesting(cache));
        assertNull(cache.put("two", "2"));
        assertEquals(Arrays.asList("two=2", "one=1"), getListForTesting(cache));
        assertNull(cache.put("three", "3"));
        assertFalse(cache.isEmpty());
        assertEquals(3, cache.size());
        assertEquals(Arrays.asList("three=3", "two=2", "one=1"), getListForTesting(cache));
        
        assertEquals("2", cache.remove("two")); // remove element in middle of linked list
        assertEquals(Arrays.asList("three=3", "one=1"), getListForTesting(cache));
        
        assertEquals("3", cache.remove("three")); // remove element at head of linked list
        assertEquals(Collections.singletonList("one=1"), getListForTesting(cache));

        assertNull(cache.put("two", "2"));
        assertEquals(Arrays.asList("two=2", "one=1"), getListForTesting(cache));
        
        assertEquals("1", cache.remove("one")); // remove element at tail of linked list
        assertEquals(Collections.singletonList("two=2"), getListForTesting(cache));
        
        assertEquals("2", cache.remove("two")); // remove last element from linked list
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());
        assertEquals(Arrays.asList(), getListForTesting(cache));

        assertNull(cache.remove("nine"));
    }
    
    private static List<String> getListForTesting(LruCache<String,String> cache) {
        List<String> list = cache.getCacheForTesting();
        
        List<String> listCopy = new ArrayList<>(list);
        Collections.reverse(listCopy);
        List<String> reverseList = cache.getReverseCacheForTesting();
        assertEquals(listCopy, reverseList);
        
        assertEquals(list.size(), cache.size());
        
        return list;
    }
    
    @Test
    public void testCompareToLinkedHashMap() {
        {
            Instant startTime = Instant.now();
            LinkedHashMap<String, String> cache = new LinkedHashMap<String, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return size() > 1024;
                }
            };
            for (int i = 0; i < 16777216; i++) {
                cache.put(Integer.toString(i), "value " + Integer.toString(i));
            }
            System.out.println("LinkedHashMap: " + Duration.between(startTime, Instant.now()).toMillis());
        }
        
        {
            Instant startTime = Instant.now();
            LruCache<String, String> cache = new LruCache<>(1024);
            for (int i = 0; i < 16777216; i++) {
                cache.put(Integer.toString(i), "value " + Integer.toString(i));
            }
            System.out.println("LruCache: " + Duration.between(startTime, Instant.now()).toMillis());
        }
    }
}
