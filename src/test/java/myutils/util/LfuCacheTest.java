package myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;


public class LfuCacheTest {    
    @Test
    public void testAddTooMany() {
        LfuCache<String, String> cache = new LfuCache<>(3);
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        cache.put("one", "1");
        assertFalse(cache.isEmpty());
        assertEquals(1, cache.size());
        assertEquals(Arrays.asList("1:one=1"), getListForTesting(cache));
        cache.put("two", "2");
        assertEquals(Arrays.asList("1:two=2", "1:one=1"), getListForTesting(cache));
        cache.put("three", "3");
        assertEquals(Arrays.asList("1:three=3", "1:two=2", "1:one=1"), getListForTesting(cache));
        cache.put("four", "4");
        assertEquals(Arrays.asList("1:four=4", "1:three=3", "1:two=2"), getListForTesting(cache));
    }
    
    @Test
    public void testPutAndGet() {
        LfuCache<String, String> cache = new LfuCache<>(3);
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        
        // put one=1 into cache and get it 3 times
        cache.put("one", "1");
        assertEquals(Arrays.asList("1:one=1"), getListForTesting(cache));
        assertEquals("1", cache.get("one"));
        assertEquals(Arrays.asList("2:one=1"), getListForTesting(cache));
        assertEquals("1", cache.get("one"));
        assertEquals(Arrays.asList("3:one=1"), getListForTesting(cache));
        
        // put two=2 into cache and get it 2 times
        cache.put("two", "2");
        assertEquals(Arrays.asList("3:one=1", "1:two=2"), getListForTesting(cache));
        assertEquals("2", cache.get("two"));
        assertEquals(Arrays.asList("3:one=1", "2:two=2"), getListForTesting(cache));
        
        // put three=3 into cache
        cache.put("three", "3");
        assertEquals(Arrays.asList("3:one=1", "2:two=2", "1:three=3"), getListForTesting(cache));
        
        // put four=4 into cache, evicting three=3
        cache.put("four", "4");
        assertEquals(Arrays.asList("3:one=1", "2:two=2", "1:four=4"), getListForTesting(cache));
        
        // put five=5 into cache, evicting four=4, and get it 5 times
        cache.put("five", "5");
        assertEquals(Arrays.asList("3:one=1", "2:two=2", "1:five=5"), getListForTesting(cache));
        assertEquals("5", cache.get("five"));
        assertEquals(Arrays.asList("3:one=1", "2:five=5", "2:two=2"), getListForTesting(cache));
        assertEquals("5", cache.get("five"));
        assertEquals(Arrays.asList("3:five=5", "3:one=1", "2:two=2"), getListForTesting(cache));
        assertEquals("5", cache.get("five"));
        assertEquals(Arrays.asList("4:five=5", "3:one=1", "2:two=2"), getListForTesting(cache));
        assertEquals("5", cache.get("five"));
        assertEquals(Arrays.asList("5:five=5", "3:one=1", "2:two=2"), getListForTesting(cache));
        assertEquals("5", cache.get("five"));
        assertEquals(Arrays.asList("6:five=5", "3:one=1", "2:two=2"), getListForTesting(cache));
        
        // put six=6 into cache, evicting two=2
        cache.put("six", "6");
        assertEquals(Arrays.asList("6:five=5", "3:one=1", "1:six=6"), getListForTesting(cache));
        
        assertNull(cache.get("nine"));
    }
    
    @Test
    public void testRemoveLeastFrequentThenRemoveAll() {
        LfuCache<String, String> cache = new LfuCache<>(3);
        cache.put("one", "1");
        cache.put("two", "2");
        cache.put("three", "3");
        assertFalse(cache.isEmpty());
        assertEquals(3, cache.size());
        cache.get("one");
        cache.get("two");
        assertEquals(Arrays.asList("2:two=2", "2:one=1", "1:three=3"), getListForTesting(cache));
        assertEquals("3", cache.remove("three"));
        assertEquals(Arrays.asList("2:two=2", "2:one=1"), getListForTesting(cache));
        assertEquals("2", cache.remove("two"));
        assertEquals(Arrays.asList("2:one=1"), getListForTesting(cache));
        assertEquals("1", cache.remove("one"));
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());
        assertEquals(Arrays.asList(), getListForTesting(cache));
        assertNull(cache.remove("nine"));
    }
    
    @Test
    public void testRemoveMostFrequent() {
        LfuCache<String, String> cache = new LfuCache<>(3);
        cache.put("one", "1");
        cache.put("two", "2");
        cache.put("three", "3");
        assertEquals(Arrays.asList("1:three=3", "1:two=2", "1:one=1"), getListForTesting(cache));
        assertEquals("2", cache.get("two"));
        assertEquals(Arrays.asList("2:two=2", "1:three=3", "1:one=1"), getListForTesting(cache));
        assertEquals("2", cache.remove("two"));
        assertEquals(Arrays.asList("1:three=3", "1:one=1"), getListForTesting(cache));
    }
    
    @Test
    public void testIncreaseFrequencyOfMiddlePage() {
        LfuCache<String, String> cache = new LfuCache<>(4);
        cache.put("one", "1");
        for (int i = 0; i < 3; i++) {
            cache.get("one");
        }
        cache.put("two", "2");
        cache.put("three", "3");
        for (int i = 0; i < 1; i++) {
            cache.get("three");
            cache.get("two");
        }
        cache.put("four", "4");
        assertEquals(Arrays.asList("4:one=1", "2:two=2", "2:three=3", "1:four=4"), getListForTesting(cache));
        
        assertEquals("3", cache.get("three"));
        assertEquals(Arrays.asList("4:one=1", "3:three=3", "2:two=2", "1:four=4"), getListForTesting(cache));
    }
    
    @Test
    public void testRemoveMiddlePage() {
        LfuCache<String, String> cache = new LfuCache<>(3);
        cache.put("one", "1");
        cache.put("two", "2");
        cache.put("three", "3");
        assertEquals("3", cache.get("three"));
        assertEquals("3", cache.get("three"));
        assertEquals("2", cache.get("two"));
        assertEquals(Arrays.asList("3:three=3", "2:two=2", "1:one=1"), getListForTesting(cache));
        assertEquals("2", cache.remove("two"));
        assertEquals(Arrays.asList("3:three=3", "1:one=1"), getListForTesting(cache));
    }
    
    @Test
    public void testGetAndPutBothIncreaseFrequencyTheSameWay() {
        LfuCache<String, String> cache = new LfuCache<>(3);
        cache.put("one", "1");
        cache.put("two", "2");
        cache.put("three", "3");
        cache.put("two", "2");
        cache.put("three", "3");
        cache.put("three", "3");
        assertEquals(Arrays.asList("3:three=3", "2:two=2", "1:one=1"), getListForTesting(cache)); // same as above test
    }
    
    private static List<String> getListForTesting(LfuCache<String,String> cache) {
        List<String> list = cache.getCacheForTesting();
        
        List<String> listCopy = new ArrayList<>(list);
        Collections.reverse(listCopy);
        List<String> reverseList = cache.getReverseCacheForTesting();
        assertEquals(listCopy, reverseList);
        
        assertEquals(list.size(), cache.size());
        
        List<Integer> sizes = cache.getSizesForTesting();
        assertFalse(sizes.contains(0));
        assertEquals(list.size(), sizes.stream().collect(Collectors.summingInt(val -> val)).intValue());
        
        return list;
    }
}
