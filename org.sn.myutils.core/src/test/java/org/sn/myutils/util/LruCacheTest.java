package org.sn.myutils.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.sn.myutils.testutils.TestUtil.assertException;
import static org.sn.myutils.testutils.TestUtil.assertExceptionFromCallable;

import java.io.Serial;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Random;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.sn.myutils.testutils.TestUtil;


public class LruCacheTest {
    @Test
    void testInvalidSize() {
        assertExceptionFromCallable(() -> new LruCache<>(0), IllegalArgumentException.class);
    }

    @Test
    void testEviction() {
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

        assertTrue(cache.containsKey("three"));
        assertTrue(cache.containsKey("two"));
        assertFalse(cache.containsKey("one"));
        assertTrue(cache.containsKey("four"));
        assertEquals(Arrays.asList("four=4", "three=3", "two=2"), getListForTesting(cache));

        assertTrue(cache.containsValue("3"));
        assertTrue(cache.containsValue("2"));
        assertFalse(cache.containsValue("1"));
        assertTrue(cache.containsValue("4"));
        assertEquals(Arrays.asList("four=4", "three=3", "two=2"), getListForTesting(cache));
    }

    @Test
    void testPutExisting() {
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
    void testGetExisting() {
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
        assertEquals(Arrays.asList("one=1", "two=2", "three=3"), getListForTesting(cache));
    }
    
    @Test
    void testRemove() {
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
        assertEquals(Collections.emptyList(), getListForTesting(cache));

        assertNull(cache.remove("nine"));
        assertTrue(cache.isEmpty());
    }
    
    @ParameterizedTest
    @ValueSource(strings = { "clear", "entrySet.clear" })
    void testClear(String method) {
        LruCache<String, String> cache = new LruCache<>(3);
        assertTrue(cache.isEmpty());
        assertNull(cache.put("one", "1"));
        assertNull(cache.put("two", "2"));
        assertNull(cache.put("three", "3"));
        assertEquals(3, cache.size());
        assertEquals(Arrays.asList("three=3", "two=2", "one=1"), getListForTesting(cache));
        assertFalse(cache.isEmpty());
        if ("clear".equals(method)) {
            cache.clear();
        } else {
            cache.entrySet().clear();
        }
        assertEquals(0, cache.size());
        assertTrue(cache.isEmpty());
        assertEquals(Collections.emptyList(), getListForTesting(cache));

        assertTrue(cache.isEmpty());
        assertNull(cache.put("one", "1"));
        assertNull(cache.put("two", "2"));
        assertNull(cache.put("three", "3"));
        assertEquals(3, cache.size());
        assertEquals(Arrays.asList("three=3", "two=2", "one=1"), getListForTesting(cache));
        assertFalse(cache.isEmpty());
    }
    
    @Test
    void testRemoveOldest() {
        LruCache<String, String> cache = new LruCache<>(3);
        assertTrue(cache.isEmpty());
        assertNull(cache.put("one", "1"));
        assertNull(cache.put("two", "2"));
        assertNull(cache.put("three", "3"));
        assertEquals(3, cache.size());
        assertEquals(Arrays.asList("three=3", "two=2", "one=1"), getListForTesting(cache));
        cache.removeOldest();
        assertEquals(Arrays.asList("three=3", "two=2"), getListForTesting(cache));
        cache.removeOldest();
        assertEquals(Collections.singletonList("three=3"), getListForTesting(cache));
        assertFalse(cache.isEmpty());
        cache.removeOldest();
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        assertTrue(cache.isEmpty());
    }
    
    @Test
    void testEntrySet() {
        LruCache<String, String> cache = new LruCache<>(4);
        assertTrue(cache.isEmpty());
        assertNull(cache.put("one", "1"));
        assertNull(cache.put("two", "2"));
        assertNull(cache.put("three", "3"));
        assertNull(cache.put("four", "4"));
        assertEquals("3", cache.get("three"));
        assertEquals("3", cache.get("three"));
        assertEquals("2", cache.get("two"));
        assertEquals(4, cache.size());
        assertEquals(4, cache.entrySet().size());
        assertEquals(Arrays.asList("two=2", "three=3", "four=4", "one=1"), getListForTesting(cache));

        {
            var iter = cache.entrySet().iterator();
            assertTrue(iter.hasNext());
            List<String> keys = new ArrayList<>();
            keys.add(iter.next().toString());
            keys.add(iter.next().toString());
            keys.add(iter.next().toString());
            keys.add(iter.next().toString());
            assertFalse(iter.hasNext());
            assertThat(keys, Matchers.contains("two=2", "three=3", "four=4", "one=1"));
            assertExceptionFromCallable(iter::next, NoSuchElementException.class);
            assertEquals(Arrays.asList("two=2", "three=3", "four=4", "one=1"), getListForTesting(cache));
        }

        {
            var iter = (LruCache<String, String>.ConcurrentModificationManager.LruCacheIterator) cache.entrySet().iterator();
            var lruCacheEntry = iter.next();
            lruCacheEntry.concurrentModificationManager().incrementExpectedModCount();
            assertExceptionFromCallable(() -> lruCacheEntry.setValue("22"), ConcurrentModificationException.class);
        }

        {
            var iter = cache.entrySet().iterator();
            assertTrue(iter.hasNext());
            assertException(iter::remove, IllegalStateException.class);
            iter.next();
            iter.remove();
            assertException(iter::remove, IllegalStateException.class);
            Entry<String, String> entryThree = iter.next();
            Entry<String, String> entryFour = iter.next();
            assertEquals("4", entryFour.setValue("44"));
            assertEquals("44", entryFour.setValue("444"));
            Entry<String, String> entryOne = iter.next();
            assertFalse(iter.hasNext());
            List<String> keys = new ArrayList<>();
            keys.add(entryThree.toString());
            keys.add(entryFour.toString());
            keys.add(entryOne.toString());
            assertThat(keys, Matchers.contains("three=3", "four=444", "one=1"));
            assertEquals(Arrays.asList("four=444", "three=3", "one=1"), getListForTesting(cache));
        }
        
        {
            // verify equals and hashCode
            
            var iter = cache.entrySet().iterator();
            Entry<String, String> entryFour = iter.next();
            Entry<String, String> entryThree = iter.next();
            Entry<String, String> entryOne = iter.next();
            assertEquals("four=444", entryFour.toString());
            assertEquals("three=3", entryThree.toString());
            assertEquals("one=1", entryOne.toString());
            assertFalse(iter.hasNext());

            assertEquals("four", entryFour.getKey());
            assertEquals("444", entryFour.getValue());

            assertNotEquals(entryFour.hashCode(), entryThree.hashCode());
            assertNotEquals(entryFour, entryThree);
            assertNotEquals(entryFour, null);
            
            var anotherEntryFour = cache.entrySet().iterator().next();
            assertNotSame(entryFour, anotherEntryFour);
            assertEquals(entryFour.hashCode(), anotherEntryFour.hashCode());
            assertEquals(entryFour, anotherEntryFour);
            
            LruCache<String, String> cache2 = new LruCache<>(1);
            cache2.put("four", "444");
            Entry<String, String> entryFourFromCache2 = cache2.entrySet().iterator().next();
            assertEquals(entryFour, entryFourFromCache2);
            
            LruCache<String, String> cache3 = new LruCache<>(1);
            cache3.put("four", "newvalue");
            Entry<String, String> entryFourFromCache3 = cache3.entrySet().iterator().next();
            assertNotEquals(entryFour, entryFourFromCache3);
        }
    }

    @Test
    void testEntrySet2() {
        LruCache<String, String> cache = new LruCache<>(4);
        assertTrue(cache.isEmpty());
        assertNull(cache.put("one", "1"));
        assertNull(cache.put("two", "2"));
        assertNull(cache.put("three", "3"));
        assertNull(cache.put("four", "4"));
        assertEquals("3", cache.get("three"));
        assertEquals("3", cache.get("three"));
        assertEquals("2", cache.get("two"));
        assertEquals(4, cache.size());
        assertEquals(4, cache.entrySet().size());
        assertEquals(Arrays.asList("two=2", "three=3", "four=4", "one=1"), getListForTesting(cache));

        var iter = cache.entrySet().iterator();
        assertTrue(iter.hasNext());
        iter.next();
        Entry<String, String> entryThree = iter.next();
        Entry<String, String> entryFour = iter.next();
        entryThree.setValue("33");
        assertEquals(Arrays.asList("three=33", "two=2", "four=4", "one=1"), getListForTesting(cache));
        entryFour.setValue("44");
        assertEquals(Arrays.asList("four=44", "three=33", "two=2", "one=1"), getListForTesting(cache));
        entryThree.setValue("333");
        assertEquals(Arrays.asList("three=333", "four=44", "two=2", "one=1"), getListForTesting(cache));
    }

    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(strings = {"getLatest", "get", "put", "remove"})
    void testIteratorFailFast(String method) {
        LruCache<String, String> cache = new LruCache<>(4);
        assertTrue(cache.isEmpty());
        assertNull(cache.put("one", "1"));
        assertNull(cache.put("two", "2"));
        assertNull(cache.put("three", "3"));
        assertNull(cache.put("four", "4"));
        assertEquals(Arrays.asList("four=4", "three=3", "two=2", "one=1"), getListForTesting(cache));
        Iterator<Entry<String, String>> iter = cache.entrySet().iterator();
        assertTrue(iter.hasNext());
        switch (method) {
            case "getLatest" -> cache.get("four");
            case "get" -> cache.get("three");
            case "put" -> cache.put("five", "5");
            case "remove" -> cache.remove("four");
            default -> throw new UnsupportedOperationException();
        }
        assertTrue(iter.hasNext());
        assertExceptionFromCallable(iter::next, ConcurrentModificationException.class);
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

    ////////////////////////////////////////////////////////////////////////////////////

    /**
     * Test to compare LinkedHashMap to LruCache.
     * In LruCache, iteration order is most recently accessed or written to least recent.
     *
     * @see LinkedHashMapTest#testBasic() 
     */
    @Test
    void testBasic() {
        Map<String, String> cache = new LruCache<>(3);
        assertNull(cache.put("one", "1"));
        assertNull(cache.put("two", "2"));
        assertNull(cache.put("three", "3"));
        assertThat(getListFromIterator(cache), Matchers.contains("three=3", "two=2", "one=1"));

        assertEquals("1", cache.get("one"));
        assertThat(getListFromIterator(cache), Matchers.contains("one=1", "three=3", "two=2"));

        assertNull(cache.put("four", "4"));
        assertThat(getListFromIterator(cache), Matchers.contains("four=4", "one=1", "three=3"));

        assertTrue(cache.containsKey("three"));
        assertNull(cache.put("five", "5"));
        assertThat(getListFromIterator(cache), Matchers.contains("five=5", "four=4", "one=1"));

        assertTrue(cache.containsValue("1"));
        assertNull(cache.put("six", "6"));
        assertThat(getListFromIterator(cache), Matchers.contains("six=6", "five=5", "four=4"));
    }

    private static List<String> getListFromIterator(Map<String, String> map) {
        return map.entrySet().stream().map(Map.Entry::toString).toList();
    }

    ////////////////////////////////////////////////////////////////////////////////////

    /**
     * This test has no assertions.
     * It just compare the speed of LinkedHashMap with constructor argument accessOrder=true to LruCache.
     * 
     * @see LruCache for comment on performance.
     */
    @Test
    void testCompareToLinkedHashMap() {
        final int maxSize = 25_000;
        final int numIterations = 25_000_000;
        int randomIntRange = 1_000;
        long randomSeed = new Random().nextLong();
        final long linkedHashMapTime;
        final long lruCacheTime;

        {
            Instant startTime = Instant.now();
            LinkedHashMap<String, String> cache = new LinkedHashMap<>(400_000, 0.75f, true) {
                @Serial
                private static final long serialVersionUID = 1L;

                @Override
                protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
                    return size() > maxSize;
                }
            };
            Random random = new Random(randomSeed);
            for (int i = 0; i < numIterations; i++) {
                cache.put(Integer.toString(random.nextInt(randomIntRange)), "value " + i);
            }
            linkedHashMapTime = Duration.between(startTime, Instant.now()).toMillis();
            System.out.println("size=" + cache.size()); // to avoid warning "Contents of collection 'cache' are updated, but never queried"
            System.out.println("LinkedHashMap: " + linkedHashMapTime);
        }
        
        {
            Instant startTime = Instant.now();
            LruCache<String, String> cache = new LruCache<>(maxSize);
            Random random = new Random(randomSeed);
            for (int i = 0; i < numIterations; i++) {
                cache.put(Integer.toString(random.nextInt(randomIntRange)), "value " + i);
            }
            lruCacheTime = Duration.between(startTime, Instant.now()).toMillis();
            System.out.println("size=" + cache.size()); // to avoid warning "Contents of collection 'cache' are updated, but never queried"
            System.out.println("LruCache: " + lruCacheTime);
        }

        double percentFaster = (((double)linkedHashMapTime - lruCacheTime) / linkedHashMapTime) * 100;
        System.out.println(percentFaster + " percent faster");
    }
}
