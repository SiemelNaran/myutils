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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.sn.myutils.testutils.TestUtil;


public class LfuCacheTest {
    @Test
    void testInvalidSize() {
        assertExceptionFromCallable(() -> new LfuCache<>(1), IllegalArgumentException.class);
    }

    @Test
    void testEviction() {
        LfuCache<String, String> cache = new LfuCache<>(3);
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        cache.put("one", "1");
        assertFalse(cache.isEmpty());
        assertEquals(1, cache.size());
        assertEquals(Collections.singletonList("1:one=1"), getListForTesting(cache));
        cache.put("two", "2");
        assertEquals(Arrays.asList("1:two=2", "1:one=1"), getListForTesting(cache));
        cache.put("three", "3");
        assertEquals(Arrays.asList("1:three=3", "1:two=2", "1:one=1"), getListForTesting(cache));
        cache.put("four", "4");
        assertEquals(Arrays.asList("1:four=4", "1:three=3", "1:two=2"), getListForTesting(cache));

        assertTrue(cache.containsKey("three"));
        assertTrue(cache.containsKey("two"));
        assertFalse(cache.containsKey("one"));
        assertTrue(cache.containsKey("four"));
        assertEquals(Arrays.asList("1:four=4", "1:three=3", "1:two=2"), getListForTesting(cache));

        assertTrue(cache.containsValue("3"));
        assertTrue(cache.containsValue("2"));
        assertFalse(cache.containsValue("1"));
        assertTrue(cache.containsValue("4"));
        assertEquals(Arrays.asList("1:four=4", "1:three=3", "1:two=2"), getListForTesting(cache));
    }

    @Test
    @SuppressWarnings("unchecked")
    void testMaxFrequency() throws ClassNotFoundException, NoSuchFieldException, IllegalAccessException {
        var mapField = LfuCache.class.getDeclaredField("map");
        Class<?> pageClass = Class.forName("org.sn.myutils.util.LfuCache$Page");
        var frequencyField = pageClass.getDeclaredField("frequency");
        mapField.setAccessible(true);
        frequencyField.setAccessible(true);

        LfuCache<String, String> cache = new LfuCache<>(3);
        cache.put("one", "1");
        cache.put("two", "2");
        cache.put("three", "3");
        cache.get("three");
        cache.get("two");
        cache.get("one");
        cache.get("one");
        assertEquals(Arrays.asList("3:one=1", "2:two=2", "2:three=3"), getListForTesting(cache));

        // change frequency of one=1 from 3 to INTEGER_MAXVALUE
        var map = (Map<String, Object>) mapField.get(cache);
        var page = map.get("one");
        frequencyField.set(page, Integer.MAX_VALUE);
        assertEquals(Arrays.asList("2147483647:one=1", "2:two=2", "2:three=3"), getListForTesting(cache));

        cache.get("one");
        assertEquals(Arrays.asList("2147483647:one=1", "2:two=2", "2:three=3"), getListForTesting(cache));

        cache.put("four", "4");
        assertEquals(Arrays.asList("2147483647:one=1", "2:two=2", "1:four=4"), getListForTesting(cache));
    }

    @Test
    void testPutAndGet() {
        LfuCache<String, String> cache = new LfuCache<>(3);
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        
        // put one=1 into cache and get it 3 times
        cache.put("one", "1");
        assertEquals(Collections.singletonList("1:one=1"), getListForTesting(cache));
        assertEquals("1", cache.get("one"));
        assertEquals(Collections.singletonList("2:one=1"), getListForTesting(cache));
        assertEquals("1", cache.get("one"));
        assertEquals(Collections.singletonList("3:one=1"), getListForTesting(cache));
        
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
    void testRemoveLeastFrequentThenRemoveAllOneByOne() {
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
        assertEquals(Collections.singletonList("2:one=1"), getListForTesting(cache));
        assertEquals("1", cache.remove("one"));
        assertTrue(cache.isEmpty());
        assertEquals(0, cache.size());
        assertEquals(Collections.emptyList(), getListForTesting(cache));
        assertNull(cache.remove("nine"));
    }
    
    @Test
    void testRemoveMostFrequent() {
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
    void testIncreaseFrequencyOfMiddlePage() {
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

        assertEquals("3", cache.get("three"));
        assertEquals(Arrays.asList("4:three=3", "4:one=1", "2:two=2", "1:four=4"), getListForTesting(cache));

        assertEquals("3", cache.get("three"));
        assertEquals(Arrays.asList("5:three=3", "4:one=1", "2:two=2", "1:four=4"), getListForTesting(cache));
    }
    
    @Test
    void testRemoveMiddlePage() {
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
    void testGetAndPutBothIncreaseFrequencyTheSameWay() {
        LfuCache<String, String> cache = new LfuCache<>(3);
        cache.put("one", "1");
        cache.put("two", "2");
        cache.put("three", "3");
        assertEquals("2", cache.put("two", "22"));
        assertEquals("3", cache.put("three", "33"));
        assertEquals("33", cache.put("three", "333"));
        assertEquals(Arrays.asList("3:three=333", "2:two=22", "1:one=1"), getListForTesting(cache));
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
        assertEquals(list.size(), sizes.stream().mapToInt(val -> val).sum());
        
        return list;
    }

    @ParameterizedTest
    @ValueSource(strings = { "clear", "entrySet.clear" })
    void testClear(String method) {
        LfuCache<String, String> cache = new LfuCache<>(3);
        assertTrue(cache.isEmpty());
        assertNull(cache.put("one", "1"));
        assertNull(cache.put("two", "2"));
        assertNull(cache.put("three", "3"));
        assertEquals(3, cache.size());
        assertEquals(Arrays.asList("1:three=3", "1:two=2", "1:one=1"), getListForTesting(cache));
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
        assertEquals(Arrays.asList("1:three=3", "1:two=2", "1:one=1"), getListForTesting(cache));
        assertFalse(cache.isEmpty());
    }

    @Test
    void testEntrySet() {
        LfuCache<String, String> cache = new LfuCache<>(4);
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
        assertEquals(Arrays.asList("3:three=3", "2:two=2", "1:four=4", "1:one=1"), getListForTesting(cache));

        {
            var iter = cache.entrySet().iterator();
            assertTrue(iter.hasNext());
            List<String> keys = new ArrayList<>();
            keys.add(iter.next().toString());
            keys.add(iter.next().toString());
            keys.add(iter.next().toString());
            keys.add(iter.next().toString());
            assertFalse(iter.hasNext());
            assertThat(keys, Matchers.contains("three=3", "two=2", "four=4", "one=1"));
            assertExceptionFromCallable(iter::next, NoSuchElementException.class);
            assertEquals(Arrays.asList("3:three=3", "2:two=2", "1:four=4", "1:one=1"), getListForTesting(cache));
        }

        {
            var iter = cache.entrySet().iterator();
            assertTrue(iter.hasNext());
            assertException(iter::remove, IllegalStateException.class);
            iter.next();
            iter.remove(); // remove "three"
            assertException(iter::remove, IllegalStateException.class);
            Map.Entry<String, String> entryTwo = iter.next();
            Map.Entry<String, String> entryFour = iter.next();
            assertEquals("4", entryFour.setValue("44"));
            assertEquals("44", entryFour.setValue("444"));
            Map.Entry<String, String> entryOne = iter.next();
            assertFalse(iter.hasNext());
            List<String> keys = new ArrayList<>();
            keys.add(entryTwo.toString());
            keys.add(entryFour.toString());
            keys.add(entryOne.toString());
            assertThat(keys, Matchers.contains("two=2", "four=444", "one=1"));
            assertEquals(Arrays.asList("3:four=444", "2:two=2", "1:one=1"), getListForTesting(cache));
        }

        {
            // verify equals and hashCode

            // this part same as above
            var iter = cache.entrySet().iterator();
            Map.Entry<String, String> entryFour = iter.next();
            Map.Entry<String, String> entryTwo = iter.next();
            Map.Entry<String, String> entryOne = iter.next();
            assertEquals("four=444", entryFour.toString());
            assertEquals("two=2", entryTwo.toString());
            assertEquals("one=1", entryOne.toString());
            assertFalse(iter.hasNext());

            assertEquals("four", entryFour.getKey());
            assertEquals("444", entryFour.getValue());

            assertNotEquals(entryFour.hashCode(), entryTwo.hashCode());
            assertNotEquals(entryFour, entryTwo);
            assertNotEquals(entryFour, null);

            var anotherEntryFour = cache.entrySet().iterator().next();
            assertNotSame(entryFour, anotherEntryFour);
            assertEquals(entryFour.hashCode(), anotherEntryFour.hashCode());
            assertEquals(entryFour, anotherEntryFour);
        }
    }

    @Test
    void testEntrySet2() {
        LfuCache<String, String> cache = new LfuCache<>(4);
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
        assertEquals(Arrays.asList("3:three=3", "2:two=2", "1:four=4", "1:one=1"), getListForTesting(cache));

        var iter = cache.entrySet().iterator();
        assertTrue(iter.hasNext());
        iter.next();
        iter.next();
        Map.Entry<String, String> entryFour = iter.next();
        Map.Entry<String, String> entryOne = iter.next();
        entryFour.setValue("44");
        assertEquals(Arrays.asList("3:three=3", "2:four=44", "2:two=2", "1:one=1"), getListForTesting(cache));
        entryOne.setValue("11");
        assertEquals(Arrays.asList("3:three=3", "2:one=11", "2:four=44", "2:two=2"), getListForTesting(cache));
        entryFour.setValue("444");
        assertEquals(Arrays.asList("3:four=444", "3:three=3", "2:one=11", "2:two=2"), getListForTesting(cache));
        entryFour.setValue("4444");
        assertEquals(Arrays.asList("4:four=4444", "3:three=3", "2:one=11", "2:two=2"), getListForTesting(cache));
        entryFour.setValue("44444");
        assertEquals(Arrays.asList("5:four=44444", "3:three=3", "2:one=11", "2:two=2"), getListForTesting(cache));
    }

    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(strings = {"getMostFrequent", "get", "putCurrentBucket1", "putCurrentBucket2", "putAnotherBucket", "remove"})
    void testIteratorFailFast(String method) {
        LfuCache<String, String> cache = new LfuCache<>(4);
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
        assertEquals(Arrays.asList("3:three=3", "2:two=2", "1:four=4", "1:one=1"), getListForTesting(cache));
        Iterator<Map.Entry<String, String>> iter = cache.entrySet().iterator();
        assertTrue(iter.hasNext());
        switch (method) {
            case "getMostFrequent": cache.get("three"); break;
            case "get": cache.get("two"); break;
            case "putCurrentBucket1": cache.put("two", "22"); break; // internal iterator is pointing to the frequency=3 bucket, so change this bucket
            case "putCurrentBucket2": cache.put("three", "33"); break; // internal iterator is pointing to the frequency=3 bucket, so change this bucket
            case "putAnotherBucket": cache.put("five", "5"); break;
            case "remove": cache.remove("three"); break;
            default: throw new UnsupportedOperationException();
        }
        if (method.equals("putAnotherBucket")) {
            assertTrue(iter.hasNext());
            assertEquals("3", iter.next().getValue());
        } else {
            assertExceptionFromCallable(iter::hasNext, ConcurrentModificationException.class);
            assertExceptionFromCallable(iter::next, ConcurrentModificationException.class);
        }
    }
}
