package org.sn.myutils.util;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.NotThreadSafe;


/**
 * An implementation of LFU (least frequently used) cache.
 * 
 * <p>Inserting a new element removes the least frequently used element,
 * even if that element has been accessed 100's of times.
 * 
 * <p>If two elements have been used at the same frequency, the oldest is removed.
 * 
 * <p>In the code below we implement this as a linked list of LRU caches.
 * This makes lookup O(1) because we have to lookup which LRU cache has the node,
 * then in that LRU cache lookup the node to get its value.
 * But it's a slow O(1) as there are 2 lookups.
 * 
 * <p>It is also possible to simply have a linked list with the most frequent nodes
 * at the head of the list, similar to the implementation of LRUCache but with the
 * Node class having an extra member variable 'frequency'.
 * 
 * <p>Fetching an item could then be O(N) because we have to scan the nodes leftwards
 * towards the head of the list to find the node with the next higher frequency.
 * For example if map is A,B,C,D,E and A has frequency 3 and the others have frequency 1,
 * and we lookup E, we quickly find E in the map.
 * But we must traverse the left pointers of E to find A,
 * the place after which to insert D as its frequency is less than A but greater than B, C, D.
 */
@NotThreadSafe
public class LfuCache<K, V> extends AbstractMap<K, V> {
    
    private final int maxSize;
    private Page<K, V> mostFrequentPage;
    private Page<K, V> leastFrequentPage;
    private final Map<K, Page<K, V>> map = new HashMap<>();
    
    public LfuCache(int maxSize) {
        this.maxSize = checkMaxSize(maxSize);
    }
    
    private static int checkMaxSize(int maxSize) {
        if (maxSize < 2) {
            throw new IllegalArgumentException("maxSize must be greater than or equal to 2: " + maxSize);
        }
        return maxSize;
    }
    
    /**
     * Insert or replace an element in the cache.
     * If inserting, if the cache is full, remove the least frequently used element,
     * but not the element just added.
     */
    @Override
    public V put(K key, V value) {
        if (map.size() == 0) {
            assert mostFrequentPage == null;
            assert leastFrequentPage == null;
            mostFrequentPage = new Page<>(null, 1, null);
            leastFrequentPage = mostFrequentPage;
            map.put(key, mostFrequentPage);
            mostFrequentPage.lru.put(key, value);
            return null;
        } else {
            assert mostFrequentPage != null;
            assert leastFrequentPage != null;
            Page<K, V> find = map.get(key);
            if (find == null) {
                if (map.size() == maxSize) {
                    K evictedKey = leastFrequentPage.lru.removeOldest();
                    map.remove(evictedKey);
                    unlinkPageIfEmpty(leastFrequentPage);
                    // since maxSize is >=2, we are sure that leastFrequentPage is not null 
                    assert mostFrequentPage != null;
                    assert leastFrequentPage != null;
                }
                if (leastFrequentPage.frequency > 1) {
                    // create a new leastFrequentPage
                    Page<K, V> newPage = new Page<>(leastFrequentPage, 1, null);
                    leastFrequentPage.next = newPage;
                    leastFrequentPage = newPage;
                }
                leastFrequentPage.lru.put(key, value);
                map.put(key, leastFrequentPage);
                return null;
            } else {
                V oldVal = find.lru.get(key);
                increaseFrequency(find, key, value);
                return oldVal;
            }
        }
    }
    
    /**
     * Unlink page from its previous and next if it is empty.
     * After this function, page will still be pointing to its former previous and next,
     * but it is assumed that page is a local variable that will go out of scope.
     * 
     * <p>If this page is the most frequent or least frequent page,
     * those member variables will be updated.
     */
    private void unlinkPageIfEmpty(Page<K, V> page) {
        if (page.lru.isEmpty()) {
            if (page.prev != null) {
                page.prev.next = page.next;
            }
            if (page.next != null) {
                page.next.prev = page.prev;
            }
            if (mostFrequentPage == page) {
                mostFrequentPage = page.next;
            }
            if (leastFrequentPage == page) {
                leastFrequentPage = page.prev;
            }
        }
    }
    
    /**
     * Get an element from the cache, returning null if the element is not found.
     * If an element is found, this increases its frequency of use.
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get(Object key) {
        Page<K, V> find = map.get(key);
        if (find == null) {
            return null;
        } else {
            V value = find.lru.get(key);
            increaseFrequency(find, (K) key, value);
            return value;
        }
    }

    /**
     * Increase the frequency of a key that is already in the cache.
     * This means moving it to a new page,
     * or increasing the frequency property of the page if it is the only key in that page and it is possible to do so.
     *
     * @param find the page holding the key-newValue pair
     * @param key the key
     * @param newValue the new newValue for the key
     * @return the previous value and new page
     */
    @SuppressWarnings("checkstyle:VariableDeclarationUsageDistance")
    private IncreaseFrequencyResult<K, V> increaseFrequency(Page<K, V> find, K key, V newValue) {
        if (find.frequency == Integer.MAX_VALUE) {
            normalize();
        }
        final IncreaseFrequencyResult<K, V> result;
        if (find.lru.size() == 1 && (find.prev == null || find.prev.frequency >= find.frequency + 2)) {
            var oldValue = find.lru.put(key, newValue);
            find.frequency++;
            result = new IncreaseFrequencyResult<>(oldValue, find);
        } else {
            find.lru.remove(key);
            if (find.prev != null && find.prev.frequency == find.frequency + 1) {
                Page<K, V> prevPage = find.prev;
                var oldValue = prevPage.lru.put(key, newValue);
                map.put(key, prevPage);
                result = new IncreaseFrequencyResult<>(oldValue, prevPage);
            } else {
                Page<K, V> newPage = new Page<>(find.prev, find.frequency + 1, find);
                var oldValue = newPage.lru.put(key, newValue); // checkstyle:VariableDeclarationUsageDistance
                map.put(key, newPage);
                if (find == mostFrequentPage) {
                    mostFrequentPage = newPage;
                } else {
                    find.prev.next = newPage;
                }
                find.prev = newPage;
                result = new IncreaseFrequencyResult<>(oldValue, newPage);
            }
            unlinkPageIfEmpty(find);
        }
        return result;
    }

    private static class IncreaseFrequencyResult<K, V> {
        private final V oldValue;
        private final Page<K, V> newPage;

        private IncreaseFrequencyResult(V oldValue, Page<K, V> newPage) {
            this.oldValue = oldValue;
            this.newPage = newPage;
        }
    }
    
    /**
     * What to do when one of the pages has frequency equal to the maximum value.
     */
    private void normalize() {
        throw new IllegalStateException();
    }
    
    /**
     * Remove an item from the cache.
     */
    @Override
    public V remove(Object key) {
        Page<K, V> find = map.get(key);
        if (find == null) {
            return null;
        } else {
            return internalRemove(find, key);
        }
    }

    private V internalRemove(Page<K, V> find, Object key) {
        V value = find.lru.remove(key);
        map.remove(key);
        unlinkPageIfEmpty(find);
        return value;
    }

    /**
     * Tells if the given key is in the cache.
     * Does not move the element to most recently used, unlike LinkedHashMap.
     */
    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    /**
     * Tells if the given value is in the cache.
     * Does not move the element to most recently used, just like LinkedHashMap.
     */
    @Override
    public boolean containsValue(Object value) {
        return map.entrySet()
                  .stream()
                  .anyMatch(pageEntry -> pageEntry.getValue().lru.containsValue(value));
    }

    /**
     * Empty out the cache.
     */
    @Override
    public void clear() {
        mostFrequentPage = null;
        leastFrequentPage = null;
        map.clear();
    }

    /**
     * Return all entries in the map, in no particular order unlike LinkedHashMap.
     */
    @Override
    public @NotNull Set<Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public int size() {
                return map.size();
            }

            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new LfuCacheIterator();
            }

            @Override
            public void clear() {
                LfuCache.this.clear();
            }
        };
    }

    private class LfuCacheIterator implements Iterator<Entry<K, V>> {
        private Iterator<Entry<K, V>> pageIter;
        private Page<K, V> nextPage;
        private Entry<K, V> lastEntry;

        LfuCacheIterator() {
            this.nextPage = LfuCache.this.mostFrequentPage;
            if (this.nextPage != null) {
                advancePage();
            }
        }

        private void advancePage() {
            pageIter = nextPage.lru.entrySet().iterator(); // each page has at least one entry
            nextPage = nextPage.next;
        }

        @Override
        public boolean hasNext() {
            return pageIter.hasNext() || nextPage != null;
        }

        @Override
        public Entry<K, V> next() {
            if (pageIter.hasNext()) {
                var page = nextPage != null ? nextPage.prev : LfuCache.this.leastFrequentPage;
                lastEntry = pageIter.next();
                return new LfuCacheEntry(page, lastEntry);
            }
            if (nextPage !=  null) {
                var page = nextPage;
                advancePage();
                lastEntry = pageIter.next();
                return new LfuCacheEntry(page, lastEntry);
            }
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            if (lastEntry == null) {
                throw new IllegalStateException();
            }
            var page = nextPage != null ? nextPage.prev : LfuCache.this.leastFrequentPage;
            LfuCache.this.internalRemove(page, lastEntry.getKey());
            lastEntry = null;
        }
    }

    private class LfuCacheEntry implements Entry<K, V> {
        private Page<K, V> page;
        private Entry<K, V> inner;

        private LfuCacheEntry(Page<K, V> page, Entry<K, V> inner) {
            this.page = page;
            this.inner = inner;
        }

        @Override
        public K getKey() {
            return inner.getKey();
        }

        @Override
        public V getValue() {
            return inner.getValue();
        }

        @Override
        public V setValue(V newValue) {
            var pojo = LfuCache.this.increaseFrequency(page, inner.getKey(), newValue);
            this.page = pojo.newPage;
            this.inner = pojo.newPage.lru.entrySet().iterator().next(); // won't throw as each page has at least one element
            return pojo.oldValue;
        }

        @Override
        public String toString() {
            return inner.toString();
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof LfuCache.LfuCacheEntry)) {
                return false;
            }
            LfuCacheEntry that = (LfuCacheEntry) thatObject;
            return this.inner.equals(that.inner);
        }

        @Override
        public int hashCode() {
            return inner.hashCode();
        }
    }


    private static class Page<K, V> {
        private Page<K, V> prev;
        private int frequency;
        private final LruCache<K, V> lru = new LruCache<>(Integer.MAX_VALUE);
        private Page<K, V> next;
        
        private Page(Page<K, V> prev, int frequency, Page<K, V> next) {
            this.prev = prev;
            this.frequency = frequency;
            this.next = next;
        }
    }
    
    List<String> getCacheForTesting() {
        List<String> cache = new ArrayList<>(size());
        for (Page<K, V> page = mostFrequentPage; page != null; page = page.next) {
            for (String elem: page.lru.getCacheForTesting()) {
                cache.add(page.frequency + ":" + elem);
            }
        }
        return cache;
    }
    
    List<String> getReverseCacheForTesting() {
        List<String> cache = new ArrayList<>(size());
        for (Page<K, V> page = leastFrequentPage; page != null; page = page.prev) {
            for (String elem: page.lru.getReverseCacheForTesting()) {
                cache.add(page.frequency + ":" + elem);
            }
        }
        return cache;
    }
    
    List<Integer> getSizesForTesting() {
        List<Integer> sizes = new ArrayList<>(size());
        for (Page<K, V> page = leastFrequentPage; page != null; page = page.prev) {
            sizes.add(page.lru.size());
        }
        return sizes;
    }
}
