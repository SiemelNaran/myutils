package org.sn.myutils.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.concurrent.NotThreadSafe;


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
public class LfuCache<K,V> {
    
    private final int maxSize;
    private Page<K,V> mostFrequentPage;
    private Page<K,V> leastFrequentPage;
    private final Map<K, Page<K,V>> map = new HashMap<>();
    
    public LfuCache(int maxSize) {
        this.maxSize = checkMaxSize(maxSize);
    }
    
    private static int checkMaxSize(int maxSize) {
        if (maxSize < 2) {
            throw new IllegalArgumentException(Integer.toString(maxSize));
        }
        return maxSize;
    }
    
    /**
     * Insert or replace an element in the cache.
     * If inserting, if the cache is full, remove the least frequently used element,
     * but not the element just added.
     */
    public void put(K key, V value) {
        if (map.size() == 0) {
            assert mostFrequentPage == null;
            assert leastFrequentPage == null;
            mostFrequentPage = new Page<K,V>(null, 1, null);
            leastFrequentPage = mostFrequentPage;
            map.put(key, mostFrequentPage);
            mostFrequentPage.lru.put(key, value);
        } else {
            assert mostFrequentPage != null;
            assert leastFrequentPage != null;
            Page<K,V> find = map.get(key);
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
                    Page<K,V> newPage = new Page<K,V>(leastFrequentPage, 1, null);
                    leastFrequentPage.next = newPage;
                    leastFrequentPage = newPage;
                }
                leastFrequentPage.lru.put(key, value);
                map.put(key, leastFrequentPage);
            } else {
                increaseFrequency(find, key, value);
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
    private void unlinkPageIfEmpty(Page<K,V> page) {
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
    public V get(K key) {
        Page<K,V> find = map.get(key);
        if (find == null) {
            return null;
        } else {
            V value = find.lru.get(key);
            increaseFrequency(find, key, value);
            return value;
        }
    }
    
    private void increaseFrequency(Page<K,V> find, K key, V value) {
        if (find.frequency == Integer.MAX_VALUE) {
            normalize();
        }
        if (find.lru.size() == 1 && (find.prev == null || find.prev.frequency >= find.frequency + 2)) {
            find.frequency++;
        } else {
            find.lru.remove(key);
            if (find.prev != null && find.prev.frequency == find.frequency + 1) {
                Page<K,V> prevPage = find.prev;
                prevPage.lru.put(key, value);
                map.put(key, prevPage);
            } else {
                Page<K,V> newPage = new Page<K,V>(find.prev, find.frequency + 1, find);
                newPage.lru.put(key, value);
                map.put(key, newPage);
                if (find == mostFrequentPage) {
                    mostFrequentPage = newPage;
                } else {
                    find.prev.next = newPage;
                }
                find.prev = newPage;
            }
            unlinkPageIfEmpty(find);
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
    public V remove(K key) {
        Page<K,V> find = map.get(key);
        if (find == null) {
            return null;
        } else {
            V value = find.lru.remove(key);
            map.remove(key);
            unlinkPageIfEmpty(find);
            return value;
        }
    }
    
    /**
     * The size of the cache.
     */
    public int size() {
        return map.size();
    }
    
    /**
     * Tell if the cache empty.
     */
    public boolean isEmpty() {
        return size() == 0;
    }
    
    private static class Page<K,V> {
        private Page<K,V> prev;
        private int frequency;
        private final LruCache<K,V> lru = new LruCache<K,V>(Integer.MAX_VALUE);
        private Page<K,V> next;
        
        private Page(Page<K,V> prev, int frequency, Page<K,V> next) {
            this.prev = prev;
            this.frequency = frequency;
            this.next = next;
        }
    }
    
    List<String> getCacheForTesting() {
        List<String> cache = new ArrayList<>(size());
        for (Page<K,V> page = mostFrequentPage; page != null; page = page.next) {
            for (String elem: page.lru.getCacheForTesting()) {
                cache.add(page.frequency + ":" + elem);
            }
        }
        return cache;
    }
    
    List<String> getReverseCacheForTesting() {
        List<String> cache = new ArrayList<>(size());
        for (Page<K,V> page = leastFrequentPage; page != null; page = page.prev) {
            for (String elem: page.lru.getReverseCacheForTesting()) {
                cache.add(page.frequency + ":" + elem);
            }
        }
        return cache;
    }
    
    List<Integer> getSizesForTesting() {
        List<Integer> sizes = new ArrayList<>(size());
        for (Page<K,V> page = leastFrequentPage; page != null; page = page.prev) {
            sizes.add(page.lru.size());
        }
        return sizes;
    }
}
