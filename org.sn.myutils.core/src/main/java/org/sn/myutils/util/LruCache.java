package org.sn.myutils.util;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.NotThreadSafe;


/**
 * An implementation of LRU (least recently used) cache.
 * In practice, prefer to use LinkedHashMap because it is a standard class.
 * 
 * <p>Two new things in this class as compared to LinkedHashMap:
 * - A function removeOldest to remove the oldest entry now.
 *   For example LruCache is used in LfuCache, where items fetched with the same
 *   frequency are stored in a LruCache, and when an item is to be evicted,
 *   the least recently used item in the bucket with the lowest frequency is evicted.
 * - LinkedHashMap's internal map is a HashMap of key to value,
 *   whereas for LruCache it is a HashMap of key to node.
 * 
 * <p>This implementation maintains a linked list of all the key-value pairs,
 * with the most recently used at the head of the list.
 * There is also a map of key to the linked list node.
 */
@NotThreadSafe
public class LruCache<K, V> extends AbstractMap<K, V> {
    
    private final int maxSize;
    private Node<K, V> newestNode;
    private Node<K, V> oldestNode;
    private final Map<K, Node<K, V>> map = new HashMap<>();

    /**
     * Create an LRU cache holding a certain number of elements.
     * 
     * @param maxSize the maximum size
     * @throws IllegalArgumentException is maxSize is < 1
     */
    public LruCache(int maxSize) {
        this.maxSize = checkMaxSize(maxSize);
    }
    
    private static int checkMaxSize(int maxSize) {
        if (maxSize < 1) {
            throw new IllegalArgumentException("maxSize must be greater than or equal to 1: " + maxSize);
        }
        return maxSize;
    }

    /**
     * Insert or replace an element in the cache.
     * If inserting, if the cache is full, remove the least recently used element.
     */
    @Override
    public V put(K key, V value) {
        if (map.size() == 0) {
            assert newestNode == null;
            assert oldestNode == null;
            newestNode = new Node<>(null, key, value, null);
            oldestNode = newestNode;
            map.put(key, newestNode);
            return null;
        } else {
            assert newestNode != null;
            assert oldestNode != null;
            Node<K,V> find = map.get(key);
            if (find == null) {
                newestNode = new Node<>(null, key, value, newestNode);
                newestNode.next.prev = newestNode;
                map.put(key, newestNode);
                if (map.size() > maxSize) {
                    removeOldest();
                }
                return null;
            } else {
                V oldValue = find.value;
                find.value = value;
                moveToFront(find);
                return oldValue;
            }
        }
    }

    /**
     * Get an element from the cache, returning null if the element is not found.
     * If an element is found, this makes it the most recently used element.
     */
    @Override
    public V get(Object key) {
        Node<K,V> find = map.get(key);
        if (find == null) {
            return null;
        } else {
            moveToFront(find);
            return find.value;
        }
    }

    private void moveToFront(Node<K,V> find) {
        if (find.prev != null) {
            find.prev.next = find.next;
            if (find.next != null) {
                find.next.prev = find.prev;
            } else {
                // this is the oldest node
                assert find == oldestNode;
                oldestNode = oldestNode.prev;
            }
            find.next = newestNode;
            find.prev = null;
            newestNode.prev = find;
            newestNode = find;
        }
    }
    
    /**
     * Remove an item from the cache.
     */
    @Override
    public V remove(Object key) {
        Node<K,V> find = map.get(key);
        return internalRemove(find);
    }
    
    private V internalRemove(Node<K,V> find) {
        if (find == null) {
            return null;
        } else {
            moveToFront(find);
            newestNode = newestNode.next;
            if (newestNode != null) {
                newestNode.prev = null;
            } else {
                oldestNode = null;
            }
            map.remove(find.key);
            return find.value;
        }
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
                  .anyMatch(entry -> Objects.equals(value, entry.getValue().value));
    }

    /**
     * Empty out the cache.
     */
    @Override
    public void clear() {
        newestNode = null;
        oldestNode = null;
        map.clear();
    }

    /**
     * Return all entries in the map, in no particular order unlike LinkedHashMap.
     */
    @Override
    public @NotNull Set<Map.Entry<K, V>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public int size() {
                return map.size();
            }

            @Override
            public Iterator<Entry<K, V>> iterator() {
                return new LruCacheIterator();
            }

            @Override
            public void clear() {
                LruCache.this.clear();
            }
        };
    }
    
    private class LruCacheIterator implements Iterator<Entry<K, V>> {
        private Node<K, V> nextNode;
        private Node<K, V> currentNode;

        LruCacheIterator() {
            this.nextNode = LruCache.this.newestNode;
        }

        @Override
        public boolean hasNext() {
            return nextNode != null;
        }

        @Override
        public Entry<K, V> next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            currentNode = nextNode;
            var result = new LruCacheEntry(currentNode);
            nextNode = currentNode.next;
            return result;
        }
        
        @Override
        public void remove() {
            if (currentNode == null) {
                throw new IllegalStateException();
            }
            LruCache.this.internalRemove(currentNode);
            currentNode = null;
        }
    }

    private class LruCacheEntry implements Entry<K, V> {
        private final Node<K, V> node;
        
        private LruCacheEntry(Node<K, V> node) {
            this.node = node;
        }
        
        @Override
        public K getKey() {
            return node.key;
        }

        @Override
        public V getValue() {
            return node.value;
        }

        @Override
        public V setValue(V newValue) {
            V old = getValue();
            node.value = newValue;
            LruCache.this.moveToFront(node);
            return old;
        }
        
        @Override
        public String toString() {
            return getKey() + "=" + getValue();
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public boolean equals(Object thatObject) {
            if (!(thatObject instanceof LruCache.LruCacheEntry)) {
                return false;
            }
            LruCacheEntry that = (LruCacheEntry) thatObject;
            return Objects.equals(this.getKey(), that.getKey()) && Objects.equals(this.getValue(), that.getValue());
        }
        
        @Override
        public int hashCode() {
            return Objects.hash(getKey(), getValue());
        }
    }
    

    /**
     * Remove oldest node.
     * LinkedHashMap does not have this function.
     * 
     * <p>This function is useful if you have an LRU cache of some large or infinite size
     * and want to remove the least recently used element.
     * 
     * @return the key of the element removed
     * @throws NullPointerException if map is empty
     */
    public K removeOldest() {
        Node<K,V> removedNode = oldestNode;
        map.remove(removedNode.key);
        oldestNode = removedNode.prev;
        if (oldestNode != null) {
            oldestNode.next = null;
        } else {
            newestNode = null;
        }
        return removedNode.key;
    }
    
    private static class Node<K,V> {
        private Node<K,V> prev;
        private final K key;
        private V value;
        private Node<K,V> next;
        
        private Node(Node<K,V> prev, K key, V value, Node<K,V> next) {
            this.prev = prev;
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }
    
    List<String> getCacheForTesting() {
        List<String> cache = new ArrayList<>(map.size());
        for (Node<K,V> node = newestNode; node != null; node = node.next) {
            cache.add(node.key.toString() + "=" + node.value.toString());
        }
        return cache;
    }
    
    List<String> getReverseCacheForTesting() {
        List<String> reverseCache = new ArrayList<>(map.size());
        for (Node<K,V> node = oldestNode; node != null; node = node.prev) {
            reverseCache.add(node.key.toString() + "=" + node.value.toString());
        }
        return reverseCache;
    }
}
