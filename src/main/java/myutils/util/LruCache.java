package myutils.util;

import java.util.*;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * An implementation of LRU (least recently used) cache.
 * In practice, prefer to use LinkedHashMap because it is a standard class
 * and according to one test, this class is almost the same speed as LinkedHashMap.
 * 
 * <p>This implementation maintains a linked list of all the key-value pairs,
 * with the most recently used at the head of the list.
 * There is also a map of key to the linked list node.
 */
@NotThreadSafe
public class LruCache<K,V> implements Map<K,V> {
    
    private final int maxSize;
    private Node<K,V> newestNode;
    private Node<K,V> oldestNode;
    private Map<K, Node<K,V>> map = new HashMap<>();
    
    public LruCache(int maxSize) {
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
     * If inserting, if the cache is full, remove the least recently used element.
     */
    @Override
    public V put(K key, V value) {
        if (map.size() == 0) {
            assert newestNode == null;
            assert oldestNode == null;
            newestNode = new Node<K,V>(null, key, value, null);
            oldestNode = newestNode;
            map.put(key, newestNode);
            return null;
        } else {
            assert newestNode != null;
            assert oldestNode != null;
            Node<K,V> find = map.get(key);
            if (find == null) {
                newestNode = new Node<K,V>(null, key, value, newestNode);
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
     * Insert many items into the cache.
     */
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        for (Map.Entry<? extends K, ? extends V> entry: map.entrySet()) {
            put(entry.getKey(), entry.getValue());
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
            map.remove(key);
            return find.value;
        }
    }

    /**
     * The size of the cache.
     */
    @Override
    public int size() {
        return map.size();
    }

    /**
     * Tell if the cache empty.
     */
    @Override
    public boolean isEmpty() {
        return size() == 0;
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
        return map.containsValue(value);
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
     * Return all the keys in the cache, in no particular order unlike LinkedHashMap.
     */
    @Override
    public @Nonnull Set<K> keySet() {
        return map.keySet();
    }

    /**
     * Return all the values in the map, in no particular order unlike LinkedHashMap.
     */
    @Override
    public @Nonnull Collection<V> values() {
        return map.values().stream().map(node -> node.value).collect(Collectors.toList());
    }

    /**
     * Return all entries in the map, in no particular order unlike LinkedHashMap.
     */
    @Override
    public @Nonnull Set<Map.Entry<K, V>> entrySet() {
        return map.entrySet()
                  .stream()
                  .map(entry -> new AbstractMap.SimpleEntry<K, V>(entry.getKey(), entry.getValue().value))
                  .collect(Collectors.toSet());
    }

    /**
     * Return true if the two caches are the same instance.
     */
    @Override
    public boolean equals(Object that) {
        return this == that;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        return map.hashCode();
    }

    /**
     * Remove oldest node.
     * 
     * @throws NullPointerException if map is empty
     */
    K removeOldest() {
        Node<K,V> removedNode = oldestNode;
        map.remove(removedNode.key);
        oldestNode = removedNode.prev;
        if (oldestNode != null) {
            oldestNode.next = null;
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
