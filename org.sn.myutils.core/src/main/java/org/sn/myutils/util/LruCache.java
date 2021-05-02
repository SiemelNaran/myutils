package org.sn.myutils.util;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.NotThreadSafe;


/**
 * An implementation of LRU (least recently used) cache.
 * Items most recently inserted, updated, or accessed are moved to the top of the map, whereas older items are evicted.
 * In practice, prefer to use LinkedHashMap because it is a standard class.
 *
 * <p>On my Mac 2.3 GHz 8-Core Intel Core I9
 * LruCache is between 15% to 2% faster than LinkedHashMap with access order.
 * 
 * <p>One new thing in this class as compared to LinkedHashMap:
 * - A function removeOldest to remove the oldest entry now.
 *   For example LruCache is used in LfuCache, where items fetched with the same
 *   frequency are stored in a LruCache, and when an item is to be evicted,
 *   the least recently used item in the bucket with the lowest frequency is evicted.
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
    private int modCount;

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
            modCount++;
            return null;
        } else {
            assert newestNode != null;
            assert oldestNode != null;
            Node<K, V> find = map.get(key);
            if (find == null) {
                newestNode = new Node<>(null, key, value, newestNode);
                newestNode.next.prev = newestNode;
                map.put(key, newestNode);
                if (map.size() > maxSize) {
                    removeOldest(); // increases modCount
                } else {
                    modCount++;
                }
                return null;
            } else {
                V oldValue = find.value;
                find.value = value;
                moveToFront(find); // increases modCount
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
        Node<K, V> find = map.get(key);
        if (find == null) {
            return null;
        } else {
            moveToFront(find); // increases modCount
            return find.value;
        }
    }

    private void moveToFront(Node<K, V> find) {
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
        modCount++;
    }
    
    /**
     * Remove an item from the cache.
     */
    @Override
    public V remove(Object key) {
        Node<K, V> find = map.get(key);
        return internalRemove(find);
    }
    
    private V internalRemove(Node<K, V> find) {
        if (find == null) {
            modCount++;
            return null;
        } else {
            moveToFront(find); // increases modCount
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
     * Does not move the element to most recently used.
     */
    @Override
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    /**
     * Empty out the cache.
     */
    @Override
    public void clear() {
        newestNode = null;
        oldestNode = null;
        map.clear();
        modCount++;
    }

    /**
     * Return all entries in the map, in no particular order unlike LinkedHashMap.
     */
    @Override
    public @NotNull LruCacheEntrySet entrySet() {
        return new LruCacheEntrySet();
    }

    class LruCacheEntrySet extends AbstractSet<Map.Entry<K, V>> {
        @Override
        public int size() {
            return map.size();
        }

        @Override
        public Iterator<Entry<K, V>> iterator() {
            return newConcurrentModificationManager().createNew();
        }

        ConcurrentModificationManager newConcurrentModificationManager() {
            return new ConcurrentModificationManager();
        }

        @Override
        public void clear() {
            LruCache.this.clear();
        }
    }

    /**
     * This is a package private helper class that serves as the base of the iterator.
     * It is for use for LfuCache, which is basically a list of LruCache's, in order to allow different instances to share the same modification count.
     *
     * <p>The EntrySet returned by LfuCache's iterator holds a LruCacheIterator, which is a pointer to the element in the LruCache.
     * This is needed for setValue to work properly.
     * As LruCacheIterator is a nested class of this class, all these LruCacheIterator (inside the LfuCacheEntrySet) share the same expected modification count.
     */
    final class ConcurrentModificationManager {
        private int expectedModCount = LruCache.this.modCount;

        private void throwConcurrentModificationExceptionIfNecessary() {
            if (LruCache.this.modCount != expectedModCount) {
                throw new ConcurrentModificationException();
            }
        }


        /**
         * Increment the iterators modification count.
         * This happens after a call to remove or setValue.
         *
         * <p>Also called from by LfuCache::setValue to increase the mod count.
         * LfuCache::setEntry operates directly on the LruCache (i.e. calling put, remove),
         * so call this function to keep the expected mod count of the iterator in sync.
         */
        void incrementExpectedModCount() {
            expectedModCount++;
        }

        /**
         * Create a new iterator pointing to the newest node.
         */
        LruCacheIterator createNew() {
            return new LruCacheIterator(LruCache.this.newestNode, null);
        }

        final class LruCacheIterator implements Iterator<Entry<K, V>> {
            private Node<K, V> nextNode;
            private Node<K, V> currentNode;

            LruCacheIterator(Node<K, V> nextNode, Node<K, V> currentNode) {
                this.nextNode = nextNode;
                this.currentNode = currentNode;
            }

            @Override
            public boolean hasNext() {
                throwConcurrentModificationExceptionIfNecessary();
                return nextNode != null;
            }

            @Override
            public LruCacheEntry next() {
                throwConcurrentModificationExceptionIfNecessary();
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
                throwConcurrentModificationExceptionIfNecessary();
                LruCache.this.internalRemove(currentNode);
                incrementExpectedModCount();
                currentNode = null;
            }
        }

        final class LruCacheEntry implements Entry<K, V> {
            private final Node<K, V> node;

            private LruCacheEntry(Node<K, V> node) {
                this.node = node;
            }

            /**
             * Does not move element to top of LruCache.
             */
            @Override
            public K getKey() {
                return node.key;
            }

            /**
             * Does not move element to top of LruCache.
             */
            @Override
            public V getValue() {
                return node.value;
            }

            @Override
            public V setValue(V newValue) {
                throwConcurrentModificationExceptionIfNecessary();
                final V old = node.value;
                node.value = newValue;
                LruCache.this.moveToFront(node);
                incrementExpectedModCount();
                return old;
            }

            @Override
            public String toString() {
                return getKey() + "=" + getValue();
            }

            @SuppressWarnings("unchecked")
            @Override
            public boolean equals(Object thatObject) {
                if (!(thatObject instanceof LruCache.ConcurrentModificationManager.LruCacheEntry)) {
                    return false;
                }
                LruCacheEntry that = (LruCacheEntry) thatObject;
                return Objects.equals(this.getKey(), that.getKey()) && Objects.equals(this.getValue(), that.getValue());
            }

            @Override
            public int hashCode() {
                return Objects.hash(getKey(), getValue());
            }

            ConcurrentModificationManager concurrentModificationManager() {
                return ConcurrentModificationManager.this;
            }
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
        Node<K, V> removedNode = oldestNode;
        map.remove(removedNode.key);
        oldestNode = removedNode.prev;
        if (oldestNode != null) {
            oldestNode.next = null;
        } else {
            newestNode = null;
        }
        modCount++;
        return removedNode.key;
    }
    
    private static class Node<K, V> {
        private Node<K, V> prev;
        private final K key;
        private V value;
        private Node<K, V> next;
        
        private Node(Node<K, V> prev, K key, V value, Node<K, V> next) {
            this.prev = prev;
            this.key = key;
            this.value = value;
            this.next = next;
        }
    }
    
    List<String> getCacheForTesting() {
        List<String> cache = new ArrayList<>(map.size());
        for (Node<K, V> node = newestNode; node != null; node = node.next) {
            cache.add(node.key.toString() + "=" + node.value.toString());
        }
        return cache;
    }
    
    List<String> getReverseCacheForTesting() {
        List<String> reverseCache = new ArrayList<>(map.size());
        for (Node<K, V> node = oldestNode; node != null; node = node.prev) {
            reverseCache.add(node.key.toString() + "=" + node.value.toString());
        }
        return reverseCache;
    }
}
