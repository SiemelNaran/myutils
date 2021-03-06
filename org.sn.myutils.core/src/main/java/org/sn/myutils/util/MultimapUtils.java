package org.sn.myutils.util;

import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.sn.myutils.annotations.NotNull;


/**
 * Helper class to deal with the multimap structure, i.e. something like Map&larr;K, List&larr;V&rarr;&rarr;.
 * This is a alternative to the Guava Multimap classes.
 *
 * @param <K> the key
 * @param <V> the type of each value in the collection for this key
 */
public class MultimapUtils<K, V> {
    private final Map<K, Collection<V>> map;
    private final Supplier<Collection<V>> creator;

    /**
     * Setup Class to perform operations on a multimap.
     * 
     * <p>The JVM should optimize away this class and inline the function called, so creating instances of this class
     * should not be expensive.
     * 
     * @param map the source map
     * @param creator function that creates a new collection. Example usage ArrayList::new
     */
    public MultimapUtils(Map<K, Collection<V>> map, Supplier<Collection<V>> creator) {
        this.map = map;
        this.creator = creator;
    }


    /**
     * Get the collection with the given key.
     * Create an empty collection if one does not exist.
     */
    public @NotNull Collection<V> getOrCreate(K key) {
        return map.computeIfAbsent(key, unused -> creator.get());
    }

    /**
     * Insert a key value pair into the map.
     */
    public void put(K key, V value) {
        Collection<V> collection = getOrCreate(key);
        collection.add(value);
    }

    /**
     * Remove a specific key value from the map.
     * 
     * @return true if something was removed
     */
    public boolean remove(K key, V value) {
        Collection<V> collection = map.get(key);
        if (collection == null) {
            return false;
        }
        boolean removed = collection.remove(value);
        if (collection.isEmpty()) {
            map.remove(key);
        }
        return removed;
    }

    public boolean removeIf(K key, Predicate<? super V> filter) {
        Collection<V> collection = map.get(key);
        if (collection == null) {
            return false;
        }
        boolean removed = collection.removeIf(filter);
        if (collection.isEmpty()) {
            map.remove(key);
        }
        return removed;
    }
}
