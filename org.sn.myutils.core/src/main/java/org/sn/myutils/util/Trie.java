package org.sn.myutils.util;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Interface for the Trie class.
 *
 * @param <T> the type of character, such as Integer (for Unicode characters), Character, etc.
 * @param <U> the type of data associated with each word in the trie. The value cannot be null.
 */
public interface Trie<T extends Comparable<T>, U> extends Map<Iterable<T>, U> {
    /**
     * Add word with associated data to trie.
     *
     * @param codePoints the word to add
     * @param data       the data associated with the word
     * @return the old data associated with the word, or null if there was no word
     * @throws NullPointerException if data is null
     */
    @Override
    @Nullable U put(Iterable<T> codePoints, @Nonnull U data);

    /**
     * Remove a word from the trie.
     *
     * @param codePoints the word to remove
     * @return the data associated with the word if found, or null if not found
     */
    @Nullable U remove(Iterable<T> codePoints);

    /**
     * Find word in trie.
     *
     * @param codePoints the word to find
     * @return the data associated with the word if found, or null if not found
     */
    @Nullable U get(Iterable<T> codePoints);

    /**
     * Return the number of words in the trie.
     */
    @Override
    int size();

    /**
     * Return an iterator going over all the words in the map.
     * This function is similar to entrySet().iterator() except that entry.getKey() is an Iterable whereas entry.getWord() is a List.
     */
    Iterator<TrieEntry<T, U>> trieIterator();

    interface TrieEntry<T extends Comparable<T>, U> extends Map.Entry<Iterable<T>, U> {
        /**
         * Get the full word of this trie node.
         */
        List<T> getWord();
    }
}
