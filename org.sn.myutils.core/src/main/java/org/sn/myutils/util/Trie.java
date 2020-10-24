package org.sn.myutils.util;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for the Trie class.
 *
 * @param <T> the type of character, such as Integer (for Unicode characters), Character, etc.
 * @param <U> the type of data associated with each word in the trie. The value cannot be null.
 */
public interface Trie<T extends Comparable<T>, U> {
    /**
     * Add word with associated data to trie.
     *
     * @param codePoints the word to add
     * @param data the data associated with the word
     * @return the old data associated with the word, or null if there was no word
     * @throws NullPointerException if data is null
     */
    @Nullable U add(Iterable<T> codePoints, @Nonnull U data);

    /**
     * Remove a word from the trie.
     *
     * @param codePoints the word to remove
     * @return the data associated with the word if found, or null if not found
     */
    @Nullable U remove(Iterable<T> codePoints);

    /**
     * Find word in trie.
     * As data is not null when you insert a word, a return value of null means the word was not found.
     *
     * @param codePoints the word to find
     * @return the data associated with the word if found, or null if not found
     */
    @Nullable U find(Iterable<T> codePoints);

    /**
     * Return the number of words in the trie.
     */
    int size();
}
