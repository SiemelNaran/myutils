package org.sn.myutils.util;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Trie class that has one node for each character.
 */
@NotThreadSafe
public class SimpleTrie<T extends Comparable<T>, U> implements Trie<T, U> {
    private final TrieNode<T, U> root;

    public SimpleTrie() {
        this.root = new TrieNode<>(null);
    }

    @Override
    public @Nullable U add(Iterable<T> codePoints, @Nonnull U data) {
        U oldData = root.add(codePoints, data);
        return oldData;
    }

    @Override
    public @Nullable U remove(Iterable<T> codePoints) {
        U oldData = root.remove(codePoints);
        return oldData;
    }

    @Override
    public @Nullable U find(Iterable<T> codePoints) {
        return root.find(codePoints);
    }

    @Override
    public int size() {
        return root.size();
    }

    /**
     * Find a string using the rewindable iterator.
     * This function is useful during parsing, when you are in the process of reading a string and don't have the full string or Iterable.
     *
     * <p>This function finds the longest word matching the input.
     * The function reads the first character and checks if it matches the first character of any word in the trie.
     * If yes and we are not at end of stream, it then proceeds to the next character.
     * If no, then the trie node found thus far is returned, and we rewind the iterator by one char to put the non-matching char back into the stream.
     */
    public @Nullable U findLongest(RewindableIterator<T> codePointsIter) {
        return root.findLongest(codePointsIter);
    }



    private static class TrieNode<T extends Comparable<T>, U> {
        private final TrieNode<T, U> parent;
        private final Map<T, TrieNode<T, U>> children = new HashMap<>();
        private U data;
        private int size;

        private TrieNode(TrieNode<T, U> parent) {
            this.parent = parent;
        }

        U add(Iterable<T> codePoints, @Nonnull U data) {
            return doAdd(this, codePoints, Objects.requireNonNull(data));
        }

        private static @Nullable <T extends Comparable<T>, U> U doAdd(@Nonnull TrieNode<T, U> trie, Iterable<T> codePoints, U data) {
            for (T codePoint : codePoints) {
                var parentTrie = trie;
                trie = parentTrie.children.computeIfAbsent(codePoint, ignored -> new TrieNode<>(parentTrie));
            }
            U oldData = trie.data;
            trie.data = data;
            if (oldData == null) {
                rollupSize(trie, 1);
            }
            return oldData;
        }

        @Nullable U remove(Iterable<T> codePoints) {
            var trie = doFind(this, codePoints);
            if (trie == null) {
                return null;
            }
            var oldData = trie.data;
            trie.data = null;
            if (oldData != null) {
                rollupSize(trie, -1);
            }
            return oldData;
        }

        @Nullable U find(Iterable<T> codePoints) {
            var trie = doFind(this, codePoints);
            return trie != null ? trie.data : null;
        }

        @Nullable U findLongest(RewindableIterator<T> codePointsIter) {
            var trie = doFindLongest(this, codePointsIter);
            return trie != null ? trie.data : null;
        }

        private static @Nullable
        <T extends Comparable<T>, U> TrieNode<T, U> doFind(@Nonnull TrieNode<T, U> trie, Iterable<T> codePoints) {
            for (T codePoint : codePoints) {
                trie = trie.children.get(codePoint);
                if (trie == null) {
                    return null;
                }
            }
            return trie;
        }

        private static @Nullable
        <T extends Comparable<T>, U> TrieNode<T, U> doFindLongest(@Nonnull TrieNode<T, U> trie, RewindableIterator<T> codePointsIter) {
            TrieNode<T, U> partialTrie = null;
            while (codePointsIter.hasNext()) {
                T codePoint = codePointsIter.next();
                var subTrie = trie.children.get(codePoint);
                if (subTrie == null) {
                    codePointsIter.rewind();
                    return partialTrie; // returns null if first char does not match
                }
                trie = subTrie;
                partialTrie = subTrie;
            }
            return trie;
        }

        private static <T extends Comparable<T>, U> void rollupSize(TrieNode<T, U> trie, int delta) {
            while (trie != null) {
                trie.size += delta;
                trie = trie.parent;
            }
        }

        int size() {
            return size;
        }
    }
}
