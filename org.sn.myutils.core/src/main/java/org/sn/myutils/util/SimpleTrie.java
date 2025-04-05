package org.sn.myutils.util;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.NotThreadSafe;
import org.sn.myutils.annotations.Nullable;


/**
 * Trie class that has one node for each character.
 */
@NotThreadSafe
public class SimpleTrie<T extends Comparable<T>, U> extends TrieIterationHelper.TrieMap<T, U> {
    private SimpleTrieNode<T, U> root;
    private int modCount;

    public SimpleTrie() {
        this.root = new SimpleTrieNode<>(null);
    }

    @Override
    public void clear() {
        this.root = new SimpleTrieNode<>(null);
    }

    @Override
    public @Nullable U put(Iterable<T> codePoints, @NotNull U data) {
        U oldData = root.add(codePoints, Objects.requireNonNull(data));
        modCount++;
        return oldData;
    }

    @Override
    public @Nullable U remove(Iterable<T> codePoints) {
        U oldData = root.remove(codePoints);
        if (oldData != null) {
            modCount++;
        }
        return oldData;
    }

    @Override
    public @Nullable U get(Iterable<T> codePoints) {
        return root.find(codePoints);
    }

    @Override
    public int size() {
        return root.size();
    }

    @Override
    public Iterator<TrieEntry<T, U>> trieIterator() {
        return new SimpleTrieEntryIterator(root);
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
    public @Nullable U getLongest(RewindableIterator<T> codePointsIter) {
        return root.findLongest(codePointsIter);
    }


    /**
     * The class reflecting each node in the trie.
     * It is a nested class so that SimpleTrie can have additional member variables for the entire trie.
     */
    private static class SimpleTrieNode<T extends Comparable<T>, U> implements TrieIterationHelper.TrieNode<T, U> {
        private final SimpleTrieNode<T, U> parent;
        private final Map<T, SimpleTrieNode<T, U>> children = new HashMap<>();
        private U data;
        private int size;

        private SimpleTrieNode(SimpleTrieNode<T, U> parent) {
            this.parent = parent;
        }

        @Override
        public U getData() {
            return data;
        }

        @Override
        public U setData(@NotNull U newData) {
            U old = data;
            data = Objects.requireNonNull(newData);
            return old;
        }

        @Override
        public Stream<Map.Entry<Iterable<T>, ? extends TrieIterationHelper.TrieNode<T, U>>> childrenIterator() {
            return children.entrySet()
                           .stream()
                           .map(entry -> new AbstractMap.SimpleImmutableEntry<Iterable<T>, TrieIterationHelper.TrieNode<T, U>>(List.of(entry.getKey()),
                                                                                                                               entry.getValue()));
        }

        U add(Iterable<T> codePoints, @NotNull U data) {
            return doAdd(this, codePoints, Objects.requireNonNull(data));
        }

        private static @Nullable <T extends Comparable<T>, U> U doAdd(@NotNull SimpleTrieNode<T, U> trie, Iterable<T> codePoints, U data) {
            for (T codePoint : codePoints) {
                var parentTrie = trie;
                trie = parentTrie.children.computeIfAbsent(codePoint, ignored -> new SimpleTrieNode<>(parentTrie));
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
        <T extends Comparable<T>, U> SimpleTrieNode<T, U> doFind(@NotNull SimpleTrieNode<T, U> trie, Iterable<T> codePoints) {
            for (T codePoint : codePoints) {
                trie = trie.children.get(codePoint);
                if (trie == null) {
                    return null;
                }
            }
            return trie;
        }

        private static @Nullable
        <T extends Comparable<T>, U> SimpleTrieNode<T, U> doFindLongest(@NotNull SimpleTrieNode<T, U> trie, RewindableIterator<T> codePointsIter) {
            SimpleTrieNode<T, U> partialTrie = null;
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

        private static <T extends Comparable<T>, U> void rollupSize(SimpleTrieNode<T, U> trie, int delta) {
            while (trie != null) {
                trie.size += delta;
                trie = trie.parent;
            }
        }

        int size() {
            return size;
        }
    }

    private class SimpleTrieEntryIterator extends TrieIterationHelper.TrieEntryIterator<T, U> {
        SimpleTrieEntryIterator(TrieIterationHelper.TrieNode<T, U> root) {
            super(root);
        }

        @Override
        int getTrieModificationCount() {
            return SimpleTrie.this.modCount;
        }

        @Override
        void doRemove(Iterable<T> word) {
            SimpleTrie.this.remove(word);
        }
    }
}
