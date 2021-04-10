package org.sn.myutils.util;

import static org.sn.myutils.util.Iterables.compareIterable;
import static org.sn.myutils.util.Iterables.lengthOfCommonPrefix;
import static org.sn.myutils.util.Iterables.substring;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.NotThreadSafe;
import org.sn.myutils.annotations.Nullable;


/**
 * Trie class that merges all chars into one string for a node if there are no child nodes.
 */
@NotThreadSafe
public class SpaceEfficientTrie<T extends Comparable<T>, U> extends TrieIterationHelper.TrieMap<T, U> {
    private SpaceEfficientTrieNode<T, U> root;
    private int modCount;

    public SpaceEfficientTrie() {
        clear();
    }

    @Override
    public void clear() {
        this.root = new SpaceEfficientTrieNode<>(null);
    }

    @Override
    public @Nullable U put(Iterable<T> codePoints, @NotNull U data) {
        U oldData = root.add(codePoints, data);
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
        return new SpaceEfficientTrieEntryIterator(root);
    }


    /**
     * The class reflecting each node in the trie.
     * It is a nested class so that SpaceEfficientTrie can have additional member variables for the entire trie.
     */
    private static class SpaceEfficientTrieNode<T extends Comparable<T>, U> implements TrieIterationHelper.TrieNode<T, U> {
        private final NavigableMap<Iterable<T>, SpaceEfficientTrieNode<T, U>> children = new TreeMap<>(compareIterable());
        private SpaceEfficientTrieNode<T, U> parent;
        private U data;
        private int size; // sum of size for each child plus 1 if data is not null

        private SpaceEfficientTrieNode(@Nullable U data) {
            this.data = data;
            if (data != null) {
                this.size = 1;
            }
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
                           .map(Function.identity()); // converts Entry<Iterable, SpaceEfficientTrieNode> to Entry<Iterable, TrieNode>
        }

        private void addChild(Iterable<T> fragment, SpaceEfficientTrieNode<T, U> child) {
            children.put(fragment, child);
            child.parent = this;
            rollupSize(this, child.size);
        }

        private void removeChild(Iterable<T> fragment) {
            var child = children.remove(fragment);
            rollupSize(this, -child.size);
        }

        private static <T extends Comparable<T>, U> void rollupSize(SpaceEfficientTrieNode<T, U> trie, int delta) {
            while (trie != null) {
                trie.size += delta;
                trie = trie.parent;
            }
        }

        int size() {
            return size;
        }

        U add(Iterable<T> codePoints, @NotNull U data) {
            var find = doAddOrFind(this, codePoints, Optional.of(data));
            return find != null ? find.oldData : null;
        }

        @Nullable U remove(Iterable<T> codePoints) {
            var find = doAddOrFind(this, codePoints, Optional.empty());
            // note find.oldData is the same as find.entry.getValue().data
            if (find == null || find.oldData == null) {
                return null;
            }
            find.entry.getValue().data = null;
            rollupSize(find.entry.getValue(), -1);
            return find.oldData;
        }

        @Nullable U find(Iterable<T> codePoints) {
            var find = doAddOrFind(this, codePoints, Optional.empty());
            // note find.oldData is the same as find.entry.getValue().data
            return find != null ? find.oldData : null;
        }

        /**
         * Add or look for a word.
         * Returns the old data associated with this word, or null if the word is new.
         * Thus this function is used for both add and lookup.
         *
         * @param <T>             the type of character
         * @param <U>             the type of the data
         * @param trie            is the root node in the initial call to this function
         * @param newWord         the word to add or look for
         * @param newDataSupplier if present it means we are adding a word, and if absent it means we are looking up a word
         * @return the old data associated with this node
         */
        private static <T extends Comparable<T>, U> FindInfo<T, U>
        doAddOrFind(SpaceEfficientTrieNode<T, U> trie,
                    Iterable<T> newWord,
                    Optional<U> newDataSupplier) {
            Map.Entry<Iterable<T>, SpaceEfficientTrieNode<T, U>> lessThanWord = trie.children.floorEntry(newWord);
            Map.Entry<Iterable<T>, SpaceEfficientTrieNode<T, U>> greaterThanWord = trie.children.ceilingEntry(newWord);
            if (lessThanWord == null && greaterThanWord == null) {
                newDataSupplier.ifPresent(newData -> {
                    var newTrie = new SpaceEfficientTrieNode<T, U>(newData);
                    trie.addChild(newWord, newTrie);
                });
                return null;
            } else if (lessThanWord == null) {
                return attach(trie, greaterThanWord, newWord, newDataSupplier);
            } else if (greaterThanWord == null) {
                return attach(trie, lessThanWord, newWord, newDataSupplier);
            } else {
                int numCommonChars1 = lengthOfCommonPrefix(greaterThanWord.getKey(), newWord);
                int numCommonChars2 = lengthOfCommonPrefix(lessThanWord.getKey(), newWord);
                if (numCommonChars1 >= numCommonChars2) {
                    return attach(trie, greaterThanWord, newWord, newDataSupplier);
                } else {
                    return attach(trie, lessThanWord, newWord, newDataSupplier);
                }
            }
        }

        private static <T extends Comparable<T>, U> FindInfo<T, U>
        attach(SpaceEfficientTrieNode<T, U> trie,
               Map.Entry<Iterable<T>, SpaceEfficientTrieNode<T, U>> existingEntry,
               Iterable<T> newWord,
               Optional<U> newDataSupplier) {
            int numCommonChars = lengthOfCommonPrefix(existingEntry.getKey(), newWord);
            List<T> commonWord = substring(newWord, 0, numCommonChars);
            if (commonWord.isEmpty()) {
                // for example the trie has "apple" and we are adding a completely new word "banana"
                // so just add it
                newDataSupplier.ifPresent(newData -> {
                    var newTrie = new SpaceEfficientTrieNode<T, U>(newData);
                    trie.addChild(newWord, newTrie);
                });
                return null;
            } else {
                List<T> restOfExistingWord = substring(existingEntry.getKey(), numCommonChars);
                List<T> restOfNewWord = substring(newWord, numCommonChars);
                if (!restOfExistingWord.isEmpty()) {
                    // for example the trie has "bottom" and we are adding "bottle"
                    // so remove "bottom"
                    // create "bott"
                    // and under this create "om" with the data that was associated to "bottom", and "le"
                    if (newDataSupplier.isPresent()) {
                        var commonTrie = new SpaceEfficientTrieNode<T, U>(null);
                        trie.removeChild(existingEntry.getKey());
                        trie.addChild(commonWord, commonTrie);
                        commonTrie.addChild(restOfExistingWord, existingEntry.getValue());
                        var newTrie = new SpaceEfficientTrieNode<T, U>(newDataSupplier.get());
                        commonTrie.addChild(restOfNewWord, newTrie);
                    }
                    return null;
                } else {
                    if (!restOfNewWord.isEmpty()) {
                        // for example the trie has "bott" in it
                        // and under this is "le" and "om"
                        // and under "le" is "neck"
                        // and we are adding "bottlenecks"
                        // when comparing "bottlenecks" to "bott" we match first 4 chars only,
                        //     and restOfExistingWord is "" and restOfNewWord is "lenecks"
                        // so call add recursively on the trie that is "le"
                        return doAddOrFind(existingEntry.getValue(), restOfNewWord, newDataSupplier);
                    } else {
                        // at this point we are replacing (or finding) a word in the trie
                        // for example trie has "bottle" and the new word is bottle
                        U oldData = existingEntry.getValue().data;
                        newDataSupplier.ifPresent(newData -> {
                            existingEntry.getValue().data = newData;
                            if (oldData == null) {
                                rollupSize(existingEntry.getValue(), 1);
                            }
                        });
                        return new FindInfo<>(existingEntry, oldData);
                    }
                }
            }
        }

        private static class FindInfo<T extends Comparable<T>, U> {
            private final Map.Entry<Iterable<T>, SpaceEfficientTrieNode<T, U>> entry;
            private final U oldData;

            private FindInfo(Map.Entry<Iterable<T>, SpaceEfficientTrieNode<T, U>> entry, U oldData) {
                this.entry = entry;
                this.oldData = oldData;
            }
        }
    }

    private class SpaceEfficientTrieEntryIterator extends TrieIterationHelper.TrieEntryIterator<T, U> {
        SpaceEfficientTrieEntryIterator(TrieIterationHelper.TrieNode<T, U> root) {
            super(root);
        }

        @Override
        int getTrieModificationCount() {
            return SpaceEfficientTrie.this.modCount;
        }

        @Override
        void doRemove(Iterable<T> word) {
            SpaceEfficientTrie.this.remove(word);
        }
    }
}
