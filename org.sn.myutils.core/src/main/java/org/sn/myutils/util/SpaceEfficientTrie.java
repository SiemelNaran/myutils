package org.sn.myutils.util;

import static org.sn.myutils.util.Iterables.compareIterable;
import static org.sn.myutils.util.Iterables.lengthOfCommonPrefix;
import static org.sn.myutils.util.Iterables.substring;

import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Optional;
import java.util.TreeMap;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Trie class that merges all chars into one string for a node if there are no child nodes.
 */
@NotThreadSafe
public class SpaceEfficientTrie<T extends Comparable<T>, U> implements Trie<T, U> {
    private final NavigableMap<Iterable<T>, SpaceEfficientTrie<T, U>> children = new TreeMap<>(compareIterable());
    private SpaceEfficientTrie<T, U> parent;
    private U data;
    private int size; // sum of size for each child plus 1 if data is not null

    public static <T extends Comparable<T>, U> SpaceEfficientTrie<T, U> create() {
        return new SpaceEfficientTrie<>(null);
    }

    private SpaceEfficientTrie(@Nullable U data) {
        this.data = data;
        if (data != null) {
            this.size = 1;
        }
    }

    private void addChild(Iterable<T> fragment, SpaceEfficientTrie<T, U> child) {
        children.put(fragment, child);
        child.parent = this;
        rollupSize(this, child.size);
    }

    private void removeChild(Iterable<T> fragment) {
        var child = children.remove(fragment);
        rollupSize(this, -child.size);
    }

    private static <T extends Comparable<T>, U> void rollupSize(SpaceEfficientTrie<T, U> trie, int delta) {
        while (trie != null) {
            trie.size += delta;
            trie = trie.parent;
        }
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public U add(Iterable<T> codePoints, @Nonnull U data) {
        var find =  doAddOrFind(this, codePoints, Optional.of(data));
        return find != null ? find.oldData : null;
    }

    @Override
    public @Nullable U remove(Iterable<T> codePoints) {
        var find = doAddOrFind(this, codePoints, Optional.empty());
        // note find.oldData is the same as find.entry.getValue().data
        if (find == null || find.oldData == null) {
            return null;
        }
        find.entry.getValue().data = null;
        rollupSize(find.entry.getValue(), -1);
        return find.oldData;
    }

    @Override
    public @Nullable U find(Iterable<T> codePoints) {
        var find = doAddOrFind(this, codePoints, Optional.empty());
        // note find.oldData is the same as find.entry.getValue().data
        return find != null ? find.oldData : null;
    }

    /**
     * Add or look for a word.
     * Returns the old data associated with this word, or null if the word is new.
     * Thus this function is used for both add and lookup.
     *
     * @param <T> the type of character
     * @param <U> the type of the data
     * @param trie is the root node in the initial call to this function
     * @param newWord the word to add or look for
     * @param newDataSupplier if present it means we are adding a word, and if absent it means we are looking up a word
     * @return the old data associated with this node
     */
    private static <T extends Comparable<T>, U> FindInfo<T, U>
    doAddOrFind(SpaceEfficientTrie<T, U> trie,
                Iterable<T> newWord,
                Optional<U> newDataSupplier) {
        Map.Entry<Iterable<T>, SpaceEfficientTrie<T, U>> lessThanWord = trie.children.floorEntry(newWord);
        Map.Entry<Iterable<T>, SpaceEfficientTrie<T, U>> greaterThanWord = trie.children.ceilingEntry(newWord);
        if (lessThanWord == null && greaterThanWord == null) {
            newDataSupplier.ifPresent(newData -> {
                var newTrie = new SpaceEfficientTrie<T, U>(newData);
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
    attach(SpaceEfficientTrie<T, U> trie,
           Map.Entry<Iterable<T>, SpaceEfficientTrie<T, U>> existingEntry,
           Iterable<T> newWord,
           Optional<U> newDataSupplier) {
        int numCommonChars = lengthOfCommonPrefix(existingEntry.getKey(), newWord);
        List<T> commonWord = substring(newWord, 0, numCommonChars);
        if (commonWord.isEmpty()) {
            // for example the trie has "apple" and we are adding a completely new word "banana"
            // so just add it
            newDataSupplier.ifPresent(newData -> {
                var newTrie = new SpaceEfficientTrie<T, U>(newData);
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
                    var commonTrie = new SpaceEfficientTrie<T, U>(null);
                    trie.removeChild(existingEntry.getKey());
                    trie.addChild(commonWord, commonTrie);
                    commonTrie.addChild(restOfExistingWord, existingEntry.getValue());
                    var newTrie = new SpaceEfficientTrie<T, U>(newDataSupplier.get());
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
                    return new FindInfo<T, U>(existingEntry, oldData);
                }
            }
        }
    }

    private static class FindInfo<T extends Comparable<T>, U> {
        private final Map.Entry<Iterable<T>, SpaceEfficientTrie<T, U>> entry;
        private final U oldData;

        private FindInfo(Map.Entry<Iterable<T>, SpaceEfficientTrie<T, U>> entry, U oldData) {
            this.entry = entry;
            this.oldData = oldData;
        }
    }
}
