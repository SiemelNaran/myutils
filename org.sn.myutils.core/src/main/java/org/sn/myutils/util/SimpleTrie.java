package org.sn.myutils.util;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;


/**
 * Trie class that has one node for each character.
 */
@NotThreadSafe
public class SimpleTrie<T extends Comparable<T>, U> implements Trie<T, U> {
    private final SimpleTrie<T,U> parent;
    private final Map<T, SimpleTrie<T, U>> children = new HashMap<>();
    private U data;
    private int size;

    public static <T extends Comparable<T>, U> SimpleTrie<T, U> create() {
        return new SimpleTrie<>(null);
    }

    private SimpleTrie(SimpleTrie<T, U> parent) {
        this.parent = parent;
    }

    @Override
    public U add(Iterable<T> codePoints, @Nonnull U data) {
        return doAdd(this, codePoints, Objects.requireNonNull(data));
    }
    
    private static @Nullable <T extends Comparable<T>, U> U doAdd(@Nonnull SimpleTrie<T, U> trie, Iterable<T> codePoints, U data) {
        for (T codePoint : codePoints) {
            var parentTrie = trie;
            trie = parentTrie.children.computeIfAbsent(codePoint, ignored -> new SimpleTrie<T, U>(parentTrie));
        }
        U oldData = trie.data;
        trie.data = data;
        if (oldData == null) {
            rollupSize(trie, 1);
        }
        return oldData;
    }

    @Override
    public @Nullable U remove(Iterable<T> codePoints) {
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

    @Override
    public @Nullable U find(Iterable<T> codePoints) {
        var trie = doFind(this, codePoints);
        return trie != null ? trie.data : null;
    }

    private static @Nullable <T extends Comparable<T>, U> SimpleTrie<T, U> doFind(@Nonnull SimpleTrie<T, U> trie, Iterable<T> codePoints) {
        for (Iterator<T> iter = codePoints.iterator(); iter.hasNext(); ) {
            T codePoint = iter.next();
            trie = trie.children.get(codePoint);
            if (trie == null) {
                return null;
            }
        }
        return trie;
    }

    private static <T extends Comparable<T>, U> void rollupSize(SimpleTrie<T, U> trie,  int delta) {
        while (trie != null) {
            trie.size += delta;
            trie = trie.parent;
        }
    }

    @Override
    public int size() {
        return size;
    }

    // New functions:

    public @Nullable SimpleTrie<T, U> findChar(T val) {
        return children.get(val);
    }

    public boolean isWord() {
        return data != null;
    }

    /**
     * Visit all strings in this trie from root to child.
     * This function is used to perform checks on the dictionary.
     *
     * <p>Running time O(N^N).
     *
     * @param consumer a consumer of two args,
     *                 where the first arg is a list of all the values from root to the child node
     *                 and the second arg is the child node.
     */
    public void visit(BiConsumer<List<T>, SimpleTrie<T, U>> consumer) {
        doVisit(new LinkedList<T>(), consumer);
    }
    
    private void doVisit(LinkedList<T> list, BiConsumer<List<T>, SimpleTrie<T, U>> consumer) {
        for (Map.Entry<T, SimpleTrie<T, U>> entry : children.entrySet()) {
            T val = entry.getKey();
            SimpleTrie<T, U> subTrie = entry.getValue();
            list.add(val);
            consumer.accept(list, subTrie);
            subTrie.doVisit(list, consumer);
            list.removeLast();
        }
    }
}
