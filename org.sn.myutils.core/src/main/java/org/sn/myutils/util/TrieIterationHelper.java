package org.sn.myutils.util;

import static org.sn.myutils.util.Iterables.concatenate;

import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


class TrieIterationHelper {
    interface TrieNode<T extends Comparable<T>, U> {
        U getData();

        U setData(@Nonnull U newData);

        Stream<Map.Entry<Iterable<T>, ? extends TrieNode<T, U>>> childrenIterator();
    }

    /**
     * A pointer to a trie node and current iteration index over that node.
     */
    private static class TrieNodePosition<T extends Comparable<T>, U> {
        private final List<T> pathToHere;
        private final TrieNode<T, U> trieNode;
        private final Iterator<Map.Entry<Iterable<T>, ? extends TrieNode<T, U>>> iter;

        private TrieNodePosition(List<T> pathToHere, TrieNode<T, U> trieNode) {
            this.pathToHere = pathToHere;
            this.trieNode = trieNode;
            this.iter = trieNode.childrenIterator().iterator();
        }
    }

    abstract static class TrieEntryIterator<T extends Comparable<T>, U> implements Iterator<Trie.TrieEntry<T, U>> {
        private int expectedModCount;
        private final Stack<TrieNodePosition<T, U>> stack = new Stack<>();
        private List<T> pathToHere;

        TrieEntryIterator(TrieNode<T, U> root) {
            expectedModCount = getTrieModificationCount();
            stack.add(new TrieNodePosition<>(Collections.emptyList(), root));
            gotoNext();
        }

        private void gotoNext() {
            while (!stack.isEmpty()) {
                var top = stack.peek();
                if (top.iter.hasNext()) {
                    Map.Entry<Iterable<T>, ? extends TrieNode<T, U>> childEntry = top.iter.next();
                    var newTop = new TrieNodePosition<>(concatenate(top.pathToHere, childEntry.getKey()), childEntry.getValue());
                    stack.add(newTop);
                    if (newTop.trieNode.getData() != null) {
                        return;
                    }
                } else {
                    stack.pop();
                }
            }
        }

        @Override
        public boolean hasNext() {
            checkForConcurrentModification();
            return !stack.isEmpty();
        }

        @Override
        public Trie.TrieEntry<T, U> next() {
            checkForConcurrentModification();
            if (stack.isEmpty()) {
                throw new NoSuchElementException();
            }
            TrieNodePosition<T, U> top = stack.peek();
            pathToHere = top.pathToHere;
            Trie.TrieEntry<T, U> entry = new TrieEntryImpl<>(pathToHere, top.trieNode);
            gotoNext();
            return entry;
        }

        private static class TrieEntryImpl<T extends Comparable<T>, U> implements Trie.TrieEntry<T, U> {
            private final List<T> pathToHere;
            private final TrieNode<T,U> node;

            public TrieEntryImpl(List<T> pathToHere, TrieNode<T, U> node) {
                this.pathToHere = pathToHere;
                this.node = node;
            }

            @Override
            public List<T> getWord() {
                return pathToHere;
            }

            @Override
            public Iterable<T> getKey() {
                return pathToHere;
            }

            @Override
            public U getValue() {
                return node.getData();
            }

            @Override
            public U setValue(U value) {
                return node.setData(value);
            }
        }

        @Override
        public void remove() {
            checkForConcurrentModification();
            if (stack.isEmpty()) {
                throw new NoSuchElementException();
            }
            if (pathToHere == null) {
                throw new IllegalStateException();
            }
            doRemove(pathToHere);
            expectedModCount = getTrieModificationCount();
            pathToHere = null;
        }

        private void checkForConcurrentModification() {
            if (getTrieModificationCount() != expectedModCount) {
                throw new ConcurrentModificationException();
            }
        }

        abstract int getTrieModificationCount();

        abstract void doRemove(Iterable<T> word);
    }

    abstract static class TrieMap<T extends Comparable<T>, U> extends AbstractMap<Iterable<T>, U> implements Trie<T, U> {
        @Override
        @SuppressWarnings("unchecked")
        public final U get(Object key) {
            if (key instanceof Iterable) {
                return get((Iterable<T>)key); // may throw is key an Iterable<SomethingElse>
            }
            return null;
        }

        @Override
        public abstract @Nullable U get(Iterable<T> codePoints);

        @Override
        @SuppressWarnings("unchecked")
        public final U remove(Object key) {
            if (key instanceof Iterable) {
                return remove((Iterable<T>)key); // may throw is key an Iterable<SomethingElse>
            }
            return null;
        }

        @Override
        public abstract @Nullable U remove(Iterable<T> codePoints);

        @Override
        public final boolean containsKey(Object key) {
            return get(key) != null;
        }

        @Override
        public final @Nonnull Set<Entry<Iterable<T>, U>> entrySet() {
            return new TrieSet<>(this);
        }
    }

    static class TrieSet<T extends Comparable<T>, U> extends AbstractSet<Map.Entry<Iterable<T>, U>> implements Iterables.Sized {
        private final Trie<T,U> trie;

        TrieSet(Trie<T, U> trie) {
            this.trie = trie;
        }

        @Override
        public int size() {
            return trie.size();
        }

        @Override
        public @Nonnull Iterator<Map.Entry<Iterable<T>, U>> iterator() {
            return new AdaptingIterator<>(trie.trieIterator(), trieEntry -> trieEntry, true); // 2nd arg as Function.identity() gives compile error
        }
    }
}