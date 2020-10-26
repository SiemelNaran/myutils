package org.sn.myutils.util;

import static org.sn.myutils.util.Iterables.concatenate;

import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.stream.Stream;


class TrieIterationHelper {
    interface TrieNode<T extends Comparable<T>, U> {
        U getData();

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
        private final int expectedModCount;
        private final Stack<TrieNodePosition<T, U>> stack = new Stack<>();

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
            Trie.TrieEntry<T, U> entry = new Trie.TrieEntry<>() {
                @Override
                public List<T> getWord() {
                    return top.pathToHere;
                }

                @Override
                public U getData() {
                    return top.trieNode.getData();
                }
            };
            gotoNext();
            return entry;
        }

        private void checkForConcurrentModification() {
            if (getTrieModificationCount() != expectedModCount) {
                throw new ConcurrentModificationException();
            }
        }

        abstract int getTrieModificationCount();
    }
}