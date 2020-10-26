package org.sn.myutils.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


public class TrieTest {
    @ParameterizedTest
    @ValueSource(strings = { "SimpleTrie", "SpaceEfficientTrie" })
    void testBasic(String clazz) {
        Trie<Character, Boolean> trie;
        if (clazz.equals("SimpleTrie")) {
            trie = new SimpleTrie<>();
        } else if (clazz.equals("SpaceEfficientTrie")) {
            trie = new SpaceEfficientTrie<>();
        } else {
            throw new UnsupportedOperationException();
        }

        assertNull(trie.add(Iterables.charsIteratorAsChar("pan"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("bottle"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("bottom"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("bottleneck"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("bottlenecks"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("orange"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("operation"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("poor"), true));
        assertEquals(8, trie.size());

        assertTrue(trie.add(Iterables.charsIteratorAsChar("bottleneck"), true));
        assertEquals(8, trie.size());

        assertNull(trie.add(Iterables.charsIteratorAsChar("p"), true));
        assertEquals(9, trie.size());

        List<String> words = getAllWords(trie);
        if (clazz.equals("SimpleTrie")) {
            assertThat(words, Matchers.containsInAnyOrder("bottle", "bottleneck", "bottlenecks", "bottom", "operation", "orange", "p", "pan", "poor"));
        } else {
            assertThat(words, Matchers.contains("bottle", "bottleneck", "bottlenecks", "bottom", "operation", "orange", "p", "pan", "poor"));
        }

        assertTrue(trie.find(Iterables.charsIteratorAsChar("bottle")));
        assertTrue(trie.find(Iterables.charsIteratorAsChar("bottom")));
        assertTrue(trie.find(Iterables.charsIteratorAsChar("bottleneck")));
        assertTrue(trie.find(Iterables.charsIteratorAsChar("bottlenecks")));
        assertTrue(trie.find(Iterables.charsIteratorAsChar("orange")));
        assertTrue(trie.find(Iterables.charsIteratorAsChar("operation")));
        assertTrue(trie.find(Iterables.charsIteratorAsChar("pan")));
        assertTrue(trie.find(Iterables.charsIteratorAsChar("poor")));

        assertNull(trie.find(Iterables.charsIteratorAsChar("big")));
        assertNull(trie.find(Iterables.charsIteratorAsChar("bottomless")));
        assertNull(trie.remove(Iterables.charsIteratorAsChar("big")));
        assertNull(trie.remove(Iterables.charsIteratorAsChar("bottomless")));
        assertEquals(9, trie.size());

        assertTrue(trie.remove(Iterables.charsIteratorAsChar("bottleneck")));
        assertEquals(8, trie.size());

        assertTrue(trie.remove(Iterables.charsIteratorAsChar("p")));
        assertEquals(7, trie.size());
        assertTrue(trie.remove(Iterables.charsIteratorAsChar("poor")));
        assertEquals(6, trie.size());

        words = getAllWords(trie);
        if (clazz.equals("SimpleTrie")) {
            assertThat(words, Matchers.containsInAnyOrder("bottle", "bottlenecks", "bottom", "operation", "orange", "pan"));
        } else {
            assertThat(words, Matchers.contains("bottle", "bottlenecks", "bottom", "operation", "orange", "pan"));
        }
    }

    private List<String> getAllWords(Trie<Character, ?> trie) {
        List<String> words = new ArrayList<>(trie.size());
        for (var iter = trie.iterator(); iter.hasNext(); ) {
            var entry = iter.next();
            assertNotNull(entry.getData());
            words.add(toString(entry.getWord()));
        }
        return words;
    }

    @Test
    void testLongest() {
        SimpleTrie<Character, Boolean> trie = new SimpleTrie<>();

        assertNull(trie.add(Iterables.charsIteratorAsChar("pan"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("bottle"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("bottom"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("bottleneck"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("bottlenecks"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("orange"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("operation"), true));
        assertNull(trie.add(Iterables.charsIteratorAsChar("poor"), true));
        assertEquals(8, trie.size());

        RewindableIterator<Character> iter = RewindableIterator.from(Iterables.charsIteratorAsChar("bottomless").iterator());
        assertTrue(trie.findLongest(iter));
        assertEquals('l', iter.next());
        assertEquals('e', iter.next());
        assertEquals('s', iter.next());
        assertEquals('s', iter.next());
        assertFalse(iter.hasNext());

        iter = RewindableIterator.from(Iterables.charsIteratorAsChar("bottom").iterator());
        assertTrue(trie.findLongest(iter));
        assertFalse(iter.hasNext());

        iter = RewindableIterator.from(Iterables.charsIteratorAsChar("bottoms").iterator());
        assertTrue(trie.findLongest(iter));
        assertEquals('s', iter.next());
        assertFalse(iter.hasNext());
    }

    private static String toString(List<Character> word) {
        StringBuilder result = new StringBuilder(word.size());
        word.forEach(result::append);
        return result.toString();
    }
}
