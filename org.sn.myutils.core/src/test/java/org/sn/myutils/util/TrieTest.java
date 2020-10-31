package org.sn.myutils.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.sn.myutils.testutils.TestUtil.assertException;
import static org.sn.myutils.testutils.TestUtil.assertExceptionFromCallable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.sn.myutils.testutils.TestBase;

/**
 * Test the trie classes.
 *
 * <p>Achieves code coverage in the following classes:
 * - Trie.java
 * - TrieIterationHelper.java
 * - SimpleTrie.java
 * - SpaceEfficientTrie.java
 */
public class TrieTest extends TestBase {
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

        assertExceptionFromCallable(() -> trie.put(Iterables.charsIteratorAsChar("NullNotAllowed"), null), NullPointerException.class);

        assertNull(trie.put(Iterables.charsIteratorAsChar("pan"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottle"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottom"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottleneck"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottlenecks"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("orange"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("operation"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("poor"), true));
        assertEquals(8, trie.size());

        assertNotNull(trie.put(Iterables.charsIteratorAsChar("bottleneck"), true));
        assertEquals(8, trie.size());

        assertNull(trie.put(Iterables.charsIteratorAsChar("p"), true));
        assertEquals(9, trie.size());

        List<String> words = getAllWords(trie);
        if (clazz.equals("SimpleTrie")) {
            assertThat(words, Matchers.containsInAnyOrder("bottle", "bottleneck", "bottlenecks", "bottom", "operation", "orange", "p", "pan", "poor"));
        } else {
            assertThat(words, Matchers.contains("bottle", "bottleneck", "bottlenecks", "bottom", "operation", "orange", "p", "pan", "poor"));
        }

        assertNotNull(trie.get(Iterables.charsIteratorAsChar("bottle")));
        assertNotNull(trie.get(Iterables.charsIteratorAsChar("bottom")));
        assertNotNull(trie.get(Iterables.charsIteratorAsChar("bottleneck")));
        assertNotNull(trie.get(Iterables.charsIteratorAsChar("bottlenecks")));
        assertNotNull(trie.get(Iterables.charsIteratorAsChar("orange")));
        assertNotNull(trie.get(Iterables.charsIteratorAsChar("operation")));
        assertNotNull(trie.get(Iterables.charsIteratorAsChar("pan")));
        assertNotNull(trie.get(Iterables.charsIteratorAsChar("poor")));

        Object cBottle = Iterables.charsIteratorAsChar("bottle");
        assertTrue(trie.containsKey(cBottle));
        assertNotNull(trie.get(cBottle));

        assertNull(trie.get(Iterables.charsIteratorAsChar("big")));
        assertNull(trie.get(Iterables.charsIteratorAsChar("bottomless")));
        assertNull(trie.remove(Iterables.charsIteratorAsChar("big")));
        assertNull(trie.remove(Iterables.charsIteratorAsChar("bottomless")));
        assertEquals(9, trie.size());

        Object cBig = Iterables.charsIteratorAsChar("big");
        assertFalse(trie.containsKey(cBig));
        assertNull(trie.get(cBig));
        assertNull(trie.get(55));

        assertNotNull(trie.remove(Iterables.charsIteratorAsChar("bottleneck")));
        assertEquals(8, trie.size());
        assertNull(trie.remove(Iterables.charsIteratorAsChar("bottleneck")));
        assertEquals(8, trie.size());
        assertNull(trie.remove(55));
        assertEquals(8, trie.size());

        assertNotNull(trie.remove(Iterables.charsIteratorAsChar("p")));
        assertEquals(7, trie.size());
        Object cPoor = Iterables.charsIteratorAsChar("poor");
        assertNotNull(trie.remove(cPoor));
        assertEquals(6, trie.size());

        words = getAllWords(trie);
        if (clazz.equals("SimpleTrie")) {
            assertThat(words, Matchers.containsInAnyOrder("bottle", "bottlenecks", "bottom", "operation", "orange", "pan"));
        } else {
            assertThat(words, Matchers.contains("bottle", "bottlenecks", "bottom", "operation", "orange", "pan"));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "SimpleTrie", "SpaceEfficientTrie" })
    void testIterationRemove(String clazz) {
        Trie<Character, Boolean> trie;
        if (clazz.equals("SimpleTrie")) {
            trie = new SimpleTrie<>();
        } else if (clazz.equals("SpaceEfficientTrie")) {
            trie = new SpaceEfficientTrie<>();
        } else {
            throw new UnsupportedOperationException();
        }

        assertNull(trie.put(Iterables.charsIteratorAsChar("pan"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottle"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottom"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottleneck"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottlenecks"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("orange"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("operation"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("poor"), true));
        assertEquals(8, trie.size());

        Iterator<Trie.TrieEntry<Character, Boolean>> iterator = trie.trieIterator();
        assertException(iterator::remove, IllegalStateException.class);
        assertNotNull(iterator.next()); // bottle for SpaceEfficientTrie
        iterator.remove();
        assertException(iterator::remove, IllegalStateException.class);
        assertNotNull(iterator.next()); // bottleneck for SpaceEfficientTrie
        iterator.remove();
        assertException(iterator::remove, IllegalStateException.class);

        List<String> words = getAllWords(trie);
        if (clazz.equals("SimpleTrie")) {
            assertThat(words, Matchers.hasSize(6));
        } else {
            assertThat(words, Matchers.contains("bottlenecks", "bottom", "operation", "orange", "pan", "poor"));
        }

        while (iterator.hasNext()) {
            assertNotNull(iterator.next());
        }

        assertException(iterator::remove, NoSuchElementException.class);

        // test ConcurrentModificationException
        assertNull(trie.put(Iterables.charsIteratorAsChar("hello"), true));
        assertExceptionFromCallable(iterator::hasNext, ConcurrentModificationException.class);
        assertExceptionFromCallable(iterator::next, ConcurrentModificationException.class);
        assertException(iterator::remove, ConcurrentModificationException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = { "SimpleTrie", "SpaceEfficientTrie" })
    void testCollection(String clazz) {
        Trie<Character, Boolean> trie;
        if (clazz.equals("SimpleTrie")) {
            trie = new SimpleTrie<>();
        } else if (clazz.equals("SpaceEfficientTrie")) {
            trie = new SpaceEfficientTrie<>();
        } else {
            throw new UnsupportedOperationException();
        }

        assertNull(trie.put(Iterables.charsIteratorAsChar("pan"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottle"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottom"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottleneck"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottlenecks"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("orange"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("operation"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("poor"), true));
        assertEquals(8, trie.size());

        // test keySet
        Set<Iterable<Character>> keySet = trie.keySet();
        assertEquals(8, keySet.size());
        Object cBottle = Iterables.charsIteratorAsChar("bottle");
        Object cPoor = Iterables.charsIteratorAsChar("poor");
        Object cBig = Iterables.charsIteratorAsChar("big");
        assertTrue(keySet.containsAll(List.of(cBottle, cPoor)));
        assertFalse(keySet.containsAll(List.of(cBottle, cBig)));

        // test values
        Collection<Boolean> values = trie.values();
        assertEquals(8, values.size());
        assertThat(values.stream().distinct().map(Object::toString).collect(Collectors.joining()),
                   Matchers.equalTo("true")); // assert that all elements are true
        assertTrue(trie.containsValue(Boolean.TRUE));
        assertFalse(trie.containsValue(Boolean.FALSE));

        // test entrySet
        List<String> words = getAllWords(trie);
        List<String> wordsViaEntrySet = new ArrayList<>();
        trie.forEach((key, value) -> wordsViaEntrySet.add(toString(key)));
        assertEquals(8, trie.entrySet().size());
        assertEquals(words, wordsViaEntrySet);
        trie.entrySet().forEach(entry -> entry.setValue(false));
        assertThat(values.stream().distinct().map(Object::toString).collect(Collectors.joining()),
                   Matchers.equalTo("false")); // assert that all elements are false
        assertExceptionFromCallable(() -> trie.entrySet().iterator().next().setValue(null), NullPointerException.class);
    }

    private List<String> getAllWords(Trie<Character, ?> trie) {
        List<String> words = new ArrayList<>(trie.size());
        var iter = trie.trieIterator();
        while (iter.hasNext()) {
            var entry = iter.next();
            assertNotNull(entry.getValue());
            words.add(toString(entry.getWord()));
        }
        assertExceptionFromCallable(iter::next, NoSuchElementException.class);
        return words;
    }

    @Test
    void testLongest() {
        SimpleTrie<Character, Boolean> trie = new SimpleTrie<>();

        assertNull(trie.put(Iterables.charsIteratorAsChar("pan"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottle"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottom"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottleneck"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("bottlenecks"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("orange"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("operation"), true));
        assertNull(trie.put(Iterables.charsIteratorAsChar("poor"), true));
        assertEquals(8, trie.size());

        RewindableIterator<Character> iter = RewindableIterator.from(Iterables.charsIteratorAsChar("bottomless").iterator());
        assertNotNull(trie.getLongest(iter));
        assertEquals('l', iter.next());
        assertEquals('e', iter.next());
        assertEquals('s', iter.next());
        assertEquals('s', iter.next());
        assertFalse(iter.hasNext());

        iter = RewindableIterator.from(Iterables.charsIteratorAsChar("bottom").iterator());
        assertNotNull(trie.getLongest(iter));
        assertFalse(iter.hasNext());

        iter = RewindableIterator.from(Iterables.charsIteratorAsChar("bottoms").iterator());
        assertNotNull(trie.getLongest(iter));
        assertEquals('s', iter.next());
        assertFalse(iter.hasNext());
    }

    private static String toString(Iterable<Character> word) {
        StringBuilder result = new StringBuilder();
        word.forEach(result::append);
        return result.toString();
    }
}
