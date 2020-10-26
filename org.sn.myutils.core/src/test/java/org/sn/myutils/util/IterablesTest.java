package org.sn.myutils.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.sn.myutils.testutils.TestUtil.assertExceptionFromCallable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


public class IterablesTest {
    @Test
    void testCompareIterable() {
        assertThat(Iterables.<Character>compareIterable().compare(Arrays.asList('a', 'b'), Arrays.asList('a', 'b')), Matchers.equalTo(0));
        assertThat(Iterables.<Character>compareIterable().compare(Arrays.asList('a', 'b'), Arrays.asList('a', 'c')), Matchers.lessThan(0));
        assertThat(Iterables.<Character>compareIterable().compare(Arrays.asList('a', 'c'), Arrays.asList('a', 'b')), Matchers.greaterThan(0));
        assertThat(Iterables.<Character>compareIterable().compare(Arrays.asList('a', 'b'), Arrays.asList('a', 'b', 'c')), Matchers.lessThan(0));
        assertThat(Iterables.<Character>compareIterable().compare(Arrays.asList('a', 'b', 'c'), Arrays.asList('a', 'b')), Matchers.greaterThan(0));
    }

    @Test
    void testLengthOfCommonPrefix() {
        assertThat(Iterables.lengthOfCommonPrefix(Arrays.asList('a', 'b'), Arrays.asList('a', 'b')), Matchers.equalTo(2));
        assertThat(Iterables.lengthOfCommonPrefix(Arrays.asList('a', null, 'c'), Arrays.asList('a', 'b', 'c')), Matchers.equalTo(1));
        assertThat(Iterables.lengthOfCommonPrefix(Arrays.asList('a', null, 'c'), Arrays.asList('a', null, 'c')), Matchers.equalTo(3));
        assertThat(Iterables.lengthOfCommonPrefix(Arrays.asList('a', 'b'), Arrays.asList('a', 'c')), Matchers.equalTo(1));
        assertThat(Iterables.lengthOfCommonPrefix(Arrays.asList('a', 'b'), Arrays.asList('a', 'b', 'c')), Matchers.equalTo(2));
        assertThat(Iterables.lengthOfCommonPrefix(Arrays.asList('a', 'b', 'c'), Arrays.asList('a', 'b')), Matchers.equalTo(2));
        assertThat(Iterables.lengthOfCommonPrefix(Collections.emptyList(), Arrays.asList('a', 'b')), Matchers.equalTo(0));
    }

    @ParameterizedTest
    @ValueSource(strings = { "list", "set" })
    void testSubstringAndGetIterator(String collection) {
        Function<Character[], Collection<Character>> creator;
        if (collection.equals("list")) {
            creator = CREATE_LIST;
        } else if (collection.equals("set")) {
            creator = CREATE_SET;
        } else {
            throw new UnsupportedOperationException();
        }

        assertThat(Iterables.substring(creator.apply(new Character[] { 'a', 'b', 'c', 'd' }), 0, 4), Matchers.equalTo(Arrays.asList('a', 'b', 'c', 'd')));
        assertThat(Iterables.substring(creator.apply(new Character[] { 'a', 'b', 'c' }), 0, 3), Matchers.equalTo(Arrays.asList('a', 'b', 'c')));
        assertThat(Iterables.substring(creator.apply(new Character[] { 'a', 'b', 'c' }), 1, 3), Matchers.equalTo(Arrays.asList('b', 'c')));
        assertExceptionFromCallable(() -> Iterables.substring(creator.apply(new Character[] { 'a', 'b', 'c', 'd' }), 3, 1), IndexOutOfBoundsException.class);
        assertExceptionFromCallable(() -> Iterables.substring(creator.apply(new Character[] { 'a', 'b', 'c', 'd' }), 1, 5), IndexOutOfBoundsException.class);
        assertExceptionFromCallable(() -> Iterables.substring(creator.apply(new Character[] { 'a', 'b', 'c', 'd' }), -1, 3), IndexOutOfBoundsException.class);
        assertExceptionFromCallable(() -> Iterables.substring(creator.apply(new Character[] { 'a', 'b', 'c', 'd' }), 5, 7), IndexOutOfBoundsException.class);

        assertThat(Iterables.substring(creator.apply(new Character[] { 'a', 'b', 'c', 'd' }), 0), Matchers.equalTo(Arrays.asList('a', 'b', 'c', 'd')));
        assertThat(Iterables.substring(creator.apply(new Character[] { 'a', 'b', 'c', 'd' }), 1), Matchers.equalTo(Arrays.asList('b', 'c', 'd')));
    }

    @ParameterizedTest
    @ValueSource(strings = { "list", "set" })
    void testConcatenate(String collection) {
        Function<Character[], Collection<Character>> creator;
        if (collection.equals("list")) {
            creator = CREATE_LIST;
        } else if (collection.equals("set")) {
            creator = CREATE_SET;
        } else {
            throw new UnsupportedOperationException();
        }

        Iterable<Character> first = creator.apply(new Character[] { 'a', 'b', 'c' });
        Iterable<Character> second = creator.apply(new Character[] { 'd', 'e', 'f', 'g' });
        List<Character> both = Iterables.concatenate(first, second);
        assertEquals(7, both.size());
        assertThat(both, Matchers.equalTo(Arrays.asList('a', 'b', 'c', 'd', 'e', 'f', 'g')));
    }

    private static final Function<Character[], Collection<Character>> CREATE_LIST = Arrays::asList;

    private static final Function<Character[], Collection<Character>> CREATE_SET = vals -> {
        Set<Character> result = new TreeSet<>();
        Collections.addAll(result, vals);
        return result;
    };

    @ParameterizedTest
    @ValueSource(strings = { "codePoints", "chars" })
    void testStringToIntIterable(String method) {
        String str = "AB";
        Iterable<Integer> iterable;
        if (method.equals("codePoints")) {
            iterable = Iterables.codePointsIterator(str);
        } else if (method.equals("chars")) {
            iterable = Iterables.charsIteratorAsInt(str);
        } else {
            throw new UnsupportedOperationException();
        }
        assertEquals(2, ((Iterables.Sized)iterable).size());
        Iterator<Integer> iter = iterable.iterator();
        assertEquals(65, iter.next());
        assertEquals(66, iter.next());
        assertFalse(iter.hasNext());
    }

    @Test
    void testStringToCharacterIterable() {
        String str = "AB";
        Iterable<Character> iterable = Iterables.charsIteratorAsChar(str);
        assertEquals(2, ((Iterables.Sized)iterable).size());
        Iterator<Character> iter = iterable.iterator();
        assertEquals('A', iter.next());
        assertEquals('B', iter.next());
        assertFalse(iter.hasNext());
    }
}
