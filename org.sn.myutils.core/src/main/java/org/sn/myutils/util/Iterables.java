package org.sn.myutils.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;


public class Iterables {
    /**
     * An interface indicating that an iterable has a size function.
     */
    public interface Sized {
        int size();
    }

    /**
     * Create a comparator that compares each element in two iterables.
     * For example ['a', 'b'] is less than ['a', 'c'], and is less than ['a', 'b', 'c'].
     *
     * @param <T> the type of each element, which must implement Comparable
     */
    public static <T extends Comparable<T>> Comparator<Iterable<T>> compareIterable() {
        return (lhs, rhs) -> {
            var lIter = lhs.iterator();
            var rIter = rhs.iterator();
            while (lIter.hasNext() || rIter.hasNext()) {
                if (!lIter.hasNext()) {
                    return -1;
                }
                if (!rIter.hasNext()) {
                    return +1;
                }
                int result = lIter.next().compareTo(rIter.next());
                if (result != 0) {
                    return result;
                }
            }
            return 0;
        };
    }

    /**
     * Find the number of leading characters common to both strings.
     */
    public static <T> int lengthOfCommonPrefix(Iterable<T> lhs, Iterable<T> rhs) {
        int count = 0;
        for (Iterator<T> lIter = lhs.iterator(), rIter = rhs.iterator(); lIter.hasNext() && rIter.hasNext(); count++) {
            boolean equal = Objects.equals(lIter.next(), rIter.next());
            if (!equal) {
                break;
            }
        }
        return count;
    }

    /**
     * Create a substring from word.
     * This function is similar to String.substring.
     *
     * @param word the original word
     * @param start the start index
     * @param end one past the last index
     * @param <T> the type of character
     * @return an ArrayList of the substring as a List
     * @throws IndexOutOfBoundsException if start/end are invalid
     */
    public static <T> List<T> substring(Iterable<T> word, int start, int end) {
        if (end < start) {
            throw new IndexOutOfBoundsException("start is greater than end: start=" + start + ", end=" + end);
        }
        List<T> result = new ArrayList<>(end - start);
        try {
            for (Iterator<T> iter = getIterator(word, start); start < end; start++) {
                result.add(iter.next());
            }
        } catch (NoSuchElementException e) {
            throw new IndexOutOfBoundsException("end is greater than size: start=" + start + ", end=" + end);
        }
        return result;
    }

    /**
     * Create a substring from word.
     * This function is similar to String.substring.
     *
     * @param word the original word
     * @param start the start index
     * @param <T> the type of character
     * @return an ArrayList of the substring as a List
     */
    public static <T> List<T> substring(Iterable<T> word, int start) {
        int capacity;
        if (word instanceof List) {
            List<T> list = (List<T>) word;
            capacity = list.size();
        } else {
            capacity = 10;
        }
        List<T> result = new ArrayList<>(capacity);
        for (Iterator<T> iter = getIterator(word, start); iter.hasNext(); ) {
            result.add(iter.next());
        }
        return result;
    }

    public static <T> List<T> concatenate(Iterable<T> first, Iterable<T> second) {
        int capacity;
        if (first instanceof List && second instanceof List) {
            List<T> firstList = (List<T>) first;
            List<T> secondList = (List<T>) second;
            capacity = firstList.size() + secondList.size();
        } else {
            capacity = 10;
        }
        List<T> result = new ArrayList<>(capacity);
        for (T val : first) {
            result.add(val);
        }
        for (T val : second) {
            result.add(val);
        }
        return result;
    }

    /**
     * Given an iterable, return an iterator pointing to the Nth element.
     * If word is a List then use return a list iterator pointing to the Nth element, an O(1) operation.
     * Otherwise get a regular iterator and advance it N times, an O(N) operation.
     *
     * @throws IndexOutOfBoundsException if offset is out of bounds
     */
    public static <T> Iterator<T> getIterator(Iterable<T> word, int offset) {
        if (offset < 0) {
            throw new IndexOutOfBoundsException("offset is negative: offset=" + offset);
        }
        if (word instanceof List) {
            List<T> list = (List<T>) word;
            return list.listIterator(offset);
        } else {
            Iterator<T> iter = word.iterator();
            try {
                for ( ; offset > 0; offset--) {
                    iter.next();
                }
                return iter;
            } catch (NoSuchElementException ignored) {
                throw new IndexOutOfBoundsException("index of bounds: offset=" + offset);
            }
        }
    }


    /**
     * Return a String as an iterable of code points, where each code point is an int.
     */
    public static Iterable<Integer> codePointsIterator(String str) {
        return new SizedIntStreamIterable(str::codePoints, str.length());
    }

    /**
     * Return a String as an iterable of chars, where each char is an int.
     */
    public static Iterable<Integer> charsIteratorAsInt(String str) {
        return new SizedIntStreamIterable(str::chars, str.length());
    }

    /**
     * Return a String as an iterable of chars, where each code point is a Character (not char).
     */
    public static Iterable<Character> charsIteratorAsChar(String str) {
        return new SizedCharacterStreamIterable(str::chars, str.length());
    }


    private static class SizedIntStreamIterable implements Iterable<Integer>, Sized {
        private final Supplier<IntStream> streamSupplier;
        private final int size;

        private SizedIntStreamIterable(Supplier<IntStream> streamSupplier, int size) {
            this.streamSupplier = streamSupplier;
            this.size = size;
        }

        @Override
        public @Nonnull Iterator<Integer> iterator() {
            return streamSupplier.get().iterator();
        }

        @Override
        public int size() {
            return size;
        }
    }

    private static class SizedCharacterStreamIterable implements Iterable<Character>, Sized {
        private final Supplier<IntStream> streamSupplier;
        private final int size;

        private SizedCharacterStreamIterable(Supplier<IntStream> streamSupplier, int size) {
            this.streamSupplier = streamSupplier;
            this.size = size;
        }

        @Override
        public @Nonnull Iterator<Character> iterator() {
            return streamSupplier.get().mapToObj(intValue -> (char)intValue).iterator();
        }

        @Override
        public int size() {
            return size;
        }
    }
}
