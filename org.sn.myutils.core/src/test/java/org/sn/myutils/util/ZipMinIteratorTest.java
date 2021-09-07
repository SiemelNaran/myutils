package org.sn.myutils.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.sn.myutils.testutils.TestUtil.assertException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;


public class ZipMinIteratorTest {
    @Test
    void test() {
        List<Integer> firstList = List.of(2, 4, 4, 5, 7, 15, 17, 23);
        List<Integer> secondList = List.of();
        List<Integer> thirdList = List.of(1, 4, 6, 7, 7, 8, 12, 14, 16, 21, 21, 24);
        List<List<Integer>> lists = List.of(firstList, secondList, thirdList);
        
        List<Integer> everything = new ArrayList<>();

        Iterator<Integer> iter = new ZipMinIterator<>(lists, Comparator.naturalOrder());

        while (iter.hasNext()) {
            int val = iter.next();
            everything.add(val);
        }

        assertException(iter::next, NoSuchElementException.class);

        assertThat(everything, Matchers.contains(1, 2, 4, 4, 4, 5, 6, 7, 7, 7, 8, 12, 14, 15, 16, 17, 21, 21, 23, 24));
    }

}
