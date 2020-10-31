package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.sn.myutils.testutils.TestUtil.assertException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


class AdaptingIteratorTest {
    @ParameterizedTest
    @ValueSource(booleans = { false, true})
    void test(boolean allowRemove) {
        List<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);

        Iterator<Integer> iterator = new AdaptingIterator<>(list.iterator(), val -> val * val, allowRemove);
        assertTrue(iterator.hasNext());
        assertEquals(1, iterator.next());
        if (allowRemove) {
            iterator.remove();
        } else {
            assertException(iterator::remove, UnsupportedOperationException.class);
        }
        assertTrue(iterator.hasNext());
        assertEquals(4, iterator.next());
        assertTrue(iterator.hasNext());
        assertEquals(9, iterator.next());
        assertFalse(iterator.hasNext());
        if (allowRemove) {
            assertEquals(List.of(2, 3), list);
        } else {
            assertEquals(List.of(1, 2, 3), list);
        }
    }
}