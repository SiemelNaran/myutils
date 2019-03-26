package myutils.util;

import static myutils.TestUtil.assertException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.stream.IntStream;

import org.junit.jupiter.api.Test;


public class ArrayListIteratorTest {
    @Test
    void testListIteratorRemoveAdd() throws Exception {
        List<Integer> list = new ArrayList<>();
        IntStream.range(1, 7).forEach(list::add);
        assertEquals("[1, 2, 3, 4, 5, 6]", list.toString());
        ListIterator<Integer> listIter = list.listIterator(4);
        
        assertEquals(4, listIter.previous().intValue());
        listIter.remove();
        assertEquals("[1, 2, 3, 5, 6]", list.toString());
        
        assertEquals(3, listIter.previous().intValue());
        listIter.remove();
        assertEquals("[1, 2, 5, 6]", list.toString());

        assertEquals(2, listIter.previous().intValue());
        listIter.remove();
        assertEquals("[1, 5, 6]", list.toString());

        assertEquals(1, listIter.previous().intValue());
        assertFalse(listIter.hasPrevious());
        listIter.remove();
        assertEquals("[5, 6]", list.toString());
        assertEquals(2, list.size());
        assertFalse(listIter.hasPrevious());
        
        listIter.add(10);
        assertEquals("[10, 5, 6]", list.toString());
        assertTrue(listIter.hasPrevious());

        listIter.add(20);
        assertEquals("[10, 20, 5, 6]", list.toString());
        assertEquals(4, list.size());

        listIter.add(30);
        assertEquals("[10, 20, 30, 5, 6]", list.toString());

        listIter.add(40);
        assertEquals("[10, 20, 30, 40, 5, 6]", list.toString());

        assertEquals(5, listIter.next().intValue());
        assertEquals(6, listIter.next().intValue());
        assertFalse(listIter.hasNext());
        assertTrue(listIter.hasPrevious());
        
        assertEquals(6, listIter.previous().intValue());
        assertEquals(5, listIter.previous().intValue());
        assertEquals(40, listIter.previous().intValue());
        
        assertEquals(40, listIter.next().intValue());
        assertEquals(5, listIter.next().intValue());
        assertEquals(6, listIter.next().intValue());
        assertFalse(listIter.hasNext());
        
        assertEquals(6, listIter.previous().intValue());
        listIter.remove();
        assertEquals("[10, 20, 30, 40, 5]", list.toString());
        assertEquals(5, listIter.previous().intValue());
        listIter.remove();
        assertEquals("[10, 20, 30, 40]", list.toString());
        assertEquals(40, listIter.previous().intValue());
        listIter.remove();
        assertEquals("[10, 20, 30]", list.toString());
        assertEquals(30, listIter.previous().intValue());
        listIter.remove();
        assertEquals("[10, 20]", list.toString());
        assertEquals(20, listIter.previous().intValue());
        listIter.remove();
        assertEquals("[10]", list.toString());
        assertEquals(10, listIter.previous().intValue());
        listIter.remove();
        assertEquals("[]", list.toString());
        
        assertFalse(listIter.hasNext());
        assertFalse(listIter.hasPrevious());
        assertEquals(0, listIter.nextIndex());
        assertEquals(-1, listIter.previousIndex());
        assertException(() -> listIter.next(), NoSuchElementException.class);
        assertException(() -> listIter.previous(), NoSuchElementException.class);
        assertException(() -> listIter.remove(), IllegalStateException.class);
               
        listIter.add(100);
        assertEquals("[100]", list.toString());
        
        listIter.add(200);
        assertEquals("[100, 200]", list.toString());
        
        listIter.add(300);
        assertEquals("[100, 200, 300]", list.toString());
        
        listIter.add(400);
        assertEquals("[100, 200, 300, 400]", list.toString());
        
        listIter.add(500);
        assertEquals("[100, 200, 300, 400, 500]", list.toString());
        
        assertException(() -> listIter.remove(), IllegalStateException.class);
        assertEquals("[100, 200, 300, 400, 500]", list.toString());
        
        assertEquals(500, listIter.previous().intValue());
    }    
}
