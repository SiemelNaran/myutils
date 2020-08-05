package myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;


public class MoreCollectionsTest {
    @Test
    void testAddLargeElementToSortedList() {
        LinkedList<Integer> sortedList = new LinkedList<>();

        MoreCollections.addLargeElementToSortedList(sortedList, Comparator.naturalOrder(), 1);
        assertEquals(List.of(1), sortedList);
        
        MoreCollections.addLargeElementToSortedList(sortedList, Comparator.naturalOrder(), 2);
        assertEquals(List.of(1, 2), sortedList);
        
        MoreCollections.addLargeElementToSortedList(sortedList, Comparator.naturalOrder(), 3);
        assertEquals(List.of(1, 2, 3), sortedList);
        
        MoreCollections.addLargeElementToSortedList(sortedList, Comparator.naturalOrder(), 4);
        assertEquals(List.of(1, 2, 3, 4), sortedList);
        
        MoreCollections.addLargeElementToSortedList(sortedList, Comparator.naturalOrder(), 6);
        assertEquals(List.of(1, 2, 3, 4, 6), sortedList);
        
        MoreCollections.addLargeElementToSortedList(sortedList, Comparator.naturalOrder(), 5);
        assertEquals(List.of(1, 2, 3, 4, 5, 6), sortedList);
   }
}
