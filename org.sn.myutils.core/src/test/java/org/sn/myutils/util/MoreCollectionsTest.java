package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.sn.myutils.testutils.TestUtil;
import org.sn.myutils.util.MoreCollections.FindWhich;


public class MoreCollectionsTest {

    private record Room(int floor) {
    }
    
    private enum BinarySearchCaller {
        OBJECT {
            @Override
            int call(List<Room> rooms, int findValue, FindWhich findWhich) {
                return MoreCollections.binarySearch(rooms, 0, rooms.size(), Room::floor, findValue, findWhich);
            }
        },
        
        INT {
            @Override
            int call(List<Room> rooms, int findValue, FindWhich findWhich) {
                return MoreCollections.binarySearchInt(rooms, 0, rooms.size(), Room::floor, findValue, findWhich);
            }
        },
        
        LONG {
            @Override
            int call(List<Room> rooms, int findValue, FindWhich findWhich) {
                return MoreCollections.binarySearchLong(rooms, 0, rooms.size(), Room::floor, findValue, findWhich);
            }
        };
        
        abstract int call(List<Room> rooms, int findValue, FindWhich findWhich);
    }
    
    private static Stream<Arguments> provideArgsFor_testBinarySearch() {
        return Stream.of(Arguments.of(FindWhich.FIND_ANY, BinarySearchCaller.OBJECT),
                         Arguments.of(FindWhich.FIND_FIRST, BinarySearchCaller.OBJECT),
                         Arguments.of(FindWhich.FIND_LAST, BinarySearchCaller.OBJECT),
                         Arguments.of(FindWhich.FIND_ANY, BinarySearchCaller.INT),
                         Arguments.of(FindWhich.FIND_FIRST, BinarySearchCaller.INT),
                         Arguments.of(FindWhich.FIND_LAST, BinarySearchCaller.INT),
                         Arguments.of(FindWhich.FIND_ANY, BinarySearchCaller.LONG),
                         Arguments.of(FindWhich.FIND_FIRST, BinarySearchCaller.LONG),
                         Arguments.of(FindWhich.FIND_LAST, BinarySearchCaller.LONG));
    }
    
    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @MethodSource("provideArgsFor_testBinarySearch")
    void testBinarySearchInt(FindWhich findWhich, BinarySearchCaller binarySearchCaller) {
        List<Room> rooms = Arrays.asList(new Room(1), new Room(2), // index 0
                                         new Room(3), new Room(3), new Room(3), new Room(3), new Room(3), new Room(3), new Room(3), // index 2
                                         new Room(4), new Room(5), new Room(6), new Room(7), new Room(8), new Room(9), // index 9
                                         new Room(10), new Room(11), new Room(12), new Room(13), new Room(14), new Room(15), // index 15
                                         new Room(17), new Room(19)); // index 21
        assertEquals(23, rooms.size());
        assertEquals(-1, binarySearchCaller.call(rooms, 0, findWhich));
        assertEquals(0, binarySearchCaller.call(rooms, 1, findWhich));
        int expect;
        switch (findWhich) { // TODO: Java14: rewrite as below
            case FIND_FIRST: expect = 2; break;
            case FIND_ANY: expect = 5; break;
            case FIND_LAST: expect = 8; break;
            default: throw new UnsupportedOperationException();
        }
        //int expect2 = switch (findWhich) {
        //    case FIND_FIRST -> 2;
        //    case FIND_ANY -> 4;
        //    case FIND_LAST -> 8;
        //}
        assertEquals(expect, binarySearchCaller.call(rooms, 3, findWhich));
        assertEquals(-23, binarySearchCaller.call(rooms, 18, findWhich));
        assertEquals(22, binarySearchCaller.call(rooms, 19, findWhich));
        assertEquals(-24, binarySearchCaller.call(rooms, 20, findWhich));
    }
    
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
