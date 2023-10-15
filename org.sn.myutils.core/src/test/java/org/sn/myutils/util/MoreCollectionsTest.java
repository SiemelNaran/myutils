package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
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
        int expect = switch (findWhich) {
            case FIND_FIRST -> 2;
            case FIND_ANY -> 5;
            case FIND_LAST -> 8;
        };
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

    private record Person(String firstName, String middleName, String lastName) {
    }

    @Test
    void testSortSortedListBySecondKey0() {
        var sortedList = new ArrayList<Person>();

        MoreCollections.sortSortedListBySecondKey(sortedList,
                                                  Comparator.comparing(Person::firstName).thenComparing(Person::lastName),
                                                  Comparator.comparing(Person::middleName));

        assertEquals(Collections.emptyList(), sortedList);
    }

    @Test
    void testSortSortedListBySecondKey1() {
        var sortedList = new ArrayList<Person>();
        sortedList.add(new Person("Banana", "1", "Hello"));

        MoreCollections.sortSortedListBySecondKey(sortedList,
                                                  Comparator.comparing(Person::firstName).thenComparing(Person::lastName),
                                                  Comparator.comparing(Person::middleName));

        assertEquals(List.of(new Person("Banana", "1", "Hello")),
                     sortedList);
    }

    @ParameterizedTest(name = TestUtil.PARAMETRIZED_TEST_DISPLAY_NAME)
    @ValueSource(booleans = {false, true})
    void testSortSortedListBySecondKey2(boolean reverse) {
        var sortedList = new ArrayList<Person>();
        if (reverse) {
            sortedList.add(new Person("Banana", "2", "Hello"));
            sortedList.add(new Person("Banana", "1", "Hello"));
        } else {
            sortedList.add(new Person("Banana", "1", "Hello"));
            sortedList.add(new Person("Banana", "2", "Hello"));
        }

        MoreCollections.sortSortedListBySecondKey(sortedList,
                                                  Comparator.comparing(Person::firstName).thenComparing(Person::lastName),
                                                  Comparator.comparing(Person::middleName));

        assertEquals(List.of(new Person("Banana", "1", "Hello"),
                             new Person("Banana", "2", "Hello")),
                     sortedList);
    }

    @Test
    void testSortSortedListBySecondKey() {
        var sortedList = new ArrayList<Person>();
        sortedList.add(new Person("Apple", "1", "Hello"));
        sortedList.add(new Person("Apple", "3", "World"));
        sortedList.add(new Person("Apple", "4", "World"));
        sortedList.add(new Person("Apple", "2", "World"));
        sortedList.add(new Person("Apple", "1", "World"));
        sortedList.add(new Person("Banana", "1", "Hello"));
        sortedList.add(new Person("Banana", "2", "Hello"));
        sortedList.add(new Person("Banana", "1", "World"));
        sortedList.add(new Person("Carrot", "2", "Hello"));
        sortedList.add(new Person("Carrot", "1", "Hello"));

        MoreCollections.sortSortedListBySecondKey(sortedList,
                                                  Comparator.comparing(Person::firstName).thenComparing(Person::lastName),
                                                  Comparator.comparing(Person::middleName));

        assertEquals(List.of(new Person("Apple", "1", "Hello"),
                             new Person("Apple", "1", "World"),
                             new Person("Apple", "2", "World"),
                             new Person("Apple", "3", "World"),
                             new Person("Apple", "4", "World"),
                             new Person("Banana", "1", "Hello"),
                             new Person("Banana", "2", "Hello"),
                             new Person("Banana", "1", "World"),
                             new Person("Carrot", "1", "Hello"),
                             new Person("Carrot", "2", "Hello")),
                     sortedList);
    }

    @Test
    void testSortSortedListBySecondKey_Performance() {
        var list = new ArrayList<Person>((70_000 + 20_000*2 + 8_000*3 + 2_000*4) * 100);

        // create an array with 100,000 groups
        // 70,000 groups will have 1 element
        // 20,000 groups will have 2 elements
        // 8,000 groups will have 3 elements
        // 2,000 groups will have 4 elements
        var numGroupsArray = new int[] { 70_000, 20_000, 8_000, 2_000 };
        var random = new Random();
        for (int numElementsInGroupMinusOne = 0, personId = 0; numElementsInGroupMinusOne < numGroupsArray.length; numElementsInGroupMinusOne++) {
            int numGroups = numGroupsArray[numElementsInGroupMinusOne];
            for (int i = 0; i < numGroups * 100; i++) {
                for (int j = 0; j <= numElementsInGroupMinusOne; j++) {
                    personId += 1;
                    String name = String.format("%08d", personId);
                    String middleName = Integer.toString(random.nextInt(64));
                    var person = new Person(name, middleName, name);
                    list.add(person);
                }
            }
        }

        var list1 = new ArrayList<>(list);
        var list2 = new ArrayList<>(list);
        long startTime;

        startTime = System.nanoTime();
        list1.sort(Comparator.comparing(Person::firstName).thenComparing(Person::lastName));
        long normalSortTimeTaken = System.nanoTime() - startTime;

        startTime = System.nanoTime();
        MoreCollections.sortSortedListBySecondKey(list2,
                                                  Comparator.comparing(Person::firstName),
                                                  Comparator.comparing(Person::middleName));
        long optimizedSortTimeTaken = System.nanoTime() - startTime;

        assertEquals(list1, list2);
        System.out.println("normalSortTimeTaken   =" + normalSortTimeTaken);
        System.out.println("optimizedSortTimeTaken=" + optimizedSortTimeTaken);
    }
}
