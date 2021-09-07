package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.sn.myutils.testutils.TestUtil.assertException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Spliterator;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;


public class PageListTest {
    
    // helpers for testing ArrayListPageList and LinkedListPageList 

    @SuppressWarnings("rawtypes")
    private static final ThreadLocal<Class<? extends PageList>> PAGE_LIST_CLASS = new ThreadLocal<>();
    
    @SuppressWarnings("rawtypes")
    private void testAllLists(Runnable runnable) {
        for (Class<? extends PageList> clazz: Arrays.asList(
                                                             ArrayListPageList.class,
                                                             LinkedListPageList.class
                                                           )) {
            try {
                PAGE_LIST_CLASS.set(clazz);
                runnable.run();
            } catch (AssertionError e) {
                throw new AssertionError("error running " + clazz.getSimpleName(), e);
            } catch (RuntimeException e) {
                throw new RuntimeException("error running " + clazz.getSimpleName(), e);
            } finally {
                PAGE_LIST_CLASS.set(null);
            }
        }
    }

    private static PageList<Integer> constructPageList(int preferredMaxPageSize, int maxPageSize) {
        if (ArrayListPageList.class.equals(PAGE_LIST_CLASS.get())) {
            return new ArrayListPageList<>(preferredMaxPageSize, maxPageSize);
        }
        if (LinkedListPageList.class.equals(PAGE_LIST_CLASS.get())) {
            return new LinkedListPageList<>(preferredMaxPageSize, maxPageSize);
        }
        throw new UnsupportedOperationException();
    }
    
    private static PageList<Integer> constructPageList() {
        if (ArrayListPageList.class.equals(PAGE_LIST_CLASS.get())) {
            return new ArrayListPageList<>();
        }
        if (LinkedListPageList.class.equals(PAGE_LIST_CLASS.get())) {
            return new LinkedListPageList<>();
        }
        throw new UnsupportedOperationException();
    }
    
    private static PageList<Integer> constructPageList(List<Integer> normalList, int preferredMaxPageSize, int maxPageSize) {
        if (ArrayListPageList.class.equals(PAGE_LIST_CLASS.get())) {
            return new ArrayListPageList<>(normalList, preferredMaxPageSize, maxPageSize);
        }
        if (LinkedListPageList.class.equals(PAGE_LIST_CLASS.get())) {
            return new LinkedListPageList<>(normalList, preferredMaxPageSize, maxPageSize);
        }
        throw new UnsupportedOperationException();
    }
    
    
    // tests

    @Test
    void testOutOfBounds1() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            for (int index: new int[] { -1, 1 }) {
                assertException(() -> list.add(index, 99),
                                IndexOutOfBoundsException.class,
                                "Index: " + index + ", Size: 0");
            }
        });
    }
    
    @Test
    void testOutOfBounds2() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            list.add(91);
            list.add(92);
            list.add(93);
            list.add(94);
            for (int index: new int[] { 5, -1 }) {
                assertException(() -> list.add(index, 99),
                                IndexOutOfBoundsException.class,
                                "Index: " + index + ", Size: 4");
            }
        });
    }
    
    @Test
    void testEmpty() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            assertEquals(0, list.size());
            assertTrue(list.isEmpty());
            assertEquals(0, list.stream().count());
            assertNull(list.spliterator().trySplit());
            list.spliterator().tryAdvance(val -> { throw new RuntimeException(); });
            list.spliterator().forEachRemaining(val -> { throw new RuntimeException(); });
        });
    }
    
    @Test
    void testOutOfBounds3() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            list.add(91);
            list.add(92);
            list.add(93);
            list.add(94);
            for (int index: new int[] { 5, 4, -1 }) {
                assertException(() -> list.set(index, 123),
                                IndexOutOfBoundsException.class,
                                "Index: " + index + ", Size: 4");
            }
        });
    }
    
    @Test
    void testAdd() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            assertEqualsByGetAndIterAndStream("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15", list);
            
            list.add(4, 0);
            assertEquals("0[1,2,3] * 3[4,0,5,6] * 7[7,8,9] * 10[10,11,12] * 13[13,14,15]", extract(list));
            
            list.add(4, 1);
            assertEquals("0[1,2,3] * 3[4,1,0,5,6] * 8[7,8,9] * 11[10,11,12] * 14[13,14,15]", extract(list));
            
            list.add(4, 2);
            assertEquals("0[1,2,3] * 3[4,2] * 5[1,0,5,6] * 9[7,8,9] * 12[10,11,12] * 15[13,14,15]", extract(list));
        });
    }
    
    @Test
    void testSet() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            list.set(0, 10);
            list.set(10, 110);
            list.set(14, 150);
            assertEquals("0[10,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,110,12] * 12[13,14,150]", extract(list));
        });
    }
    
    @Test
    void testAddAll1() {
        testAllLists(() -> {
            List<Integer> normalList = new ArrayList<>();
            IntStream.range(1, 16).forEach(normalList::add);
            PageList<Integer> list = constructPageList(normalList, 3, 5);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            assertEqualsByGetAndIterAndStream("1,2,3,4,5,6,7,8,9,10,11,12,13,14,15", list);
            
            list.addAll(4, Arrays.asList(0));
            assertEquals("0[1,2,3] * 3[4,0,5,6] * 7[7,8,9] * 10[10,11,12] * 13[13,14,15]", extract(list));
            
            list.addAll(4, Arrays.asList(1));
            assertEquals("0[1,2,3] * 3[4,1,0,5,6] * 8[7,8,9] * 11[10,11,12] * 14[13,14,15]", extract(list));
            
            list.addAll(4, Arrays.asList(2));
            assertEquals("0[1,2,3] * 3[4,2] * 5[1,0,5,6] * 9[7,8,9] * 12[10,11,12] * 15[13,14,15]", extract(list));
        });
    }
    
    @Test
    void testAddAll2() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            list.addAll(3, Arrays.asList(9, 9, 9, 9, 9)); // insert new elements at start of a page
            assertEquals("0[1,2,3] * 3[9,9,9] * 6[9,9] * 8[4,5,6] * 11[7,8,9] * 14[10,11,12] * 17[13,14,15]", extract(list));
            
            list.addAll(9, Arrays.asList(8, 8, 8, 8)); // insert new elements at middle of a page
            assertEquals("0[1,2,3] * 3[9,9,9] * 6[9,9] * 8[4,8,8] * 11[8,8] * 13[5,6] * 15[7,8,9] * 18[10,11,12] * 21[13,14,15]", extract(list));
            
            list.addAll(24, Arrays.asList(7, 7, 7, 7)); // insert new elements at end
            assertEquals("0[1,2,3] * 3[9,9,9] * 6[9,9] * 8[4,8,8] * 11[8,8] * 13[5,6] * 15[7,8,9] * 18[10,11,12] * 21[13,14,15] * 24[7,7,7] * 27[7]", extract(list));
        });
    }
    
    @Test
    void testRemove() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            list.remove(4);
            assertEquals("0[1,2,3] * 3[4,6] * 5[7,8,9] * 8[10,11,12] * 11[13,14,15]", extract(list));
            
            list.remove(4);
            assertEquals("0[1,2,3] * 3[4] * 4[7,8,9] * 7[10,11,12] * 10[13,14,15]", extract(list));
            
            list.remove(4);
            assertEquals("0[1,2,3] * 3[4] * 4[8,9] * 6[10,11,12] * 9[13,14,15]", extract(list));
            
            list.remove(4);
            assertEquals("0[1,2,3] * 3[4] * 4[9] * 5[10,11,12] * 8[13,14,15]", extract(list));
            
            list.remove(4);
            assertEquals("0[1,2,3] * 3[4] * 4[10,11,12] * 7[13,14,15]", extract(list));
        });
    }
    
    @Test
    void testSplice_removeEntireLastPage() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            PageList<Integer> splice = list.splice(4, 12);
            assertEquals("0[1,2,3] * 3[4] * 4[13,14,15]", extract(list));
            assertEquals("0[5,6] * 2[7,8,9] * 5[10,11,12]", extract(splice));
        });
    }
    
    @Test
    void testSplice_removeEntireFirstPage() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            PageList<Integer> splice = list.splice(3, 11);
            assertEquals("0[1,2,3] * 3[12] * 4[13,14,15]", extract(list));
            assertEquals("0[4,5,6] * 3[7,8,9] * 6[10,11]", extract(splice));
        });
    }
    
    @Test
    void testSplice_removeEntireFirstLastPage() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            PageList<Integer> splice = list.splice(3, 12);
            assertEquals("0[1,2,3] * 3[13,14,15]", extract(list));
            assertEquals("0[4,5,6] * 3[7,8,9] * 6[10,11,12]", extract(splice));
        });
    }
    
    @Test
    void testSplice_removePartialFirstLastPage() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            PageList<Integer> splice = list.splice(4, 11);
            assertEquals("0[1,2,3] * 3[4] * 4[12] * 5[13,14,15]", extract(list));
            assertEquals("0[5,6] * 2[7,8,9] * 5[10,11]", extract(splice));
        });
    }
    
    @Test
    void testRemoveRange_removePartialFirstLastPage() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            list.subList(4, 11).clear();
            assertEquals("0[1,2,3] * 3[4] * 4[12] * 5[13,14,15]", extract(list));
        });
    }
    
    @Test
    void testSplice_removeElementsInOnePage() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            PageList<Integer> splice = list.splice(3, 5);
            assertEquals("0[1,2,3] * 3[6] * 4[7,8,9] * 7[10,11,12] * 10[13,14,15]", extract(list));
            assertEquals("0[4,5]", extract(splice));
        });
    }
    
    @Test
    void testSplice_removeOnePage() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            PageList<Integer> splice = list.splice(3, 6);
            assertEquals("0[1,2,3] * 3[7,8,9] * 6[10,11,12] * 9[13,14,15]", extract(list));
            assertEquals("0[4,5,6]", extract(splice));
        });
    }
    
    @Test
    void testSplice_nothing() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));        
            PageList<Integer> splice = list.splice(3, 3);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));        
            assertEquals("", extract(splice));
        });
    }
    
    @Test
    void testSplice_error() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertException(() -> list.splice(4, 3),
                            IllegalArgumentException.class,
                            "startInclusive(4) should be less than endExclusive(3)");
        });
    }
        
    @Test
    @SuppressWarnings("checkstyle:LineLength")
    void testConstructors() {
        testAllLists(() -> {
            assertException(() -> constructPageList(3, 3), IllegalArgumentException.class, "preferredMaxPageSize(3) should be < maxPageSize(3)");
            assertException(() -> constructPageList(2, 5), IllegalArgumentException.class, "preferredMaxPageSize(2) should be >= 3");
            
            PageList<Integer> list = constructPageList();
            IntStream.range(1, 54).forEach(list::add);
            assertEquals("0[1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50] * 50[51,52,53]",
                         extract(list));
        });
    }
        
    @Test
    void testListIterator() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            {
                ListIterator<Integer> listIter = list.listIterator(13);
                
                assertEquals(13, listIter.nextIndex());
                assertEquals(14, listIter.next().intValue());
                
                assertEquals(14, listIter.nextIndex());
                assertEquals(15, listIter.next().intValue());
                
                assertEquals(15, listIter.nextIndex());
                assertFalse(listIter.hasNext());
                assertTrue(listIter.hasPrevious());
                assertException(() -> listIter.next(), NoSuchElementException.class);
            }        
            
            {
                ListIterator<Integer> listIter = list.listIterator(2);

                assertEquals(1, listIter.previousIndex());
                assertEquals(2, listIter.previous().intValue());
                
                assertEquals(0, listIter.previousIndex());
                assertEquals(1, listIter.previous().intValue());

                assertEquals(-1, listIter.previousIndex());
                assertFalse(listIter.hasPrevious());
                assertTrue(listIter.hasNext());
                assertException(() -> listIter.previous(), NoSuchElementException.class);
            }
        });
    }
    
    @Test
    void testListIteratorSet() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            ListIterator<Integer> listIter = list.listIterator(3);
            assertException(() -> listIter.set(40), IllegalStateException.class);
            listIter.next();
            listIter.set(40);
            assertEquals("0[1,2,3] * 3[40,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            listIter.set(400);
            assertEquals("0[1,2,3] * 3[400,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
        });
    }
    
    @Test
    void testListIteratorRemove() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            {
                ListIterator<Integer> listIter = list.listIterator(3);
                assertException(() -> listIter.remove(), IllegalStateException.class);
                
                assertEquals(4, listIter.next().intValue());
                listIter.remove();
                assertEquals("0[1,2,3] * 3[5,6] * 5[7,8,9] * 8[10,11,12] * 11[13,14,15]", extract(list));
                assertException(() -> listIter.remove(), IllegalStateException.class);
                
                assertEquals(5, listIter.next().intValue());
                listIter.remove();
                assertEquals("0[1,2,3] * 3[6] * 4[7,8,9] * 7[10,11,12] * 10[13,14,15]", extract(list));
                
                assertEquals(6, listIter.next().intValue());
                listIter.remove();
                assertEquals("0[1,2,3] * 3[7,8,9] * 6[10,11,12] * 9[13,14,15]", extract(list));
                
                assertEquals(3, listIter.nextIndex());
                assertEquals(7, listIter.next().intValue());
                listIter.remove();
                assertEquals("0[1,2,3] * 3[8,9] * 5[10,11,12] * 8[13,14,15]", extract(list));
            }
            
            {
                ListIterator<Integer> listIter = list.listIterator(8);
                
                assertEquals(12, listIter.previous().intValue());
                listIter.remove();
                assertEquals("0[1,2,3] * 3[8,9] * 5[10,11] * 7[13,14,15]", extract(list));
                
                assertEquals(11, listIter.previous().intValue());
                listIter.remove();
                assertEquals("0[1,2,3] * 3[8,9] * 5[10] * 6[13,14,15]", extract(list));
                
                assertEquals(10, listIter.previous().intValue());
                listIter.remove();
                assertEquals("0[1,2,3] * 3[8,9] * 5[13,14,15]", extract(list));
                
                assertEquals(5, listIter.nextIndex());
                assertEquals(9, listIter.previous().intValue());
                listIter.remove();
                assertEquals("0[1,2,3] * 3[8] * 4[13,14,15]", extract(list));
            }
        });
    }
    
    @Test
    void testListIteratorRemoveAdd() {
        testAllLists(() -> {
            List<Integer> normalList = new ArrayList<>();
            IntStream.range(1, 7).forEach(normalList::add);
            PageList<Integer> list = constructPageList(normalList, 3, 5);
            assertEquals("0[1,2,3] * 3[4,5,6]", extract(list));
            ListIterator<Integer> listIter = list.listIterator(4);
            
            assertEquals(4, listIter.previous().intValue());
            listIter.remove();
            assertEquals("0[1,2,3] * 3[5,6]", extract(list));
            
            assertEquals(3, listIter.previous().intValue());
            listIter.remove();
            assertEquals("0[1,2] * 2[5,6]", extract(list));

            assertEquals(2, listIter.previous().intValue());
            listIter.remove();
            assertEquals("0[1] * 1[5,6]", extract(list));

            assertEquals(1, listIter.previous().intValue());
            assertFalse(listIter.hasPrevious());
            listIter.remove();
            assertEquals("0[5,6]", extract(list));
            assertEquals(2, list.size());
            assertFalse(listIter.hasPrevious());
            
            listIter.add(10);
            assertEquals("0[10,5,6]", extract(list));
            assertTrue(listIter.hasPrevious());
            
            listIter.add(20);
            assertEquals("0[10,20,5,6]", extract(list));
            assertEquals(4, list.size());
            
            listIter.add(30);
            assertEquals("0[10,20,30,5,6]", extract(list));
            
            listIter.add(40);
            assertEquals("0[10,20,30,40] * 4[5,6]", extract(list));

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
            assertEquals("0[10,20,30,40] * 4[5]", extract(list));
            assertEquals(5, listIter.previous().intValue());
            listIter.remove();
            assertEquals("0[10,20,30,40]", extract(list));
            assertEquals(40, listIter.previous().intValue());
            listIter.remove();
            assertEquals("0[10,20,30]", extract(list));
            assertEquals(30, listIter.previous().intValue());
            listIter.remove();
            assertEquals("0[10,20]", extract(list));
            assertEquals(20, listIter.previous().intValue());
            listIter.remove();
            assertEquals("0[10]", extract(list));
            assertEquals(10, listIter.previous().intValue());
            listIter.remove();
            assertEquals("", extract(list));
            
            assertFalse(listIter.hasNext());
            assertFalse(listIter.hasPrevious());
            assertEquals(0, listIter.nextIndex());
            assertEquals(-1, listIter.previousIndex());
            assertException(() -> listIter.next(), NoSuchElementException.class);
            assertException(() -> listIter.previous(), NoSuchElementException.class);
            assertException(() -> listIter.remove(), IllegalStateException.class);
                   
            listIter.add(100);
            assertEquals("0[100]", extract(list));
            
            listIter.add(200);
            assertEquals("0[100,200]", extract(list));
            
            listIter.add(300);
            assertEquals("0[100,200,300]", extract(list));
            
            listIter.add(400);
            assertEquals("0[100,200,300] * 3[400]", extract(list));
            
            listIter.add(500);
            assertEquals("0[100,200,300] * 3[400,500]", extract(list));
            
            assertException(() -> listIter.remove(), IllegalStateException.class);
            assertEquals("0[100,200,300] * 3[400,500]", extract(list));
            
            assertEquals(500, listIter.previous().intValue());
        });
    }
        
    @Test
    void testListIteratorAdd() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            ListIterator<Integer> listIter = list.listIterator(3);
            listIter.add(99);
            assertEquals("0[1,2,3] * 3[99,4,5,6] * 7[7,8,9] * 10[10,11,12] * 13[13,14,15]", extract(list));
            
            listIter.add(98);
            assertEquals("0[1,2,3] * 3[99,98,4,5,6] * 8[7,8,9] * 11[10,11,12] * 14[13,14,15]", extract(list));
            
            listIter.add(97);
            assertEquals("0[1,2,3] * 3[99,98,97] * 6[4,5,6] * 9[7,8,9] * 12[10,11,12] * 15[13,14,15]", extract(list));
        });
    }
    
    @Test
    @SuppressWarnings("checkstyle:LineLength")
    void testConcurrentModification() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 3).forEach(list::add);
            
            {
                ListIterator<Integer> listIter = list.listIterator();
                list.add(11);
                assertException(() -> listIter.hasNext(), ConcurrentModificationException.class); // AbstractList.Itr.hasNext() does not throw
                assertException(() -> listIter.next(), ConcurrentModificationException.class);
                assertException(() -> listIter.remove(), ConcurrentModificationException.class); // AbstractList.Itr.remove() would throw IllegalStateException first
                assertException(() -> listIter.hasPrevious(), ConcurrentModificationException.class); // AbstractList.Itr.hasPrevious() does not throw
                assertException(() -> listIter.previous(), ConcurrentModificationException.class);
                assertException(() -> listIter.nextIndex(), ConcurrentModificationException.class); // AbstractList.Itr.nextIndex() does not throw
                assertException(() -> listIter.previousIndex(), ConcurrentModificationException.class); // AbstractList.Itr.previousIndex() does not throw
                assertException(() -> listIter.set(99), ConcurrentModificationException.class); // AbstractList.Itr.set(element) would throw IllegalStateException first
                assertException(() -> listIter.add(99), ConcurrentModificationException.class);
            }
        });
    }
        
    @Test
    void testRemoveIf() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));

            assertTrue(list.removeIf(val -> val % 2 == 0));
            assertEquals("0[1,3] * 2[5] * 3[7,9] * 5[11] * 6[13,15]", extract(list));
            
            assertFalse(list.removeIf(val -> val % 2 == 0));
            assertEquals("0[1,3] * 2[5] * 3[7,9] * 5[11] * 6[13,15]", extract(list));
        });        
    }
    
    @Test
    void testForEach() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            StringBuilder builder = new StringBuilder();
            list.forEach(val -> builder.append(val).append(' '));
            assertEquals("1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 ", builder.toString());
        });        
    }
    
    @Test
    void testReplaceAll() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            list.replaceAll(val -> val * 10);
            assertEquals("0[10,20,30] * 3[40,50,60] * 6[70,80,90] * 9[100,110,120] * 12[130,140,150]", extract(list));
        });        
    }
    
    @Test
    void testSort() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            
            list.sort((lhs, rhs) -> Integer.compare(rhs, lhs));
            assertEquals("0[15,14,13] * 3[12,11,10] * 6[9,8,7] * 9[6,5,4] * 12[3,2,1]", extract(list));
        });        
    }
    
    @Test
    void testSplitIterator() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,15]", extract(list));
            int expectedSum = list.stream().collect(Collectors.summingInt(val -> val));
            assertEquals(120, expectedSum);
            
            Spliterator<Integer> iter1 = list.spliterator();
            assertEquals(15, iter1.estimateSize());
            
            Spliterator<Integer> iter2 = iter1.trySplit();
            assertEquals(9, iter1.estimateSize());
            assertEquals(6, iter2.estimateSize());
            
            AtomicInteger sum = new AtomicInteger();
            
            Thread thread1 = new Thread(() -> {
                while (iter1.tryAdvance(val -> sum.addAndGet(val))) {
                    ;
                }
            });
            Thread thread2 = new Thread(() -> {
                while (iter2.tryAdvance(val -> sum.addAndGet(val))) {
                    ;
                }
            });
            
            thread1.start();
            thread2.start();
            
            try {
                thread1.join();
                thread2.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            
            assertEquals(expectedSum, sum.get());
        });        
    }

    @Test
    void testBinarySearch() {
        testAllLists(() -> {
            PageList<Integer> list = constructPageList(3, 5);
            IntStream.range(1, 16).forEach(list::add);
            list.set(14, 100);
            assertEquals("0[1,2,3] * 3[4,5,6] * 6[7,8,9] * 9[10,11,12] * 12[13,14,100]", extract(list));

            assertEquals(9, MoreCollections.binarySearch(list, 10));
            assertEquals(10, MoreCollections.binarySearch(list, 11));
            assertEquals(11, MoreCollections.binarySearch(list, 12));
            
            assertEquals(0, MoreCollections.binarySearch(list, 1));
            assertEquals(-1, MoreCollections.binarySearch(list, 0));
            assertEquals(-15, MoreCollections.binarySearch(list, 15));
            assertEquals(-16, MoreCollections.binarySearch(list, 999));
        });        
    }
    
    @Test
    @SuppressWarnings("checkstyle:Indentation") // to suppress complaints about arrayCreators below
    void testPerformance_BinarySearch() {
        System.out.println("\ntestPerformance_BinarySearch");
        
        List<Integer> arrayList = Collections.unmodifiableList(generateHugeSortedArrayList());
        Random random = new Random();
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            indices.add(random.nextInt(arrayList.size()));
        }
        
        List<Supplier<List<Integer>>> arrayCreators = Arrays.asList(() -> new ArrayList<>(arrayList),
                                                                    () -> new ArrayListPageList<>(arrayList),
                                                                    () -> new LinkedListPageList<>(arrayList));

        for (Supplier<List<Integer>> arrayCreator: arrayCreators) {
            List<Integer> list = arrayCreator.get();

            List<NamedFunction<Integer, Integer>> functions = Arrays.asList
                (
                    new NamedFunction<Integer, Integer>("Collections.binarySearch", val -> Collections.binarySearch(list, val)),
                    new NamedFunction<Integer, Integer>("MoreCollections.binarySearch", val -> MoreCollections.binarySearch(list, val))
                );

            for (NamedFunction<Integer, Integer> function: functions) {
                System.out.print(list.getClass().getSimpleName() + " + " + function.name() + " = ");
                final long startTime = System.currentTimeMillis();
                for (int index: indices) {
                    int val = arrayList.get(index);
                    int findIndex = function.function().apply(val);
                    assertEquals(index, findIndex);
                }
                final long timeTaken = System.currentTimeMillis() - startTime;
                System.out.println(timeTaken + "ms");
            }
        }
        
        /*
            testPerformance_BinarySearch
            ArrayList=33ms
            ArrayListPageList=131ms
         */
    }

    @Test
    void testPerformance_Remove() {
        System.out.println("\ntestPerformance_Remove");
        Random random = new Random();
        ArrayList<Integer> normalList = generateHugeSortedArrayList();
        ArrayListPageList<Integer> pageList = new ArrayListPageList<>(normalList);
        
        List<Integer> indices = new ArrayList<>();
        for (int i = 0; i < 50_000; i++) {
            indices.add(random.nextInt(normalList.size() - i));
        }
        
        for (List<Integer> list: Arrays.asList(normalList, pageList)) {
            int originalSize = list.size();
            final long startTime = System.currentTimeMillis();
            for (int index: indices) {
                list.remove(index);
            }
            assertEquals(originalSize - 50_000, list.size());
            final long endTime = System.currentTimeMillis();
            System.out.println(list.getClass().getSimpleName() + "=" + (endTime - startTime) + "ms");
        }

        /*
            testPerformance_Remove
            ArrayList=4557ms
            ArrayListPageList=1367ms
         */
    }
            
    @Test
    void testSerialization() {
        testAllLists(() -> {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            
            {
                PageList<Integer> list = constructPageList(3, 5);
                IntStream.range(1, 7).forEach(list::add);
                list.add(1, 99);
                assertEquals("0[1,99,2,3] * 4[4,5,6]", extract(list));
                
                try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
                    oos.writeObject(list);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
            
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bos.toByteArray()))) {
                @SuppressWarnings("unchecked")
                PageList<Integer> list = (PageList<Integer>) ois.readObject();
                assertEquals("0[1,99,2,3] * 4[4,5,6]", extract(list));
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }
    
    
    // helpers
    
    private static final Field pagesField;
    private static final Class<?> pageClass;
    private static final Field startIndexField;
    private static final Field listField;
    
    static {
        try {
            pagesField = AbstractPageList.class.getDeclaredField("pages");
            pageClass = find(AbstractPageList.class.getDeclaredClasses(), "Page");
            startIndexField = pageClass.getDeclaredField("startIndex");
            listField = pageClass.getDeclaredField("list");
            
            pagesField.setAccessible(true);
            startIndexField.setAccessible(true);
            listField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    private static Class<?> find(Class<?>[] declaredClasses, String findName) {
        for (Class<?> clazz: declaredClasses) {
            if (clazz.getSimpleName().equals(findName)) {
                return clazz;
            }
        }
        throw new NoSuchElementException();
    }
        
    @SuppressWarnings("unchecked")
    private static String extract(PageList<Integer> arrayPageList) {
        try {
            List<Object> pages = (List<Object>) pagesField.get(arrayPageList);
            StringJoiner result = new StringJoiner(" * ");
            int size = 0;
            for (Object page: pages) {
                List<Object> list = (List<Object>) listField.get(page);
                assertTrue(!list.isEmpty(), "page must not be empty");
                size += list.size();
                result.add(page.toString().replaceAll(" ",  ""));
            }
            assertEquals(size, arrayPageList.size(), "comparing size");
            return result.toString();
        } catch (IllegalArgumentException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
    
    private static void assertEqualsByGetAndIterAndStream(String expected, PageList<Integer> list) {
        assertEquals(expected, listToStringByGet(list));
        assertEquals(expected, listToStringByIter(list));
        assertEquals(expected, listToStringByStream(list));
    }
    
    private static String listToStringByGet(List<Integer> list) {
        StringJoiner result = new StringJoiner(",");
        for (int i = 0; i < list.size(); i++) {
            result.add(Integer.toString(list.get(i)));
        }
        return result.toString();
    }
    
    private static String listToStringByIter(List<Integer> list) {
        StringJoiner result = new StringJoiner(",");
        for (int elem: list) {
            result.add(Integer.toString(elem));
        }
        return result.toString();
    }
    
    private static String listToStringByStream(List<Integer> list) {
        return list.stream().map(val -> Integer.toString(val)).collect(Collectors.joining(","));
    }
    
    private static ArrayList<Integer> generateHugeSortedArrayList() {
        final int size = 1_000_000;
        ArrayList<Integer> output = new ArrayList<>(size);
        Random random = new Random();
        int val = 0;
        for (int i = 0; i < size; i++) {
            val += random.nextInt(8) + 1;
            output.add(val);
        }
        return output;
    }

    private record NamedFunction<T, R>(String name, Function<T, R> function) {
    }
}
