package org.sn.myutils.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;
import org.sn.myutils.util.AbstractPageList.Page;


public class MoreCollections {
    private MoreCollections() {
    }
    
    public enum FindWhich {
        FIND_FIRST,
        FIND_ANY,
        FIND_LAST,
    }
    
    /**
     * An extension of Collection.binarySearch that searches in a subarray for the specified element.
     * We could use Collections.sort on a list.sublist(start, end) to search a subarray,
     * but there is an extra level of indirection,
     * and the indices of the sublist start at zero and don't match the index in the full array,
     * which may at times lead to awkward code.
     * 
     * <p>This function also takes an extractor that specifies how to map each element in the array
     * to a new element. For example, if you had a list of contacts sorted by age, and you wanted
     * to find a contact with age N, the key would be N and the extract would be Contact::getAge.
     * 
     * <p>This implementation is not specialized for PageList.
     * 
     * <p>Running time: O(lg(N)).
     * 
     * @param <T> the type of element in the list
     * @param <U> the type of element we are extracting from T, or the type of element we are reducing T to. Must implement Comparable.
     * @param list the list to search, must be sorted by U
     * @param startInclusive the start index of the subarray
     * @param endExclusive the end index of the subarray
     * @param extractor a function for getting a value U from an element in the original list, which is a list type T
     * @param key the value of type U to search for
     * @param findWhich if there are many elements with the same `key` this parameter specifies which one we want to find.
     *        FIND_ANY means find any value, which will likely be the middle value. It is what Collections.binarySearch does and offers the best performance.
     *        FIND_FIRST means find the first (or smallest index) with this value.
     *        FIND_LAST means find the last (or largest index) with this value.
     * @return the index of the element in the list, or a negative number indicating the closest element
     */
    public static <T, U extends Comparable<? super U>>
    int binarySearch(List<T> list,
                     int startInclusive,
                     int endExclusive,
                     Function<T, U> extractor,
                     U key,
                     FindWhich findWhich) {
        int low = startInclusive;
        int high = endExclusive - 1;
        Integer best = null; // used when findWhich is FIND_FIRST or FIND_LAST

        while (low <= high) {
            int mid = (low + high) >>> 1;
            U midVal = extractor.apply(list.get(mid));
            int cmp = midVal.compareTo(key);

            if (cmp < 0) {
                // look in right half
                low = mid + 1;
            } else if (cmp > 0) {
                // look in left half
                high = mid - 1;
            } else {
                switch (findWhich) {
                    case FIND_LAST -> {
                        // look in right half
                        best = mid;
                        low = mid + 1;
                    }
                    case FIND_FIRST -> {
                        // look in left half
                        best = mid;
                        high = mid - 1;
                    }
                    default -> {
                        return mid;
                    }
                }
            }
        }
        
        if (best != null) {
            return best;
        }
        return -(low + 1); 
    }

    /**
     * Same Javadoc as the binarySearch of 6 arguments in this file.
     * 
     * <p>Use this function when you are looking for an int instead of Integer.
     */
    public static <T>
    int binarySearchInt(List<T> list,
                        int startInclusive,
                        int endExclusive,
                        ToIntFunction<T> extractor,
                        int key,
                        FindWhich findWhich) {
        int low = startInclusive;
        int high = endExclusive - 1;
        Integer best = null; // used when findWhich is FIND_FIRST or FIND_LAST

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = extractor.applyAsInt(list.get(mid));
            int cmp = Integer.compare(midVal, key);

            if (cmp < 0) {
                // look in right half
                low = mid + 1;
            } else if (cmp > 0) {
                // look in left half
                high = mid - 1;
            } else {
                switch (findWhich) {
                    case FIND_LAST -> {
                        // look in right half
                        best = mid;
                        low = mid + 1;
                    }
                    case FIND_FIRST -> {
                        // look in left half
                        best = mid;
                        high = mid - 1;
                    }
                    default -> {
                        return mid;
                    }
                }
            }
        }
        
        if (best != null) {
            return best;
        }
        return -(low + 1); 
    }

    /**
     * Same Javadoc as the binarySearch of 6 arguments in this file.
     * 
     * <p>Use this function when you are looking for a long instead of Long.
     */
    public static <T>
    int binarySearchLong(List<T> list,
                         int startInclusive,
                         int endExclusive,
                         ToLongFunction<T> extractor,
                         int key,
                         FindWhich findWhich) {
        int low = startInclusive;
        int high = endExclusive - 1;
        Integer best = null; // used when findWhich is FIND_FIRST or FIND_LAST

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = extractor.applyAsLong(list.get(mid));
            int cmp = Long.compare(midVal, key);

            if (cmp < 0) {
                // look in right half
                low = mid + 1;
            } else if (cmp > 0) {
                // look in left half
                high = mid - 1;
            } else {
                switch (findWhich) {
                    case FIND_LAST -> {
                        // look in right half
                        best = mid;
                        low = mid + 1;
                    }
                    case FIND_FIRST -> {
                        // look in left half
                        best = mid;
                        high = mid - 1;
                    }
                    default -> {
                        return mid;
                    }
                }
            }
        }
        
        if (best != null) {
            return best;
        }
        return -(low + 1); 
    }

    /**
     * Binary search that provides a more efficient implementation for PageList
     * if the list is an AbstractPageList.
     * 
     * @param list a List
     * @param key the key to search for
     * @return the index of the element in the list, or a negative number indicating the closest element
     */
    @SuppressWarnings("checkstyle:OverloadMethodsDeclarationOrder")
    public static <U extends Comparable<? super U>>
    int binarySearch(List<U> list,
                     U key) {
        if (list instanceof AbstractPageList<U> abstractPageList) {
            return pageListBinarySearch(abstractPageList, key);
        } else {
            return Collections.binarySearch(list, key);
        }
    }

    /**
     * Binary search that provides a more efficient implementation for PageList.
     * 
     * @param list an AbstractPageList
     * @param key the key to search for
     * @return the index of the element in the list, or a negative number indicating the closest element
     */
    public static <U extends Comparable<? super U>>
    int pageListBinarySearch(AbstractPageList<U> list,
                             U key) {
        ArrayList<Page<U>> pages = list.getPages();
        int index = binarySearch(pages, 0, pages.size(), page -> page.getList().get(0), key, FindWhich.FIND_ANY);
        if (index >= 0) {
            Page<U> page = pages.get(index);
            return page.getStartIndex();
        } else {
            if (index == -1) {
                return -1;
            } else {
                index = -index - 2;
                Page<U> page = pages.get(index);
                int subIndex = Collections.binarySearch(page.getList(), key);
                if (subIndex >= 0) {
                    return page.getStartIndex() + subIndex;
                } else {
                    return -(page.getStartIndex() + (-subIndex - 1)) - 1;
                }
            }
        }
    }
    
    
    /**
     * Given a sorted deque and an element that is most likely larger than the largest element in the list,
     * add the element to the list so that the list is still sorted.
     * The algorithm scans from the last element to the first to find the right place to insert the new element,
     * so the worst case running time is O(N).
     * However, the best case running time is O(1). 
     * 
     * @param <U> the type of elements in the collection
     * @param sortedList a list sorted by the given comparator
     * @param comparator a comparator to compare two values of type U
     * @param newValue the value to add
     */
    public static <U> void addLargeElementToSortedList(LinkedList<U> sortedList, Comparator<? super U> comparator, U newValue) {
        for (ListIterator<U> iter = sortedList.listIterator(sortedList.size()); iter.hasPrevious(); ) {
            U value = iter.previous();
            if (comparator.compare(newValue, value) >= 0) {
                iter.next();
                iter.add(newValue);
                return;
            }
        }
        sortedList.add(0, newValue);
    }
}
