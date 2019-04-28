package myutils.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import myutils.util.AbstractPageList.Page;


@SuppressWarnings({"checkstyle:NeedBraces", "checkstyle:"})
public class MoreCollections {
    
    /**
     * An extension of Collection.binarySearch that searches in a subarray for the specified element.
     * This function also takes an extractor that specifies how to map each element in the array
     * to a new element. For example, if you had a list of contacts sorted by age and you wanted
     * to find a contact with age N, the key would be N and the extract would be Contact::getAge.
     * 
     * <p>This implementation is not specialized for PageList.
     * 
     * @param <T> the type of element in the list
     * @param <U> the type of element we are extracting from T, or the type of element we are reducing T to
     * @param list the list to search, must be sorted by U
     * @param startInclusive the start index of the subarray
     * @param endExclusive the end index of the subarray
     * @param extractor a function for getting a value U from an element in the original list, which is a list type T
     * @param key the value of type U to search for
     * @return the index of the element in the list, or a negative number indicating the closest element
     */
    public static <T, U extends Comparable<? super U>>
    int binarySearch(List<T> list,
                     int startInclusive,
                     int endExclusive,
                     Function<T, U> extractor,
                     U key) {
        int low = startInclusive;
        int high = endExclusive - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            U midVal = extractor.apply(list.get(mid));
            int cmp = midVal.compareTo(key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        
        return -(low + 1); 
    }

    /**
     * Same Javadoc as the binarySearch of 5 arguments in this file.
     * 
     * <p>Use this function when you are looking for an int instead of Integer.
     */
    public static <T>
    int binarySearchInt(List<T> list,
                        int startInclusive,
                        int endExclusive,
                        ToIntFunction<T> extractor,
                        int key) {
        int low = startInclusive;
        int high = endExclusive - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            int midVal = extractor.applyAsInt(list.get(mid));
            int cmp = Integer.compare(midVal, key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
        }
        
        return -(low + 1); 
    }

    /**
     * Same Javadoc as the binarySearch of 5 arguments in this file.
     * 
     * <p>Use this function when you are looking for a long instead of Long.
     */
    public static <T>
    int binarySearchLong(List<T> list,
                         int startInclusive,
                         int endExclusive,
                         ToLongFunction<T> extractor,
                         int key) {
        int low = startInclusive;
        int high = endExclusive - 1;

        while (low <= high) {
            int mid = (low + high) >>> 1;
            long midVal = extractor.applyAsLong(list.get(mid));
            int cmp = Long.compare(midVal, key);

            if (cmp < 0)
                low = mid + 1;
            else if (cmp > 0)
                high = mid - 1;
            else
                return mid;
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
        if (list instanceof AbstractPageList)
            return pageListBinarySearch((AbstractPageList<U>) list, key);
        else
            return Collections.binarySearch(list, key);
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
        int index = binarySearch(pages, 0, pages.size(), page -> page.getList().get(0), key);
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
                if (subIndex >= 0)
                    return page.getStartIndex() + subIndex;
                else
                    return -(page.getStartIndex() + (-subIndex - 1)) - 1;
            }
        }
    }       
}
