package myutils.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import java.util.function.ToLongFunction;

import myutils.util.AbstractPageList.Page;


@SuppressWarnings("checkstyle:NeedBraces")
public class MoreCollections {
    
    // binary search handling PageList
    
    /**
     * Binary search that provides a more efficient implementation for PageList.
     */
    public static <U extends Comparable<? super U>>
    int binarySearch(List<U> list,
                     U key){
        if (list instanceof AbstractPageList)
            return pageListBinarySearch((AbstractPageList<U>) list, key);
        else
            return Collections.binarySearch(list, key);
    }

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
    
    
    // binarySearch with extractor, not handling PageList

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
}
