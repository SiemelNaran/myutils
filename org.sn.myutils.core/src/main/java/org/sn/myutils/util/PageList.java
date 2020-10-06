package org.sn.myutils.util;

import java.util.List;

/**
 * Class representing an array as an array of pages.
 * 
 * <p>Removing an element from a page list is fast because we only need to remove
 * the element from one page and update the indices of the subsequent pages.
 * 
 * <p>Similarly, adding an element to a page is fast.
 *  
 * <p>Fetching an element by index via a call to {@code get(int index)} is slower than
 * in ArrayList because we have to first look up the page that contains the element
 * at that index, then look up the element in that page.
 * 
 * <p>This class extends List but adds a splice function.
 * 
 * @author snaran
 *
 * @param <E> the type of the element stored in the page list
 */
public interface PageList<E> extends List<E> {
    PageList<E> splice(int startInclusive, int endExclusive);
}
