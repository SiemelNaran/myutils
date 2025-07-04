package org.sn.myutils.util;

import java.io.Serial;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.RandomAccess;
import org.sn.myutils.annotations.NotThreadSafe;


/**
 * {@inheritDoc}
 * 
 * <p>Class representing an array as an array of pages, where is page is an ArrayList.
 * 
 * <p>Fetching an element by index via a call to {@code get(int index)} is O(lg(N)).
 */
@NotThreadSafe
public class ArrayListPageList<E> extends AbstractPageList<E> implements PageList<E>, RandomAccess, Serializable {

    @Serial
    private static final long serialVersionUID = 1L;

    ArrayListPageList() {
        super();
    }

    ArrayListPageList(Collection<E> elements) {
        super(elements);
    }
    
    ArrayListPageList(int preferredMaxPageSize, int maxPageSize) {
        super(Collections.emptyList(), preferredMaxPageSize, maxPageSize);
    }

    ArrayListPageList(Collection<E> elements, int preferredMaxPageSize, int maxPageSize) {
        super(elements, preferredMaxPageSize, maxPageSize);
    }
    
    @Override
    protected Page<E> createNewEmptyPage(int startIndex) {
        return new Page<>(startIndex, new ArrayList<E>());
    }

    @Override
    protected Page<E> createNewPageByCopyingSublist(int startIndex, List<E> subListToCopy) {
        return new Page<>(startIndex, new ArrayList<>(subListToCopy));
    }

    @Override
    protected ArrayListPageList<E> createNewPageList() {
        return new ArrayListPageList<>(preferredMaxPageSize, maxPageSize);
    }

    @Override
    public ArrayListPageList<E> splice(int startInclusive, int endExclusive) {
        return (ArrayListPageList<E>) super.splice(startInclusive, endExclusive);
    }
}
