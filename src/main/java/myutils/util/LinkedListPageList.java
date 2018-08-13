package myutils.util;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.RandomAccess;

import javax.annotation.concurrent.NotThreadSafe;


/**
 * {@inheritDoc}
 * 
 * Class representing an array as an array of pages, where is page is an LinkedList.<p>
 * 
 * Fetching an element by index via a call to {@code get(int index)} is O(lg(N) + pageSize).<p>
 * 
 * This class implements java.io.Serializable.<p>
 * 
 * @author snaran
 */
@NotThreadSafe
public class LinkedListPageList<E> extends AbstractPageList<E> implements PageList<E>, RandomAccess, Serializable {
    private static final long serialVersionUID = 1L;

    LinkedListPageList() {
        super();
    }

    LinkedListPageList(Collection<E> elements) {
        super(elements);
    }
    
    LinkedListPageList(int preferredMaxPageSize, int maxPageSize) {
        super(Collections.emptyList(), preferredMaxPageSize, maxPageSize);
    }

    LinkedListPageList(Collection<E> elements, int preferredMaxPageSize, int maxPageSize) {
        super(elements, preferredMaxPageSize, maxPageSize);
    }
    
    @Override
    protected Page<E> createNewEmptyPage(int startIndex) {
        return new LinkedListPage<E>(startIndex);
    }

    @Override
    protected Page<E> createNewPageByCopyingSublist(int startIndex, List<E> subListToCopy) {
        return new LinkedListPage<E>(startIndex, new LinkedList<E>(subListToCopy));
    }

    @Override
    protected LinkedListPageList<E> createNewPageList() {
        return new LinkedListPageList<E>(preferredMaxPageSize, maxPageSize);
    }

    @Override
    public LinkedListPageList<E> splice(int startInclusive, int endExclusive) {
        return (LinkedListPageList<E>) super.splice(startInclusive, endExclusive);
    }
    
    private void writeObject(ObjectOutputStream output) throws IOException {
        output.defaultWriteObject();
        super.finalDefaultWriteAbstractPageList(output);
    }

    private void readObject(ObjectInputStream input) throws IOException, ClassNotFoundException{
        input.defaultReadObject();
        super.finalDefaultReadAbstractPageList(input);
    }

    protected static class LinkedListPage<E> extends Page<E> implements Serializable {
        private static final long serialVersionUID = 1L;
        
        private LinkedListPage(int startIndex) {
            super(startIndex, new LinkedList<E>());
        }
        
        private LinkedListPage(int startIndex, List<E> list) {
            super(startIndex, list);
        }
    }
}
