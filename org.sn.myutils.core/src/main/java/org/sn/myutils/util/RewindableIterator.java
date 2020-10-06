package org.sn.myutils.util;

import java.util.Iterator;


/**
 * Class that adapts iterator to store the iteration index,
 * and also provides a rewind function to go back one step.
 * 
 * @param <E> the type of element returned by this iterator
 * 
 * @author snaran
 */
public class RewindableIterator<E> implements Iterator<E> {
    public static <E> RewindableIterator<E> from(Iterator<E> iter) {
        return new RewindableIterator<E>(iter);
    }

    private final Iterator<E> iter;
    private int nextIndex;
    private E last;
    private boolean isRewind = false;
    
    private RewindableIterator(Iterator<E> iter) {
        this.iter = iter;
    }
    
    @Override
    public boolean hasNext() {
        return isRewind || iter.hasNext();
    }

    @Override
    public E next() {
        E next;
        if (!isRewind) {
            next = iter.next();
            last = next;
        } else {
            next = last;
            isRewind = false;
        }
        nextIndex++;
        return next;
    }
    
    public int getNextIndex() {
        return nextIndex;
    }

    /**
     * Rewind the iterator one step.
     * 
     * @throws IllegalStateException if you rewind twice
     */
    public void rewind() {
        if (isRewind) {
            throw new IllegalStateException("cannot rewind twice");
        }
        isRewind = true;
        if (--nextIndex < 0) {
            throw new IllegalStateException("cannot rewind new iterator");
        }
    }
}
