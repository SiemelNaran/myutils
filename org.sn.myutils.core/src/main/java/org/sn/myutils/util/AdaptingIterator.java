package org.sn.myutils.util;

import java.util.Iterator;
import java.util.function.Function;


/**
 * Adapt an iterator that returns items of type T to items of type R using a supplied mapping function.
 *
 * <p>In general, prefer to use streams with the map function, like <code>list.stream().map(val -> val * val)</code>.
 * This class is only necessary when an iterator is needed, or one needs to call the iterator's remove function.
 */
public class AdaptingIterator<T, R> implements Iterator<R> {
    private final Iterator<T> iter;
    private final Function<T,R> mapper;
    private final boolean allowRemove;

    public AdaptingIterator(Iterator<T> iter, Function<T, R> mapper, boolean allowRemove) {
        this.iter = iter;
        this.mapper = mapper;
        this.allowRemove = allowRemove;
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public R next() {
        return mapper.apply(iter.next());
    }

    @Override
    public void remove() {
        if (allowRemove) {
            iter.remove();
        } else {
            throw new UnsupportedOperationException("remove");
        }
    }
}
