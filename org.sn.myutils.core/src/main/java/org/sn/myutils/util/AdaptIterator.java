package org.sn.myutils.util;

import java.util.Iterator;
import java.util.function.Function;


public class AdaptIterator<T, R> implements Iterator<R> {
    private final Iterator<T> iter;
    private final Function<T,R> mapper;

    public AdaptIterator(Iterator<T> iter, Function<T, R> mapper) {
        this.iter = iter;
        this.mapper = mapper;
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
        iter.remove();
    }
}
