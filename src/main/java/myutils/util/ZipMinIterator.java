package myutils.util;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;


/**
 * Return an iterator that takes collection of collection of values, and each call to next() returns the next highest value.
 * Each collection is assumed to be sorted.
 *
 * @param <T> the type of element in each collection
 */
public class ZipMinIterator<T> implements Iterator<T> {
    private PriorityQueue<ValueAndLocation<T>> elements = new PriorityQueue<>();

    public ZipMinIterator(Collection<? extends Collection<T>> collections, Comparator<? super T> comparator) {
        this.elements = new PriorityQueue<ValueAndLocation<T>>((lhs, rhs) -> comparator.compare(lhs.value, rhs.value));
        fillElements(collections);
    }
    
    private void fillElements(Collection<? extends Collection<T>> collections) {
        for (var collection : collections) {
            Iterator<T> iter = collection.iterator();
            if (iter.hasNext()) {
                elements.offer(new ValueAndLocation<T>(iter));
            }
        }
    }

    /**
     * Tell if there are any more elements.
     * Running time: O(1).
     */
    @Override
    public boolean hasNext() {
        return elements.size() > 0;
    }

    /**
     * Retrieve the next element and advance the internal iterator to the next highest element.
     * Running time: O(lg(N)).
     */
    @Override
    public T next() {
        ValueAndLocation<T> result = elements.poll();
        var iter = result.location;
        if (iter.hasNext()) {
            elements.offer(new ValueAndLocation<T>(iter));
        }
        return result.value;
    }
    
    private static class ValueAndLocation<T> {
        private final Iterator<T> location;
        private final T value;
        
        private ValueAndLocation(Iterator<T> location) {
            this.location = location;
            this.value = location.next();
        }
    }
}
