package org.sn.myutils.util;

import java.util.EmptyStackException;

/**
 * Stack of a maximum size.
 *
 * <p>One could use a LinkedList, and pop the front element when we push an element onto a stack which is at its
 * maximum size. This has the overhead of linked lists (more memory and performance), but should be ok in most cases.
 *
 * <p>This implementation uses an array of fixed size, so will occupy lots of memory if the maxSize is large,
 * there are many MaxSizeStack's, and most have none to few elements in them.
 */
public class MaxSizeStack<T> {
    private final T[] elements;
    private int size = 0;
    private int next = -1;

    @SuppressWarnings("unchecked")
    public MaxSizeStack(int  maxSize) {
        elements = (T[]) new Object[maxSize];
    }

    public boolean isEmpty() {
        return size == 0;
    }

    public int size() {
        return size;
    }

    public void push(T val) {
        if (++next == elements.length) {
            next = 0;
        }
        elements[next] = val;
        if (size < elements.length) {
            size++;
        }
    }

    public T pop() {
        var result = peek();
        if (--next < 0) {
            next = elements.length - 1;
        }
        size--;
        return result;
    }

    public T peek() {
        if (size == 0) {
            throw new EmptyStackException();
        }
        return elements[next];
    }
}
