package org.sn.myutils.util;

import java.util.EmptyStackException;

/**
 * Stack of a maximum size.
 *
 * <p>One could use a LinkedList, and pop the front element when we push an element onto a stack which is at its
 * maximum size. This has the overhead of linked lists (more memory and performance), but should be ok in most cases.
 */
public class MaxSizeStack<T> {
    private final T[] elements;
    private int size = 0;
    private int next = -1;

    @SuppressWarnings("unchecked")
    public MaxSizeStack(int size) {
        elements = (T[]) new Object[size];
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
