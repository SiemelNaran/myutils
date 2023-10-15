package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.EmptyStackException;
import org.junit.jupiter.api.Test;


public class MaxSizeStackTest {
    @Test
    void test() {
        var maxSizeStack = new MaxSizeStack<Integer>(3);
        assertTrue(maxSizeStack.isEmpty());
        assertThrows(EmptyStackException.class, maxSizeStack::peek);

        maxSizeStack.push(1);
        assertEquals(1, maxSizeStack.size());
        assertEquals(1, maxSizeStack.peek());
        maxSizeStack.push(2);
        assertEquals(2, maxSizeStack.size());
        assertEquals(2, maxSizeStack.peek());
        maxSizeStack.push(3);
        assertEquals(3, maxSizeStack.size());
        assertEquals(3, maxSizeStack.peek());
        maxSizeStack.push(4);
        assertEquals(3, maxSizeStack.size());
        assertEquals(4, maxSizeStack.peek());

        assertEquals(4, maxSizeStack.pop());
        assertEquals(2, maxSizeStack.size());
        assertEquals(3, maxSizeStack.pop());
        assertEquals(1, maxSizeStack.size());
        assertEquals(2, maxSizeStack.pop());
        assertEquals(0, maxSizeStack.size());
        assertThrows(EmptyStackException.class, maxSizeStack::pop);
    }
}
