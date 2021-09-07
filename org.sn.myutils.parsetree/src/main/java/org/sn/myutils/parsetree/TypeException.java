package org.sn.myutils.parsetree;


import java.io.Serial;

public class TypeException extends RuntimeException {
    @Serial
    private static final long serialVersionUID = 1L;

    /**
     * Assert that first and second are the same.
     * 
     * @throws TypeException if the types are not the same
     */
    public static void assertSameType(String name, int position, Class<?> first, Class<?> second) {
        if (!first.equals(second)) {
            throw new TypeException("arguments to " + name + " at position " + position + " are not of the same type");
        }
    }

    public TypeException(String message) {
        super(message);
    }
}
