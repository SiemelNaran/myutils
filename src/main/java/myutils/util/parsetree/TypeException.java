package myutils.util.parsetree;


public class TypeException extends RuntimeException {
	private static final long serialVersionUID = 1L;

    public static void assertSameType(String name, int position, Class<?> first, Class<?> second) {
        if (!first.equals(second)) {
            throw new TypeException("arguments to " + name + " at position " + position + " are not of the same type");
        }
    }

    public TypeException(String message) {
		super(message);
	}
}
