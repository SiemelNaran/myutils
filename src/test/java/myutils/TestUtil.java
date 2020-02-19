package myutils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.Callable;


public class TestUtil {

    /**
     * Assert that the desired exception is thrown.
     * 
     * @return the exception (not null)
     * @throws AssertionError if assertion fails
     */
    @SuppressWarnings("unchecked")
    public static <T, U> U assertExceptionFromCallable(Callable<T> callable, Class<U> expectedException) {
        try {
            callable.call();
            fail("Expected exception " + expectedException.getSimpleName() + ", but got no exception");
            throw new AssertionError();
        } catch (Exception e) {
            assertTrue(expectedException.isInstance(e), "Expected " + expectedException.getSimpleName()
                    + " or an exception derived from it, " + "but got " + e.getClass().getSimpleName());
            return (U) e;
        }
    }

    /**
     * Assert that the desired exception is thrown.
     * 
     * @return the exception (not null)
     * @throws AssertionError if assertion fails
     */
    @SuppressWarnings("unchecked")
    public static <T, U> U assertExceptionFromCallable(Callable<T> callable, Class<U> expectedException,
            String expectedMessage) {
        try {
            callable.call();
            fail("Expected exception " + expectedException.getSimpleName() + ", but got no exception");
            throw new AssertionError();
        } catch (Exception e) {
            assertTrue(expectedException.isInstance(e), "Expected " + expectedException.getSimpleName()
                    + " or an exception derived from it, " + "but got " + e.getClass().getSimpleName());
            assertEquals(expectedMessage, e.getMessage());
            return (U) e;
        }
    }

    /**
     * Assert that the desired exception is thrown.
     * 
     * @return the exception (not null)
     * @throws AssertionError if assertion fails
     */
    @SuppressWarnings("unchecked")
    public static <U> U assertException(Runnable runnable, Class<U> expectedException) {
        try {
            runnable.run();
            fail("Expected exception " + expectedException.getSimpleName() + ", but got no exception");
            throw new AssertionError();
        } catch (RuntimeException e) {
            assertTrue(expectedException.isInstance(e), "Expected " + expectedException.getSimpleName()
                    + " or an exception derived from it, " + "but got " + e.getClass().getSimpleName());
            return (U) e;
        }
    }

    /**
     * Assert that the desired exception is thrown.
     * 
     * @return the exception (not null)
     * @throws AssertionError if assertion fails
     */
    @SuppressWarnings("unchecked")
    public static <U> U assertException(Runnable runnable, Class<U> expectedException, String expectedMessage) {
        try {
            runnable.run();
            fail("Expected exception " + expectedException.getSimpleName() + ", but got no exception");
            throw new AssertionError();
        } catch (RuntimeException e) {
            assertTrue(expectedException.isInstance(e), "Expected " + expectedException.getSimpleName()
                    + " or an exception derived from it, " + "but got " + e.getClass().getSimpleName());
            assertEquals(expectedMessage, e.getMessage());
            return (U) e;
        }
    }
}
