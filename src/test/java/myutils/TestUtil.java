package myutils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.concurrent.Callable;


public class TestUtil {
    
    @SuppressWarnings("unchecked")
    public static <T, U> U assertExceptionFromCallable(Callable<T> callable, Class<U> expectedException) {
        try {
            callable.call();
            fail("Expected exception " + expectedException.getSimpleName() + ", but got no exception");
            throw new AssertionError();
        } catch (Exception e) {
            assertTrue("Expected " + expectedException.getSimpleName() + " or an exception derived from it, "
                           + "but got " + e.getClass().getSimpleName(),
                       expectedException.isInstance(e));;
            return (U) e;
        }
    }
    
    public static <T, U> void assertExceptionFromCallable(Callable<T> callable, Class<U> expectedException, String expectedMessage) {
        try {
            callable.call();
            fail("Expected exception " + expectedException.getSimpleName() + ", but got no exception");
        } catch (Exception e) {
            assertTrue("Expected " + expectedException.getSimpleName() + " or an exception derived from it, "
                           + "but got " + e.getClass().getSimpleName(),
                       expectedException.isInstance(e));;
            assertEquals(expectedMessage, e.getMessage());
        }
    }
    
    @SuppressWarnings("unchecked")
    public static <T, U> U assertException(Runnable runnable, Class<U> expectedException) {
        try {
            runnable.run();
            fail("Expected exception " + expectedException.getSimpleName() + ", but got no exception");
            throw new AssertionError();
        } catch (RuntimeException e) {
            assertTrue("Expected " + expectedException.getSimpleName() + " or an exception derived from it, "
                           + "but got " + e.getClass().getSimpleName(),
                       expectedException.isInstance(e));
            return (U) e;
        }
    }
    
    public static <T, U> void assertException(Runnable runnable, Class<U> expectedException, String expectedMessage) {
        try {
            runnable.run();
            fail("Expected exception " + expectedException.getSimpleName() + ", but got no exception");
        } catch (RuntimeException e) {
            assertTrue("Expected " + expectedException.getSimpleName() + " or an exception derived from it, "
                           + "but got " + e.getClass().getSimpleName(),
                       expectedException.isInstance(e));;
            assertEquals(expectedMessage, e.getMessage());
        }
    }
}
