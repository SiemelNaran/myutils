package myutils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.jupiter.params.ParameterizedTest;


public class TestUtil {
    
    public static final String PARAMETRIZED_TEST_DISPLAY_NAME = ParameterizedTest.DISPLAY_NAME_PLACEHOLDER + " [" + ParameterizedTest.INDEX_PLACEHOLDER + "]";


    /**
     * Same as Thread.sleep, except throws a RuntimeException instead of InterruptedException.
     * 
     * @throws SleepInterruptedException if current thread is interrupted.
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            throw new SleepInterruptedException(e);
        }
    }

    public static class SleepInterruptedException extends RuntimeException {
        private static final long serialVersionUID = 1L;
        
        SleepInterruptedException(InterruptedException cause) {
            super(cause);
        }
    }
    
    
    /**
     * Given a list of Future, call get on each item and return a list of T.
     * 
     * @throws CompletionException if we encounter a checked exception while calling future.get()
     */
    public static <T> List<T> toList(Collection<Future<T>> collection) {
        List<T> list = new ArrayList<>(collection.size());
        for (var future : collection) {
            try {
                list.add(future.get());
            } catch (ExecutionException | InterruptedException e) {
                throw new CompletionException(e);
            }
        }
        return list;
    }
    
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
    public static <T, U> U assertExceptionFromCallable(Callable<T> callable, Class<U> expectedException, String expectedMessage) {
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
