package org.sn.myutils.testutils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.params.ParameterizedTest;


public class TestUtil {
    
    public static final String PARAMETRIZED_TEST_DISPLAY_NAME = ParameterizedTest.DISPLAY_NAME_PLACEHOLDER + " [" + ParameterizedTest.INDEX_PLACEHOLDER + "]";


    /**
     * Return a ThreadFactory whose first thread is threadA, second is threadB, etc.
     */
    public static ThreadFactory myThreadFactory() {
        AtomicInteger threadNumber = new AtomicInteger();
        return runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A'));
    }


    /**
     * Same as Thread.sleep, except throws a RuntimeException instead of InterruptedException.
     * 
     * @throws SleepInterruptedException if current thread is interrupted. SleepInterruptedException is a RuntimeException.
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
     * @throws CancellationException if any future was cancelled
     * @throws CompletionException if any future encountered an ExecutionException or InterruptedException
     */
    public static <T> List<T> toList(Collection<? extends Future<T>> collection) {
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
     * Same as future.get() except throws CompletionException in case of error.
     */
    public static <T> T join(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new CompletionException(e);
        }
    }
    
    /**
     * Convert a CompletionStage to a Future.
     * Only works if the class inherits from Future, as CompletableFuture does.
     */
    @SuppressWarnings("unchecked")
    public static <T> Future<T> toFuture(CompletionStage<T> future) {
        return (Future<T>) future;
    }

    /**
     * Given a list like [I, I, A, A, I, I, I, C, C] in other words 2 A's, 2 C's, 5 I's,
     * return a string like the following.
     * A=2, C=1, I=5
     * 
     * <p>This could be used to check if a list contains the correct quantities of each value in any order.
     */
    public static String countElementsInListByType(List<String> list) {
        return countElementsInListByType(list, Function.identity());
    }
    
    
    /**
     * Given a list like [fI, fI, fA, fA, fI, fI, fI, fC, fC] in other words 2 A's, 2 C's, 5 I's, and the mapper function as removing the 'f' at the start of each word,
     * return a string like the following.
     * A=2, C=1, I=5
     * 
     * <p>This could be used to check if a list contains the correct quantities of each value in any order.
     */
    public static String countElementsInListByType(List<String> list, Function<String, String> mapper) {
        return list.stream()
                   .collect(Collectors.groupingBy(mapper,
                                                  TreeMap::new,
                                                  Collectors.counting()))
                   .entrySet()
                   .stream()
                   .map(entry -> entry.getKey() + '=' + entry.getValue())
                   .collect(Collectors.joining(", "));
    }
    
    /**
     * Verify that each element in the list is greater than the previous one.
     * 
     * @throws AssertionError if an element is the list is equal to or less than the element before it
     */
    public static <T extends Comparable<T>> void assertIncreasing(List<T> list) {
        if (list.isEmpty()) {
            return;
        }
        var iter = list.iterator();
        T prev = iter.next();
        while (iter.hasNext()) {
            T val = iter.next();
            if (val.compareTo(prev) <= 0) {
                throw new AssertionError("list " + list + " is not increasing at [" + prev + ", " + val + "]");
            }
            prev = val;
        }
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
    
    public static <T extends Comparable<T>> Between<T> between(T low, T high) {
        return new Between<>(low, high);
    }
    
    static class Between<T extends Comparable<T>> extends BaseMatcher<T> {
        private final T low;
        private final T high;
        
        public Between(T low, T high) {
            if (low.compareTo(high) > 0) {
                throw new IllegalArgumentException("low (" + low + ") should be less than or equal to high (" + high + ")");
            }
            this.low = low;
            this.high = high;
        }

        @Override
        public boolean matches(Object actualObject) {
            @SuppressWarnings("unchecked")
            T actual = (T) actualObject;
            return low.compareTo(actual) <= 0 && high.compareTo(actual) >= 0;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("between " + low + " and " + high + " inclusive");
        }
    }
}
