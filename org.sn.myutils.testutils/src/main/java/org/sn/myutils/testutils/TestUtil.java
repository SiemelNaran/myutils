package org.sn.myutils.testutils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.Serial;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.jupiter.params.ParameterizedTest;


public class TestUtil {
    private TestUtil() {
    }

    public static final String PARAMETRIZED_TEST_DISPLAY_NAME = ParameterizedTest.DISPLAY_NAME_PLACEHOLDER + " [" + ParameterizedTest.INDEX_PLACEHOLDER + "]";


    /**
     * Return a ThreadFactory whose first thread is threadA, second is threadB, etc.
     */
    public static ThreadFactory myThreadFactory() {
        AtomicInteger threadNumber = new AtomicInteger();
        return runnable -> new Thread(runnable, "thread" + Character.toString(threadNumber.getAndIncrement() + 'A'));
    }

    /**
     * Return the smaller of two elements.
     * If they are the same value, return the first one.
     * This is just like Math.min.
     */
    public static <T extends Comparable<T>> T min(T first, T second) {
        return first.compareTo(second) <= 0 ? first : second;
    }

    /**
     * Return the larger of two elements.
     * If they are the same value, return the first one.
     * This is just like Math.max.
     */
    public static <T extends Comparable<T>> T max(T first, T second) {
        return first.compareTo(second) >= 0 ? first : second;
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
        @Serial
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
     *
     * @throws CompletionException if there was an exception with the cause as the real exception
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
     * @throws AssertionError if assertion fails
     */
    public static <T, U extends Throwable> void assertExceptionFromCallable(Callable<T> callable, Class<U> expectedExceptionClass) {
        assertExceptionFromCallable(callable, expectedExceptionClass, _ -> { });
    }

    /**
     * Assert that the desired exception is thrown.
     *
     * @throws AssertionError if assertion fails
     */
    public static <T, U extends Throwable> void assertExceptionFromCallable(Callable<T> callable, Class<U> expectedExceptionClass, String expectedMessage) {
        assertExceptionFromCallable(callable, expectedExceptionClass, exception -> assertEquals(expectedMessage, exception.getMessage()));
    }

    /**
     * Assert that the desired exception is thrown.
     *
     * @param callable the function to run.
     * @param expectedExceptionClass the class of exception to expect.
     * @param exceptionChecker the function to check if the exception has the right value,
     *        for example <code>exception -> assertEquals(expectedMessage, exception.getMessage())</code>
     * @throws AssertionError if assertion fails
     */
    @SuppressWarnings("unchecked")
    public static <T, U extends Throwable> void assertExceptionFromCallable(Callable<T> callable, Class<U> expectedExceptionClass, Consumer<U> exceptionChecker) {
        try {
            callable.call();
            fail("Expected exception " + expectedExceptionClass.getSimpleName() + ", but got no exception");
            throw new AssertionError();
        } catch (Throwable e) {
            assertTrue(expectedExceptionClass.isInstance(e), "Expected " + expectedExceptionClass.getSimpleName()
                    + " or an exception derived from it, " + "but got " + e.getClass().getSimpleName());
            exceptionChecker.accept((U) e);
        }
    }

    /**
     * Assert that the desired exception is thrown.
     * 
     * @throws AssertionError if assertion fails
     */
    public static <U extends Throwable> void assertException(Runnable runnable, Class<U> expectedExceptionClass) {
        assertException(runnable, expectedExceptionClass, _ -> { });
    }

    /**
     * Assert that the desired exception is thrown.
     *
     * @throws AssertionError if assertion fails
     */
    public static <U extends Throwable> void assertException(Runnable runnable, Class<U> expectedExceptionClass, String expectedMessage) {
        assertException(runnable, expectedExceptionClass, exception -> assertEquals(expectedMessage, exception.getMessage()));
    }

    /**
     * Assert that the desired exception is thrown.
     *
     * @param runnable the function to run.
     * @param expectedExceptionClass the class of exception to expect.
     * @param exceptionChecker the function to check if the exception has the right value,
     *        for example <code>exception -> assertEquals(expectedMessage, exception.getMessage())</code>
     * @throws AssertionError if assertion fails
     */
    @SuppressWarnings("unchecked")
    public static <U extends Throwable> void assertException(Runnable runnable, Class<U> expectedExceptionClass, Consumer<U> exceptionChecker) {
        try {
            runnable.run();
            fail("Expected exception " + expectedExceptionClass.getSimpleName() + ", but got no exception");
            throw new AssertionError();
        } catch (RuntimeException | Error e) {
            assertTrue(expectedExceptionClass.isInstance(e), "Expected " + expectedExceptionClass.getSimpleName()
                    + " or an exception derived from it, " + "but got " + e.getClass().getSimpleName());
            exceptionChecker.accept((U) e);
        }
    }

    public static <T extends Comparable<T>> Between<T> between(T low, T high) {
        return new Between<>(low, high);
    }
    
    public static class Between<T extends Comparable<T>> extends BaseMatcher<T> {
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
