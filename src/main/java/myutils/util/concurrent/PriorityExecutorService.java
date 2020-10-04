package myutils.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;


/**
 * An extension of ExecutorService to submit jobs with a priority so that tasks with highest priority run first.
 */
public interface PriorityExecutorService extends ExecutorService {
    /**
     * Submit a callable of the given priority.
     * The other version of this function (without the priority argument) submits a callable with the default priority.
     * 
     * @param <V> the type returned by the callable
     * @param priority the priority of the task, must be in the range [1, 10]
     * @param task the callable
     * @return a future
     */
    <V> Future<V> submit(int priority, Callable<V> task);

    /**
     * Submit a runnable of the given priority.
     * The other version of this function (without the priority argument) submits a runnable with the default priority.
     * 
     * @param priority the priority of the task, must be in the range [1, 10]
     * @param task the callable
     * @return a future
     */
    Future<?> submit(int priority, Runnable task);

    /**
     * Submit a runnable of the given priority.
     * The other version of this function (without the priority argument) submits a runnable with the default priority.
     * 
     * @param <V> the type of the result
     * @param priority the priority of the task, must be in the range [1, 10]
     * @param task the callable
     * @param result the result
     * @return a future
     */
    <V> Future<V> submit(int priority, Runnable task, V result);
}
