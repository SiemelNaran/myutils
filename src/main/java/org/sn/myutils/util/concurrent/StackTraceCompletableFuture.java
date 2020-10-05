package org.sn.myutils.util.concurrent;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;


/**
 * Replacement for CompletableFuture that adds a layer to remember the call stack of the place
 * that created the completed future. This is useful for debugging.
 * 
 * <p>If the future fails, then e.printStackTrace() will print something like:
 * <blockquote><pre><code>
 * coding.util.concurrent.StackTraces$StackTracesCompletionException: <i>failure message</i>
 *    call stack lines
 *    ...
 * Caused by: 
 *    call stack lines
 *    ...
 * ... more Caused by clauses may follow ...
 * Called from
 *    call stack lines
 *    ...
 * Called from
 *    call stack lines
 *    ...
 * ... more Called from blocks may follow ...
 * </code></pre></blockquote>
 * 
 * @param <T> the type returned by the future
 */
@SuppressWarnings("checkstyle:LineLength")
class StackTraceCompletableFuture<T> extends CompletableFuture<T> {

    private final @Nonnull StackTraces stackTraces;
    private final @Nonnull CompletableFuture<T> future;
    
    private StackTraceCompletableFuture(StackTraces source, CompletableFuture<T> future) {
        this.stackTraces = new StackTraces(source);
        this.future = future.whenComplete((result, throwable) -> {
            if (throwable != null) {
                Exception newExceptionWithCalledFromDetail = stackTraces.generateException(throwable);
                super.completeExceptionally(newExceptionWithCalledFromDetail);
            } else {
                super.complete(result);
            }
        });
    }
    
    //
    // Overrides of static functions in CompletableFuture
    
    public static <T> StackTraceCompletableFuture<T> supplyAsync(Supplier<T> supplier) {
        return new StackTraceCompletableFuture<T>(null, CompletableFuture.supplyAsync(supplier));
    }

    public static <T> StackTraceCompletableFuture<T> supplyAsync(Supplier<T> supplier, Executor executor) {
        return new StackTraceCompletableFuture<T>(null, CompletableFuture.supplyAsync(supplier, executor));
    }

    public static StackTraceCompletableFuture<Void> runAsync(Runnable runnable) {
        return new StackTraceCompletableFuture<Void>(null, CompletableFuture.runAsync(runnable));
    }
    
    public static StackTraceCompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
        return new StackTraceCompletableFuture<Void>(null, CompletableFuture.runAsync(runnable, executor));
    }
    
    public static <U> StackTraceCompletableFuture<U> completedFuture(U value) {
        return new StackTraceCompletableFuture<U>(null, CompletableFuture.completedFuture(value));
    }
    
    public static <U> StackTraceCompletableFuture<U> failedFuture(Throwable ex) {
        return new StackTraceCompletableFuture<U>(null, CompletableFuture.failedFuture(ex));
    }
    
    public static <U> CompletionStage<U> completedStage(U value) {
        return CompletableFuture.completedStage(value);
    }
    
    public static <U> CompletionStage<U> failedStage(Throwable ex) {
        return CompletableFuture.failedStage(ex);
    }
    
    public static StackTraceCompletableFuture<Void> allOf(CompletableFuture<?>... cfs) {
        return new StackTraceCompletableFuture<Void>(null, CompletableFuture.allOf(cfs));
    }
    
    public static StackTraceCompletableFuture<Object> anyOf(CompletableFuture<?>... cfs) {
        return new StackTraceCompletableFuture<Object>(null, CompletableFuture.anyOf(cfs));
    }

    // Java 9
    
    @Override
    public <U> StackTraceCompletableFuture<U> newIncompleteFuture() {
        return new StackTraceCompletableFuture<U>();
    }

    @Override
    public StackTraceCompletableFuture<T> copy() {
        return new StackTraceCompletableFuture<T>(stackTraces, future.copy());
    }

    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.completeAsync(supplier));
    }
    
    @Override
    public CompletableFuture<T> completeAsync(Supplier<? extends T> supplier, Executor executor) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.completeAsync(supplier, executor));
    }
    
    @Override
    public StackTraceCompletableFuture<T> orTimeout(long timeout, TimeUnit unit) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.orTimeout(timeout, unit));
    }
    
    @Override
    public CompletableFuture<T> completeOnTimeout(T value, long timeout, TimeUnit unit) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.completeOnTimeout(value, timeout, unit));
    }
    
    //
    // Overrides of functions in CompletionStage<T>
    
    @Override
    public <U> StackTraceCompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.thenApply(fn));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.thenApplyAsync(fn));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn, Executor executor) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.thenApplyAsync(fn, executor));
    }

    @Override
    public StackTraceCompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.thenAccept(action));
    }

    @Override
    public StackTraceCompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.thenAcceptAsync(action));
    }

    @Override
    public StackTraceCompletableFuture<Void> thenAcceptAsync(Consumer<? super T> action, Executor executor) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.thenAcceptAsync(action, executor));
    }

    @Override
    public StackTraceCompletableFuture<Void> thenRun(Runnable action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.thenRun(action));
    }

    @Override
    public StackTraceCompletableFuture<Void> thenRunAsync(Runnable action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.thenRunAsync(action));
    }

    @Override
    public StackTraceCompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.thenRunAsync(action, executor));
    }

    @Override
    public <U, V> StackTraceCompletableFuture<V> thenCombine(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return new StackTraceCompletableFuture<V>(stackTraces, future.thenCombine(other, fn));
    }

    @Override
    public <U, V> StackTraceCompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn) {
        return new StackTraceCompletableFuture<V>(stackTraces, future.thenCombineAsync(other, fn));
    }

    @Override
    public <U, V> StackTraceCompletableFuture<V> thenCombineAsync(CompletionStage<? extends U> other, BiFunction<? super T, ? super U, ? extends V> fn, Executor executor) {
        return new StackTraceCompletableFuture<V>(stackTraces, future.thenCombineAsync(other, fn, executor));
    }

    @Override
    public <U> StackTraceCompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.thenAcceptBoth(other, action));
    }

    @Override
    public <U> StackTraceCompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.thenAcceptBothAsync(other, action));
    }

    @Override
    public <U> StackTraceCompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super T, ? super U> action, Executor executor) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.thenAcceptBothAsync(other, action, executor));
    }

    @Override
    public StackTraceCompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.runAfterBoth(other, action));
    }

    @Override
    public StackTraceCompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.runAfterBothAsync(other, action));
    }

    @Override
    public StackTraceCompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.runAfterBothAsync(other, action, executor));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> applyToEither(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.applyToEither(other, fn));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.applyToEitherAsync(other, fn));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> applyToEitherAsync(CompletionStage<? extends T> other, Function<? super T, U> fn, Executor executor) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.applyToEitherAsync(other, fn, executor));
    }

    @Override
    public StackTraceCompletableFuture<Void> acceptEither(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.acceptEither(other, action));
    }

    @Override
    public StackTraceCompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.acceptEitherAsync(other, action));
    }

    @Override
    public StackTraceCompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends T> other, Consumer<? super T> action, Executor executor) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.acceptEitherAsync(other, action, executor));
    }

    @Override
    public StackTraceCompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.runAfterEither(other, action));
    }

    @Override
    public StackTraceCompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.runAfterEitherAsync(other, action));
    }

    @Override
    public StackTraceCompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return new StackTraceCompletableFuture<Void>(stackTraces, future.runAfterEitherAsync(other, action, executor));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> thenCompose(Function<? super T, ? extends CompletionStage<U>> fn) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.thenCompose(fn));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.thenComposeAsync(fn));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> thenComposeAsync(Function<? super T, ? extends CompletionStage<U>> fn, Executor executor) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.thenComposeAsync(fn, executor));
    }

    @Override
    public StackTraceCompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.exceptionally(fn));
    }

    @Override
    public StackTraceCompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.whenComplete(action));
    }

    @Override
    public StackTraceCompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.whenCompleteAsync(action));
    }

    @Override
    public StackTraceCompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.whenCompleteAsync(action, executor));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> handle(BiFunction<? super T, Throwable, ? extends U> fn) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.handle(fn));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.handleAsync(fn));
    }

    @Override
    public <U> StackTraceCompletableFuture<U> handleAsync(BiFunction<? super T, Throwable, ? extends U> fn, Executor executor) {
        return new StackTraceCompletableFuture<U>(stackTraces, future.handleAsync(fn, executor));
    }

    @Override
    public CompletableFuture<T> toCompletableFuture() {
        return this;
    }    
    
    // java 12
    
    @Override
    public CompletableFuture<T> exceptionallyAsync(Function<Throwable, ? extends T> fn) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.exceptionallyAsync(fn));
    }

    @Override
    public CompletableFuture<T> exceptionallyAsync(Function<Throwable, ? extends T> fn, Executor executor) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.exceptionallyAsync(fn, executor));
    }
    
    @Override
    public CompletableFuture<T> exceptionallyCompose(Function<Throwable, ? extends CompletionStage<T>> fn) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.exceptionallyCompose(fn));
    }

    @Override
    public CompletableFuture<T> exceptionallyComposeAsync(Function<Throwable, ? extends CompletionStage<T>> fn) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.exceptionallyComposeAsync(fn));
    }

    @Override
    public CompletableFuture<T> exceptionallyComposeAsync(Function<Throwable, ? extends CompletionStage<T>> fn, Executor executor) {
        return new StackTraceCompletableFuture<T>(stackTraces, future.exceptionallyComposeAsync(fn, executor));
    }
    
    //
    // Overrides of functions in Future<T>
    
    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return future.isCancelled();
    }

    @Override
    public boolean isDone() {
        return future.isDone();
    }

    @Override
    public T get() throws CancellationException, InterruptedException, ExecutionException {
        try {
            return future.get();
        } catch (CancellationException | InterruptedException | ExecutionException e) {
            return super.get(); // will rethrow exception with cause of StackTracesCompletionException
        }
    }

    @Override
    public T get(long timeout, TimeUnit unit) throws CancellationException, InterruptedException, ExecutionException, TimeoutException {
        try {
            return future.get(timeout, unit);
        } catch (CancellationException | InterruptedException | ExecutionException | TimeoutException e) {
            return super.get(); // will rethrow exception with cause of StackTracesCompletionException
        }
    }

    //
    // Overrides of functions in CompletableFuture<T>
    
    public StackTraceCompletableFuture() {
        this.stackTraces = new StackTraces(null);
        this.future = new CompletableFuture<T>();
    }
    
    @Override
    public T join() { 
        try {
            return future.join();
        } catch (CancellationException | CompletionException e) {
            return super.join(); // will rethrow exception with cause of StackTracesCompletionException
        }
    }
    
    @Override
    public T getNow(T valueIfAbsent) {
        return future.getNow(valueIfAbsent);
    }
    
    @Override
    public boolean complete(T value) {
        boolean changed = future.complete(value);
        super.complete(value);
        return changed;
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        boolean changed = future.completeExceptionally(ex);
        super.completeExceptionally(ex);
        return changed;
    }

    @Override
    public boolean isCompletedExceptionally() {
        return future.isCompletedExceptionally();
    }

    @Override
    public void obtrudeValue(T value) {
        future.obtrudeValue(value);
        super.obtrudeValue(value);
    }

    @Override
    public void obtrudeException(Throwable ex) {
        future.obtrudeException(ex);
        super.obtrudeException(ex);
    }

    @Override
    public int getNumberOfDependents() {
        return future.getNumberOfDependents();
    }

    //
    // Overrides of functions in Object
    
    @Override
    public String toString() {
        return "StackTraceCompletableFuture -> " + future.toString();
    }
    
    //
    // New functions
    
    public String getCalledFrom() {
        return stackTraces.generateCalledFrom();
    }    
}
