package myutils.util.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;


/**
 * Replacement for the static functions in java.util.concurrent.CompletableFuture to call either
 * the corresponding function in java.util.concurrent.CompletableFuture, or
 * the corresponding function in myutils.util.concurrent.StackTraceCompletableFuture.
 * 
 * <p>By default, this class calls the functions in java.util.concurrent.CompletableFuture.
 * To use StackTraceCompletableFuture instead, set the system property
 * "java.util.concurrent.CompletableFutureFactory" to "StackTraceCompletableFuture".
 * 
 * @param <T> the type returned by the future
 */
public class CompletableFutureFactory<T> {
    
    public static final String COMPLETABLE_FUTURE_FACTORY = "java.util.concurrent.CompletableFutureFactory";

    @SuppressWarnings("checkstyle:EmptyLineSeparator")
    public interface Registration {
        <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier);
        <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier, Executor executor);
        CompletableFuture<Void> runAsync(Runnable runnable);
        CompletableFuture<Void> runAsync(Runnable runnable, Executor executor);
        <U> CompletableFuture<U> completedFuture(U value);
        <U> CompletableFuture<U> failedFuture(Throwable ex);
        CompletableFuture<Void> allOf(CompletableFuture<?>... cfs);
        CompletableFuture<Object> anyOf(CompletableFuture<?>... cfs);
    }
    
    private static final Registration DEFAULT_REGISTRATION;
    private static volatile ConcurrentHashMap<String, Registration> registry = new ConcurrentHashMap<>();
    
    public static <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
        return find().supplyAsync(supplier);
    }

    public static <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier, Executor executor) {
        return find().supplyAsync(supplier, executor);
    }

    public static CompletableFuture<Void> runAsync(Runnable runnable) {
        return find().runAsync(runnable);
    }
    
    public static CompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
        return find().runAsync(runnable, executor);
    }
    
    public static <U> CompletableFuture<U> completedFuture(U value) {
        return find().completedFuture(value);
    }
    
    public <U> CompletableFuture<U> failedFuture(Throwable ex) {
        return find().failedFuture(ex);
    }
    
    public static CompletableFuture<Void> allOf(CompletableFuture<?>... cfs) {
        return find().allOf(cfs);
    }
    
    public static CompletableFuture<Object> anyOf(CompletableFuture<?>... cfs) {
        return find().anyOf(cfs);
    }
    
    private static Registration find() {
        String property = System.getProperty(COMPLETABLE_FUTURE_FACTORY);
        if (property == null) {
            return DEFAULT_REGISTRATION;
        }
        return registry.getOrDefault(property, DEFAULT_REGISTRATION);
    }
    
    /**
     * Register a type of CompletableFuture.
     * 
     * @param key corresponds to the value of the system property "java.util.concurrent.CompletableFutureFactory"
     * @param registration a class that calls that static functions of the specified completable future
     * @throws IllegalArgumentException if the same class is registered twice
     */
    public static synchronized void register(String key, Registration registration) {
        if (registry.containsKey(key)) {
            throw new IllegalArgumentException("duplicate registration");
        }
        registry.put(key, registration);
    }
    
    static {
        DEFAULT_REGISTRATION = new Registration() {
            @Override
            public <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
                return CompletableFuture.supplyAsync(supplier);
            }
    
            @Override
            public <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier, Executor executor) {
                return CompletableFuture.supplyAsync(supplier, executor);
            }
    
            @Override
            public CompletableFuture<Void> runAsync(Runnable runnable) {
                return CompletableFuture.runAsync(runnable);
            }
            
            @Override
            public CompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
                return CompletableFuture.runAsync(runnable, executor);
            }
            
            @Override
            public <U> CompletableFuture<U> completedFuture(U value) {
                return CompletableFuture.completedFuture(value);
            }
            
            @Override
            public <U> CompletableFuture<U> failedFuture(Throwable ex) {
                return CompletableFuture.failedFuture(ex);
            }
            
            @Override
            public CompletableFuture<Void> allOf(CompletableFuture<?>... cfs) {
                return CompletableFuture.allOf(cfs);
            }
            
            @Override
            public CompletableFuture<Object> anyOf(CompletableFuture<?>... cfs) {
                return CompletableFuture.anyOf(cfs);
            }
        };
        
        register("StackTraceCompletableFuture", new Registration() {
            @Override
            public <T> StackTraceCompletableFuture<T> supplyAsync(Supplier<T> supplier) {
                return StackTraceCompletableFuture.supplyAsync(supplier);
            }
    
            @Override
            public <T> StackTraceCompletableFuture<T> supplyAsync(Supplier<T> supplier, Executor executor) {
                return StackTraceCompletableFuture.supplyAsync(supplier, executor);
            }
    
            @Override
            public StackTraceCompletableFuture<Void> runAsync(Runnable runnable) {
                return StackTraceCompletableFuture.runAsync(runnable);
            }
            
            @Override
            public StackTraceCompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
                return StackTraceCompletableFuture.runAsync(runnable, executor);
            }
            
            @Override
            public <U> StackTraceCompletableFuture<U> completedFuture(U value) {
                return StackTraceCompletableFuture.completedFuture(value);
            }
            
            @Override
            public <U> CompletableFuture<U> failedFuture(Throwable ex) {
                return StackTraceCompletableFuture.failedFuture(ex);
            }
            
            @Override
            public StackTraceCompletableFuture<Void> allOf(CompletableFuture<?>... cfs) {
                return StackTraceCompletableFuture.allOf(cfs);
            }
            
            @Override
            public StackTraceCompletableFuture<Object> anyOf(CompletableFuture<?>... cfs) {
                return StackTraceCompletableFuture.anyOf(cfs);
            }
        });
    }
}
