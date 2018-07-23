package myutils.util.concurrent;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.function.Supplier;


/**
 * Replacement for the static functions in java.util.concurrent.CompletableFuture to call either
 * the corresponding function in java.util.concurrent.CompletableFuture, or
 * the corresponding function in myutils.util.concurrent.StackTraceCompletableFuture.<p>
 * 
 * By default, this class calls the functions in coding.util.concurrent.CompletableFuture.
 * To use StackTraceCompletableFuture instead, set the system property
 * "coding.util.concurrent.CompletableFutureFactory" to "StackTraceCompletableFuture".
 * 
 * @author snaran
 *
 * @param <T> the type returned by the future
 */
public class CompletableFutureFactory<T> {
    
    public static final String COMPLETABLE_FUTURE_FACTORY = "coding.util.concurrent.CompletableFutureFactory";

    public interface Registration {
        <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier);
        <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier, Executor executor);
        CompletableFuture<Void> runAsync(Runnable runnable);
        CompletableFuture<Void> runAsync(Runnable runnable, Executor executor);
        <U> CompletableFuture<U> completedFuture(U value);
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
    
    public static CompletableFuture<Void> allOf(CompletableFuture<?>... cfs) {
        return find().allOf(cfs);
    }
    
    public static CompletableFuture<Object> anyOf(CompletableFuture<?>... cfs) {
        return find().anyOf(cfs);
    }
    
    private static Registration find() {
        String property = System.getProperty(COMPLETABLE_FUTURE_FACTORY);
        if (property == null)
            return DEFAULT_REGISTRATION;
        return registry.getOrDefault(property, DEFAULT_REGISTRATION);
    }
    
    public static synchronized void register(String key, Registration registration) {
        if (registry.containsKey(key)) {
            throw new IllegalArgumentException("duplicate registration");
        }
        registry.put(key, registration);
    }
    
    static {
        DEFAULT_REGISTRATION = new Registration() {
            public <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier) {
                return CompletableFuture.supplyAsync(supplier);
            }
    
            public <T> CompletableFuture<T> supplyAsync(Supplier<T> supplier, Executor executor) {
                return CompletableFuture.supplyAsync(supplier, executor);
            }
    
            public CompletableFuture<Void> runAsync(Runnable runnable) {
                return CompletableFuture.runAsync(runnable);
            }
            
            public CompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
                return CompletableFuture.runAsync(runnable, executor);
            }
            
            public <U> CompletableFuture<U> completedFuture(U value) {
                return CompletableFuture.completedFuture(value);
            }
            
            public CompletableFuture<Void> allOf(CompletableFuture<?>... cfs) {
                return CompletableFuture.allOf(cfs);
            }
            
            public CompletableFuture<Object> anyOf(CompletableFuture<?>... cfs) {
                return CompletableFuture.anyOf(cfs);
            }
        };
        
        register("StackTraceCompletableFuture", new Registration() {
            public <T> StackTraceCompletableFuture<T> supplyAsync(Supplier<T> supplier) {
                return StackTraceCompletableFuture.supplyAsync(supplier);
            }
    
            public <T> StackTraceCompletableFuture<T> supplyAsync(Supplier<T> supplier, Executor executor) {
                return StackTraceCompletableFuture.supplyAsync(supplier, executor);
            }
    
            public StackTraceCompletableFuture<Void> runAsync(Runnable runnable) {
                return StackTraceCompletableFuture.runAsync(runnable);
            }
            
            public StackTraceCompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
                return StackTraceCompletableFuture.runAsync(runnable, executor);
            }
            
            public <U> StackTraceCompletableFuture<U> completedFuture(U value) {
                return StackTraceCompletableFuture.completedFuture(value);
            }
            
            public StackTraceCompletableFuture<Void> allOf(CompletableFuture<?>... cfs) {
                return StackTraceCompletableFuture.allOf(cfs);
            }
            
            public StackTraceCompletableFuture<Object> anyOf(CompletableFuture<?>... cfs) {
                return StackTraceCompletableFuture.anyOf(cfs);
            }
        });
    }
}
