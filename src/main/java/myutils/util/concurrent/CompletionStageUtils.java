package myutils.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.function.BiFunction;


public class CompletionStageUtils {
    /**
     * Convert a list of completion stages to a completion stage of list.
     * If any completion stage failed, we invoke an error handler,
     * and if the error handler returns a result then add it to the list,
     * otherwise the error handler throws an exception so don't add it to the list.
     * 
     * <p>If any or all of the original completion stages fail, the completion stage
     * returned by this function does not complete exceptionally.
     * 
     * <p>The elements in the returned list will be in the same order as 'original'.
     * 
     * @param original list of completion stages
     * @param errorHandler what to do if any of the completion stages fail,
     *                     return the value to put into the list, or throw an exception to cause is to not add it to the list.
     * @return completion stage of list
     */
    public static <T, S extends CompletionStage<T>> CompletionStage<List<T>> awaitAll(List<S> original,
                                                                                      BiFunction<Integer /*index*/, Throwable, T> errorHandler) {
        @SuppressWarnings("unchecked")
        CompletableFuture<T>[] originalAsFutures =
                original.stream()
                        .map(stage -> stage.toCompletableFuture())
                        .toArray(CompletableFuture[]::new);
        CompletionStage<Void> all = CompletableFuture.allOf(originalAsFutures);
        return all.handle((ignore, throwable) -> {
            List<T> results = new ArrayList<>(originalAsFutures.length);
            for (int i = 0; i < originalAsFutures.length; i++) {
                CompletableFuture<T> future = originalAsFutures[i];
                try {
                    T result = getResult(i, future, errorHandler);
                    results.add(result);
                } catch (RuntimeException e) {
                    ;
                }
            }
            return results;
        });
    }
    
    private static <T> T getResult(int index, CompletableFuture<T> future, BiFunction<Integer, Throwable, T> errorHandler) {
        try {
            return future.get();
        } catch (ExecutionException | CompletionException e) {
            return errorHandler.apply(index, getCause(e));
        } catch (InterruptedException e) {
            return errorHandler.apply(index, e);
        } catch (RuntimeException | Error e) {
            return errorHandler.apply(index, e);
        }
    }
    
    private static Throwable getCause(Throwable t) {
        Throwable prev = t;
        t = t.getCause();
        while (true) {
            if (t == null) {
                t = prev;
                break;
            }
            if (t instanceof ExecutionException || t instanceof CompletionException) {
                prev = t;
                t = t.getCause();
            } else {
                break;
            }
        }
        return t;
    }
}
