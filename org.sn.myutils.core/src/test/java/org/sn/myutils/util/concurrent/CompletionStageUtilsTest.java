package org.sn.myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;


public class CompletionStageUtilsTest {
    @Test
    void testAwaitAllWithoutError() {
        CompletionStage<Integer> first = CompletableFuture.supplyAsync(() -> sleepAndReturn(3, 1));
        CompletionStage<Integer> second = CompletableFuture.supplyAsync(() -> sleepAndReturn(1, 2));
        CompletionStage<Integer> third = CompletableFuture.supplyAsync(() -> sleepAndReturn(2, 3));
        List<String> errors = Collections.synchronizedList(new ArrayList<>());
        CompletionStage<List<Integer>> all = CompletionStageUtils.awaitAll(
            Arrays.asList(first, second, third),
            (index, throwable) -> { errors.add(index + "=" + throwable.getMessage()); return null; });
        List<Integer> result = all.toCompletableFuture().join();
        assertEquals(3, result.size());
        assertEquals(1, result.get(0).intValue());
        assertEquals(2, result.get(1).intValue());
        assertEquals(3, result.get(2).intValue());
        assertTrue(errors.isEmpty());
    }

    @Test
    void testAwaitAllWithError() {
        CompletionStage<Integer> first = CompletableFuture.supplyAsync(() -> sleepAndThrow(3));
        CompletionStage<Integer> second = CompletableFuture.supplyAsync(() -> sleepAndReturn(1, 2));
        CompletionStage<Integer> third = CompletableFuture.supplyAsync(() -> sleepAndReturn(2, 3));
        List<String> errors = Collections.synchronizedList(new ArrayList<>());
        CompletionStage<List<Integer>> all = CompletionStageUtils.awaitAll(
            Arrays.asList(first, second, third),
            (index, throwable) -> { errors.add(index + "=" + throwable.getMessage()); return null; });
        List<Integer> result = all.toCompletableFuture().join();
        assertEquals(3, result.size());
        assertNull(result.get(0));
        assertEquals(2, result.get(1).intValue());
        assertEquals(3, result.get(2).intValue());
        assertEquals(Collections.singletonList("0=my failure"), errors);
    }
    
    @Test
    void testAwaitAllWithError2() {
        CompletionStage<Integer> first = CompletableFuture.supplyAsync(() -> sleepAndThrow(3));
        CompletionStage<Integer> second = CompletableFuture.supplyAsync(() -> sleepAndReturn(1, 2));
        CompletionStage<Integer> third = CompletableFuture.supplyAsync(() -> sleepAndReturn(2, 3));
        List<String> errors = Collections.synchronizedList(new ArrayList<>());
        CompletionStage<List<Integer>> all = CompletionStageUtils.awaitAll(
            Arrays.asList(first, second, third),
            (index, throwable) -> { errors.add(index + "=" + throwable.getMessage()); throw new CompletionException(throwable); });
        List<Integer> result = all.toCompletableFuture().join();
        assertEquals(2, result.size());
        assertEquals(2, result.get(0).intValue());
        assertEquals(3, result.get(1).intValue());
        assertEquals(Collections.singletonList("0=my failure"), errors);
    }
    
    private static int sleepAndReturn(int seconds, int result) {
        try {
            Thread.sleep(seconds * 1000L);
            return result;
        } catch (InterruptedException e) {
            throw new CompletionException(e);
        }
    }

    private static int sleepAndThrow(int seconds) {
        try {
            Thread.sleep(seconds * 1000L);
        } catch (InterruptedException e) {
            throw new CompletionException(e);
        }
        throw new CompletionException(new ExecutionException(new RuntimeException("my failure")));
    }
}
