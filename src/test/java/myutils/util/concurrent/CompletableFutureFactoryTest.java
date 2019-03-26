package myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


public class CompletableFutureFactoryTest {
    @BeforeEach
    @AfterEach
    void removeProperty() {
        System.getProperties().remove("coding.util.concurrent.CompletableFutureFactory");
    }
    
    @Test
    void testDefault() throws InterruptedException, ExecutionException {
        CompletionStage<Integer> future = CompletableFutureFactory.supplyAsync(() -> 1);
        assertTrue(future instanceof CompletableFuture);
        assertEquals(1, future.toCompletableFuture().get().intValue());
    }

    @Test
    void testStackTrace() throws InterruptedException, ExecutionException {
        System.setProperty("coding.util.concurrent.CompletableFutureFactory", "StackTraceCompletableFuture");
        CompletionStage<Integer> future = CompletableFutureFactory.supplyAsync(() -> 1);
        assertTrue(future instanceof StackTraceCompletableFuture);
        assertEquals(1, future.toCompletableFuture().get().intValue());
    }
}
