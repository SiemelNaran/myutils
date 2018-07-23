package myutils.util.concurrent;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class CompletableFutureFactoryTest {
    @Before
    @After
    public void removeProperty() {
        System.getProperties().remove("coding.util.concurrent.CompletableFutureFactory");
    }
    
    @Test
    public void testDefault() throws InterruptedException, ExecutionException {
        CompletionStage<Integer> future = CompletableFutureFactory.supplyAsync(() -> 1);
        assertTrue(future instanceof CompletableFuture);
        assertEquals(1, future.toCompletableFuture().get().intValue());
    }

    @Test
    public void testStackTrace() throws InterruptedException, ExecutionException {
        System.setProperty("coding.util.concurrent.CompletableFutureFactory", "StackTraceCompletableFuture");
        CompletionStage<Integer> future = CompletableFutureFactory.supplyAsync(() -> 1);
        assertTrue(future instanceof StackTraceCompletableFuture);
        assertEquals(1, future.toCompletableFuture().get().intValue());
    }
}
