package org.sn.myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.StringJoiner;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;


public class StackTraceCompletableFutureTest {
    
    private static final long SLEEP_TIME = 1000;

    @BeforeAll
    public static void setupIgnoreStackTrace() {
        StackTraces.addIgnoreClassOrPackageName(Arrays.asList("org.eclipse", "org.junit", "sun.reflect"));
    }
    
    @AfterEach
    public void printBlankLineAfterTest() {
        System.out.println();
    }
    
    private CompletionStage<Integer> doEvenMore(CompletionStage<Integer> stage) {
        return stage.thenApply(val -> { // line 39
            sleep(SLEEP_TIME);
            System.out.println("phase 3");
            return val + 3;
        });
    }
    
    private CompletionStage<Integer> doMore(CompletionStage<Integer> stage) {
        return doEvenMore(stage); // line 47
    }
    
    @SuppressWarnings("checkstyle:LineLength")
    private CompletionStage<Integer> common(boolean shouldThrow) {
        CompletionStage<Integer> stage = StackTraceCompletableFuture.supplyAsync(() -> { // line 52
            sleep(SLEEP_TIME);
            System.out.println("phase 1");
            return 3;
        });
        
        stage = stage.thenApply(val -> { // line 58
            sleep(SLEEP_TIME);
            System.out.println("phase 2");
            return val + 3;
        });
        
        stage = doMore(stage); // line 64
        
        stage = stage.toCompletableFuture().thenApply(val -> { // line 66
            sleep(SLEEP_TIME);
            System.out.println("phase 4");
            if (shouldThrow) {
                throw new IllegalStateException("failed");
            } else {
                return val + 3;
            }
        });
        
        System.out.println(stage.toString()); // example output: StackTraceCompletableFuture -> java.util.concurrent.CompletableFuture@7fac631b[Not completed]
        assertStringContainsInOrder(Arrays.asList("StackTraceCompletableFuture", "java.util.concurrent.CompletableFuture", "[Not completed]"), stage.toString());
        
        return stage;
    }

    private static final List<String> EXPECTED_CALLED_FROM = Collections.unmodifiableList(Arrays.asList(
            "Called from", "StackTraceCompletableFutureTest.java:65",
            "Called from", "StackTraceCompletableFutureTest.java:38", "46", "63",
            "Called from", "StackTraceCompletableFutureTest.java:57",
            "Called from", "StackTraceCompletableFutureTest.java:51"));

    @Test
    void testNormalExecutionAllOf() throws InterruptedException, ExecutionException, TimeoutException {
        System.out.println("testNormalExecutionAllOf");
        CompletionStage<Integer> stage = common(false);
        CompletionStage<Void> waitForAll = StackTraceCompletableFuture.allOf(stage.toCompletableFuture());
        
        waitForAll.toCompletableFuture().join();
        
        int answer = stage.toCompletableFuture().get(0, TimeUnit.MILLISECONDS);
        System.out.println("answer=" + answer);
        assertEquals(12, answer);
        
        String calledFrom = ((StackTraceCompletableFuture<Integer>)stage).getCalledFrom();
        System.out.println(calledFrom);
        assertStringContainsInOrder(EXPECTED_CALLED_FROM, calledFrom);
        
        System.out.println(stage.toString());
        assertStringContainsInOrder(Arrays.asList("StackTraceCompletableFuture",
                                                  "java.util.concurrent.CompletableFuture",
                                                  "[Completed normally]"),
                                    stage.toString());
    }

    @Test
    void testNormalExecutionGet() throws InterruptedException, ExecutionException {
        System.out.println("testNormalExecutionGet");
        CompletionStage<Integer> stage = common(false);
        
        int answer = stage.toCompletableFuture().get();
        System.out.println("answer=" + answer);
        assertEquals(12, answer);
        
        String calledFrom = ((StackTraceCompletableFuture<Integer>)stage).getCalledFrom();
        System.out.println(calledFrom);
        assertStringContainsInOrder(EXPECTED_CALLED_FROM, calledFrom);
        
        System.out.println(stage.toString());
        assertStringContainsInOrder(Arrays.asList("StackTraceCompletableFuture",
                                                  "java.util.concurrent.CompletableFuture",
                                                  "[Completed normally]"),
                                    stage.toString());
    }

    @Test()
    void testExceptionalExecutionAllOf() throws InterruptedException, ExecutionException, TimeoutException {
        System.out.println("testExceptionalExecutionAllOf");
        CompletionStage<Integer> stage = common(true);
        CompletionStage<Void> waitForAll = StackTraceCompletableFuture.allOf(stage.toCompletableFuture()); // line 135
        
        try {
            waitForAll.toCompletableFuture().join();
            fail();
        } catch (CompletionException e) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            PrintStream stream = new PrintStream(bos, /*autoFlush*/ true);
            e.printStackTrace(stream);
            String eString = bos.toString();
            System.out.println(eString);
            assertStringContainsInOrder(Arrays.asList("Caused by: java.lang.IllegalStateException: failed",
                                                      "Called from",
                                                      "StackTraceCompletableFutureTest.java:134"),
                                        eString);
        }
        
        System.out.println(stage.toString());
        assertStringContainsInOrder(Arrays.asList("StackTraceCompletableFuture",
                                                  "java.util.concurrent.CompletableFuture",
                                                  "[Completed exceptionally: java.util.concurrent.CompletionException: java.lang.IllegalStateException: failed]"),
                                    stage.toString());
    }
    
    @Test()
    void testExceptionalExecutionJoin() throws InterruptedException, ExecutionException, TimeoutException {
        System.out.println("testExceptionalExecutionJoin");
        CompletionStage<Integer> stage = common(true);
        
        try {
            stage.toCompletableFuture().join();
            fail();
        } catch (CompletionException e) {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            PrintStream stream = new PrintStream(bos, /*autoFlush*/ true);
            e.printStackTrace(stream);
            String eString = bos.toString();
            System.out.println(eString);
            assertStringContainsInOrder(add(Arrays.asList("Caused by: java.lang.IllegalStateException: failed"),
                                            EXPECTED_CALLED_FROM),
                                        eString);
        }
        
        System.out.println(stage.toString());
        assertStringContainsInOrder(Arrays.asList("StackTraceCompletableFuture",
                                                  "java.util.concurrent.CompletableFuture",
                                                  "[Completed exceptionally: java.util.concurrent.CompletionException: java.lang.IllegalStateException: failed]"),
                                    stage.toString());
    }
    
    private static void sleep(long millis)  {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    
    @SafeVarargs
    private static List<String> add(List<String>...lists) {
        List<String> result = new ArrayList<>();
        for (List<String> list: lists) {
            result.addAll(list);
        }
        return Collections.unmodifiableList(result);
    }
    
    private static void assertStringContainsInOrder(List<String> expecteds, String string) {
        boolean hasErrors = false;
        StringJoiner text = new StringJoiner("\n");
        int index = 0;
        for (String expected: expecteds) {
            int found = string.indexOf(expected, index);
            if (found == -1) {
                hasErrors = true;
                text.add("<MISSING> " + expected);
            } else {
                text.add("<---ok-->" + expected);
                index = found + expected.length();
            }
        }
        if (hasErrors) {
            System.err.println(text);
            fail();
        }
    }
}
