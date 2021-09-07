package org.sn.myutils.util.concurrent;

import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;


public class StackTraceCompletableFutureOverrideTest {
    /**
     * The purpose of this test is to ensure that StackTraceCompletableFuture overrides all functions of Future and CompletionStage.
     * Suppose in the next version of Java they add more functions to CompletionStage.
     * These functions should be overridden in StackTraceCompletableFuture so that we add stack tracing capability,
     * but because StackTraceCompletableFuture derives from CompletableFuture, there won't be any compile time errors.
     */
    @Test
    void testThatWeOverrideAllFunctions() {
        MethodSet stackTraceCompletionStageMethods = new MethodSet(StackTraceCompletableFuture.class);
        List<String> errors = new ArrayList<>();
        for (Class<?> clazz: Arrays.asList(CompletionStage.class, Future.class, CompletableFuture.class)) {
            for (Method method: clazz.getMethods()) {
                if (clazz.equals(CompletableFuture.class)
                        && Arrays.asList("completedStage", "failedStage", "minimalCompletionStage",
                                         "defaultExecutor", "delayedExecutor").contains(method.getName())) {
                    continue;
                }
                if (!method.getDeclaringClass().equals(clazz)) {
                    continue;
                }
                if (!stackTraceCompletionStageMethods.contains(method)) {
                    String nameAndParams = extractNameAndParams(method);
                    Optional<String> found = errors.stream().filter(error -> error.endsWith(nameAndParams)).findFirst();
                    if (found.isEmpty()) {
                        errors.add(String.format("%02d", errors.size() + 1) + " " + extractReturnType(method) + " " + nameAndParams);
                    }
                }
            }
        }
        if (!errors.isEmpty()) {
            System.err.println("No override found for:");
            errors.forEach(System.err::println);
            fail("Not all methods are overridden");
        }
    }

    private static class MethodSet {
        private final List<Method> methods;
        
        MethodSet(Class<?> declaringClass) {
            this.methods = Arrays.stream(declaringClass.getMethods())
                                 .filter(method -> method.getDeclaringClass().equals(declaringClass))
                                 .toList();
        }

        public boolean contains(Method find) {
            for (Method method: methods) {
                if (method.getName().equals(find.getName())
                        && Arrays.equals(method.getParameterTypes(), find.getParameterTypes())
                        && isStatic(method) == isStatic(find)) {
                    return true;
                }
            }
            return false;
        }
    }

    private static boolean isStatic(Method method) {
        return Modifier.isStatic(method.getModifiers());
    }
    
    private static String extractNameAndParams(Method method) {
        StringBuilder out = new StringBuilder();
        out.append(method.getName());
        out.append('(');
        boolean firstParam = true;
        for (Class<?> param: method.getParameterTypes()) {
            if (!firstParam) {
                out.append(", ");
            }
            out.append(param.getSimpleName());
            firstParam = false;
        }
        out.append(')');
        out.append(" of ");
        out.append(method.getDeclaringClass().getSimpleName());
        return out.toString();
    }
    
    private static String extractReturnType(Method method) {
        StringBuilder out = new StringBuilder();
        out.append(Modifier.toString(method.getModifiers() & (Modifier.PUBLIC | Modifier.PROTECTED | Modifier.STATIC)));
        out.append(' ');
        out.append(method.getReturnType().getSimpleName());
        return out.toString();
    }
}
