package org.sn.myutils.util.concurrent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.annotations.Nullable;


public class StackTraces {

    private static volatile List<String> _ignoreClassOrPackageNameList = new ArrayList<>();
    private static volatile List<String> _ignoreModuleNameList = new ArrayList<>();
    
    static {
        _ignoreClassOrPackageNameList.add("org.sn.myutils.util.concurrent.StackTraceCompletableFuture");
    }
    
    /**
     * Add ignore class or package or module names.
     * Call stack lines whose class starts with one of these will be removed from the call stack.
     * An example list would be : ["org.eclipse", "org.junit", "java/"]
     * 
     * @param ignores the list of package or class or module names to ignore. If an element ends with / it is a module name.
     */
    public static synchronized void addIgnoreClassOrPackageOrModuleName(List<String> ignores) {
        List<String> newList = new ArrayList<>(_ignoreClassOrPackageNameList);
        newList.addAll(ignores.stream()
                              .filter(elem -> !elem.endsWith("/")) // filter only class and package names
                              .collect(Collectors.toList()));
        newList.sort(Comparator.naturalOrder());
        _ignoreClassOrPackageNameList = newList;
        
        newList = new ArrayList<>(_ignoreModuleNameList);
        newList.addAll(ignores.stream()
                              .filter(elem -> elem.endsWith("/")) // filter only module names
                              .map(elem -> elem.substring(0, elem.length() - 1)) // remove the trailing /
                              .collect(Collectors.toList()));
        newList.sort(Comparator.naturalOrder());
        _ignoreModuleNameList = newList;
    }
    
    private static boolean shouldIgnore(@Nullable String moduleName, @NotNull String className) {
        if (shouldIgnoreClassOrPackage(className)) {
            return true;
        }
        if (shouldIgnoreModule(moduleName)) {
            return true;
        }
        return false;
    }
    
    private static boolean shouldIgnoreClassOrPackage(@NotNull String className) {
        List<String> ignoreList = _ignoreClassOrPackageNameList;
        int index = Collections.binarySearch(ignoreList, className);
        if (index >= 0) {
            return true;
        }
        index = -index - 2;
        if (index < 0) {
            return false;
        }
        String token = ignoreList.get(index);
        if (className.startsWith(token)) {
            // if we are ignoring java.util then also ignore java.util.concurrent
            // if we are ignoring a.b.ClassName then also ignore nested classes a.b.ClassName$NestedClass
            char c = className.charAt(token.length());
            return c == '.' || c == '$';
        }
        return false;
    }

    private static boolean shouldIgnoreModule(@Nullable String moduleName) {
        if (moduleName == null) {
            return false;
        }
        List<String> ignoreList = _ignoreModuleNameList;
        int index = Collections.binarySearch(ignoreList, moduleName);
        if (index >= 0) {
            return true;
        }
        index = -index - 2;
        if (index < 0) {
            return false;
        }
        String token = ignoreList.get(index);
        if (moduleName.startsWith(token)) {
            // if we are ignoring java/ then also ignore java.base/
            char c = moduleName.charAt(token.length());
            return c == '.';
        }
        return false;
    }

    private final @Nullable StackTraces parent; 
    private final @NotNull List<StackTraceElement> stackTrace;
    
    StackTraces(@Nullable StackTraces parent) {
        this.parent = parent;
        this.stackTrace = computeStackTrace(parent);
    }
    
    private static List<StackTraceElement> computeStackTrace(StackTraces parent) {
        List<StackTraceElement> currentFiltered = new ArrayList<>();
        
        List<StackTraceElement> parentStackTrace = parent != null ? parent.stackTrace : Collections.emptyList();
        StackTraceElement[] currentStackTrace = new Exception().getStackTrace();
        
        boolean done = false;
        for (int i = 4; i < currentStackTrace.length; i++) {
            StackTraceElement currentElem = currentStackTrace[i];
            if (currentElem.getLineNumber() == 1 || shouldIgnore(currentElem.getModuleName(), currentElem.getClassName())) {
                continue;
            }
            for (StackTraceElement parentElem: parentStackTrace) {
                if (currentElem.equals(parentElem)) {
                    done = true;
                }
            }
            currentFiltered.add(currentElem);
            if (done) {
                break;
            }
        }
        
        return currentFiltered;
    }
    
    private void writeCalledFrom(PrintWriter writer) {
        for (StackTraces traces = this; traces != null; traces = traces.parent) {
            writer.println("Called from");
            for (StackTraceElement elem: traces.stackTrace) {
                writer.println("\t" + elem.toString());
            }
        }
    }

    String generateCalledFrom() {
        try (ByteArrayOutputStream bis = new ByteArrayOutputStream()) {
            PrintWriter writer = new PrintWriter(bis);
            writeCalledFrom(writer);
            writer.flush();
            return bis.toString();
        } catch (IOException e) {
            return "";
        }
    }
    
    RuntimeException generateException(Throwable throwable) {
        while (throwable instanceof CompletionException) {
            throwable = throwable.getCause();
        }
        String calledFrom = generateCalledFrom();
        return new StackTracesCompletionException(throwable, calledFrom);
    }
    
    private static class StackTracesCompletionException extends CompletionException {
        private static final long serialVersionUID = 1L;
        
        private final String calledFrom;
        
        private StackTracesCompletionException(Throwable throwable, String calledFrom) {
            super(throwable);
            this.calledFrom = calledFrom;
        }
        
        @Override
        public void printStackTrace(PrintWriter writer) {
            super.printStackTrace(writer);
            writer.println(calledFrom);
        }
        
        @Override
        public void printStackTrace(PrintStream stream) {
            super.printStackTrace(stream);
            stream.println(calledFrom);
        }
    }
}
