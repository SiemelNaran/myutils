package myutils.util.concurrent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CompletionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;


class StackTraces {

    private static volatile List<String> _ignoreClassOrPackageNameList = new ArrayList<>();
    
    static {
        _ignoreClassOrPackageNameList.add("myutils.util.concurrent.StackTraceCompletableFuture");
    }
    
    /**
     * Add ignore class or package names.  Call stack lines whose class starts with one of these will be removed from the call stack.
     * An example list would be : ["org.eclipse", "org.junit", "sun.reflect"]
     * 
     * @param ignores the list of package names to ignore
     */
    public static synchronized void addIgnoreClassOrPackageName(List<String> ignores) {
        List<String> newList = new ArrayList<>(_ignoreClassOrPackageNameList);
        newList.addAll(ignores);
        newList.sort(Comparator.naturalOrder());
        _ignoreClassOrPackageNameList = newList;
    }
    
    private static boolean shouldIgnore(List<String> ignoreList, String text) {
        int index = Collections.binarySearch(ignoreList, text);
        if (index >= 0) {
            return true;
        }
        index = -index - 2;
        if (index < 0) {
            return false;
        }
        String token = ignoreList.get(index);
        if (text.startsWith(token)) {
            char c = text.charAt(token.length());
            return c == '.' || c == '$';
        }
        return false;
    }

    private final @Nullable StackTraces parent; 
    private final @Nonnull List<StackTraceElement> stackTrace;
    
    StackTraces(@Nullable StackTraces parent) {
        this.parent = parent;
        this.stackTrace = computeStackTrace(parent);
    }
    
    private static List<StackTraceElement> computeStackTrace(StackTraces parent) {
        List<StackTraceElement> currentFiltered = new ArrayList<>();
        
        List<StackTraceElement> parentStackTrace = parent != null ? parent.stackTrace : Collections.emptyList();
        StackTraceElement[] currentStackTrace = new Exception().getStackTrace();
        List<String> ignoreList = _ignoreClassOrPackageNameList;
        
        boolean done = false;
        for (int i = 4; i < currentStackTrace.length; i++) {
            StackTraceElement currentElem = currentStackTrace[i];
            if (currentElem.getLineNumber() == 1 || shouldIgnore(ignoreList, currentElem.getClassName())) {
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
