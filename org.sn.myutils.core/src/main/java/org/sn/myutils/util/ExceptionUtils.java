package org.sn.myutils.util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;


public class ExceptionUtils {
    
    /**
     * Unwrap a CompletionException or ExecutionException.
     * This function is recursive: if the cause is also a CompletionException or ExecutionException, then unwrap that as well.
     * 
     * <p>This function does not check for infinite recursion (i.e. the exception's cause is the exception itself.
     */
    public static Throwable unwrapCompletionException(Throwable throwable) {
        while (throwable instanceof CompletionException || throwable instanceof ExecutionException) {
            throwable = throwable.getCause();
        }
        return throwable;
    }
}
