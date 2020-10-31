package org.sn.myutils.util;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;


public class ExceptionUtils {
    
    /**
     * Unwrap a CompletionException or ExecutionException.
     */
    public static Throwable unwrapCompletionException(Throwable throwable) {
        if (throwable instanceof CompletionException || throwable instanceof ExecutionException) {
            throwable = throwable.getCause();
        }
        return throwable;
    }
}
