package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.Test;


public class ExceptionUtilsTest {
    @Test
    void testUnwrapCompletionException() throws Exception {
        IOException ioe = new IOException("test");
        assertEquals(RuntimeException.class, ExceptionUtils.unwrapCompletionException(new RuntimeException(ioe)).getClass());
        assertEquals(IOException.class, ExceptionUtils.unwrapCompletionException(new CompletionException(ioe)).getClass());
        assertEquals(IOException.class, ExceptionUtils.unwrapCompletionException(new ExecutionException(ioe)).getClass());
    }
}
