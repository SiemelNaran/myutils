package org.sn.myutils;

import java.util.Arrays;
import java.util.Optional;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;


/**
 * The purpose of this class is to log the call stack upon failure to stderr after a test method finishes.
 * IntelliJ 2019.3 does this by default, but Eclipse 2019-03 does not.
 */
public final class LogFailureToConsoleTestWatcher implements TestWatcher {
    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
    }

    @Override
    public void testSuccessful(ExtensionContext context) {
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
        System.err.println(context.getDisplayName() + " aborted");
        
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        System.err.println(context.getDisplayName() + " failed");
        cause.setStackTrace(truncateCallStack(cause.getStackTrace()));
        cause.printStackTrace();
    }

    private static StackTraceElement[] truncateCallStack(StackTraceElement[] stackTraceElements) {
        // set lastElem to the last item in the call stack that starts with "myutils."
        // but exclude stuff called by any custom test runner like "myutils.util.concurrent.PriorityLockTestRunner.main"
        int lastElem = stackTraceElements.length - 1;
        for ( ; lastElem >= 0; lastElem--) {
            StackTraceElement elem = stackTraceElements[lastElem];
            if (elem.getClassName().startsWith("myutils.") && !elem.getMethodName().equals("main")) {
                break;
            }
        }
        return Arrays.copyOf(stackTraceElements, lastElem + 1);
    }
}
