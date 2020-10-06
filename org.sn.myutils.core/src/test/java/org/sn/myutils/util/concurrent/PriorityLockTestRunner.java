package org.sn.myutils.util.concurrent;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;

import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;


/**
 * The purpose of this test is to run a few tests in PriorityLockTest
 * because there is one test that passes when run alone but fails when running all of the tests (or a certain combination of some tests).
 * As running all tests is too long, we use this test runner to run the few tests that lead to the error.
 */
public class PriorityLockTestRunner {
    public static void main(String[] args) throws InterruptedException {
        String basename = PriorityLockTest.class.getName();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request().selectors(
                selectMethod(basename + "#testLockExceptionOnAwait(java.lang.Class)"),
                selectMethod(basename + "#testSignalTwiceWithOneAwait()"),
                selectMethod(basename + "#testLockExceptionOnLockTimeout")
           ).build();
        Launcher launcher = LauncherFactory.create();
        launcher.discover(request);
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);
    }
}
