package myutils.pubsub;

import static org.junit.platform.engine.discovery.DiscoverySelectors.selectMethod;

import java.io.PrintWriter;
import org.junit.platform.launcher.Launcher;
import org.junit.platform.launcher.LauncherDiscoveryRequest;
import org.junit.platform.launcher.core.LauncherDiscoveryRequestBuilder;
import org.junit.platform.launcher.core.LauncherFactory;
import org.junit.platform.launcher.listeners.SummaryGeneratingListener;


/**
 * The purpose of this test is to run a few tests
 * because the tests pass when run one at a time but fail if we run the entire class.
 */
public class DistributedPubSubTestRunner {
    public static void main(String[] args) throws InterruptedException {
        String basename = DistributedPubSubIntegrationTest.class.getName();
        LauncherDiscoveryRequest request = LauncherDiscoveryRequestBuilder.request().selectors(
                selectMethod(basename + "#testPerformance()"),
                selectMethod(basename + "#testSubscribeAndPublishAndUnsubscribe")
           ).build();
        Launcher launcher = LauncherFactory.create();
        launcher.discover(request);
        SummaryGeneratingListener listener = new SummaryGeneratingListener();
        launcher.registerTestExecutionListeners(listener);
        launcher.execute(request);
        listener.getSummary().printTo(new PrintWriter(System.out));
    }
}
