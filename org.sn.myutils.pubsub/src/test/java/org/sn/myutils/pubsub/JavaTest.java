package org.sn.myutils.pubsub;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.sn.myutils.testutils.TestUtil.sleep;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;
import org.sn.myutils.testutils.TestBase;


@SuppressWarnings("ResultOfMethodCallIgnored")
public class JavaTest extends TestBase {
    /**
     * The purpose of this test is to show that stream iteration can also throw ConcurrentModificationException.
     */
    @Test
    void testConcurrentModificationWhileStreaming() throws InterruptedException {
        StringBuffer result = new StringBuffer();
        List<Integer> list = new ArrayList<>(Arrays.asList(1, 2, 3, 4, 5, 6));
        
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        executor.submit(() -> {
            try {
                list.stream() // replacing with list.forEach causes test to fail
                    .forEach(val -> {
                        sleep(100);
                        System.out.println("found result " + val);
                        result.append(val).append(", ");
                    });
            } catch (RuntimeException | Error e) {
                result.append(e.getClass().getSimpleName());
            }
        });
        
        executor.submit(() -> {
            try {
                sleep(250);
                System.out.println("about to add elements to list");
                list.addAll(Arrays.asList(7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17));
            } catch (RuntimeException | Error e) {
                result.append(e.getClass().getSimpleName());
            }
        });
        
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        
        // the forEach probably ends up calling ArrayList.Spliterator.forEachRemaining which throws ConcurrentModificationException at the end of the iteration
        assertThat(result.toString(), Matchers.equalTo("1, 2, 3, 4, 5, 6, ConcurrentModificationException"));
        
        assertThat(list, Matchers.contains(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17));
    }
}
