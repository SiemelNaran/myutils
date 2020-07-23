package myutils.pubsub;

import static myutils.TestUtil.sleep;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;


public class JavaTest {
    @Test
    void testNoConcurrentModificationWhileStreaming() throws InterruptedException {
        List<Integer> result = new ArrayList<>();
        
        List<Integer> list = new ArrayList<>();
        list.addAll(Arrays.asList(1, 2, 3, 4, 5, 6));
        
        ExecutorService executor = Executors.newFixedThreadPool(2);
        
        executor.submit(() -> {
            list.stream()
                .forEach(val -> {
                    sleep(100);
                    System.out.println("found result " + val);
                    result.add(val);
                });
        });
        
        executor.submit(() -> {
            sleep(250);
            System.out.println("about to add element to list");
            list.add(7);
        });
        
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
        
        assertThat(result, Matchers.contains(1, 2, 3, 4, 5, 6));
        assertThat(list, Matchers.contains(1, 2, 3, 4, 5, 6, 7));
    }
}
