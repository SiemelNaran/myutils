package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.sn.myutils.testutils.TestBase;


public class WeightedRandomTest extends TestBase {
    @Test
    void testIntegers() {
        List<Integer> weights = Arrays.asList(7, 2, 1 );
        WeightedRandom random = new WeightedRandom(weights);
        int N = 100_000;
        double[] hits = new double[weights.size()];
        for (int i = 0; i < N; i++) {
            int value = random.get();
            hits[value] = hits[value] + 1;
        }
        List<Double> ratios = Arrays.stream(hits).mapToObj(hit -> (double)hit / (double)N).collect(Collectors.toList());
        System.out.println("ratios=" + ratios);
        assertEquals(0.7, ratios.get(0), 0.01);
        assertEquals(0.2, ratios.get(1), 0.01);
        assertEquals(0.1, ratios.get(2), 0.01);
    }
}
