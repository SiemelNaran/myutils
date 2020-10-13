package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.sn.myutils.testutils.TestUtil.assertException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.sn.myutils.testutils.TestBase;


public class WeightedRandomTest extends TestBase {
    @Test
    void testIntegers() {
        List<Integer> weights = Arrays.asList(7, 2, 1);
        WeightedRandom random = new WeightedRandom(weights);
        int N = 100_000;
        int[] hits = new int[weights.size()];
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
    
    @Test
    void testDoubles() {
        List<Double> weights = Arrays.asList(7.0, 2.5, 0.5);
        WeightedRandom random = new WeightedRandom(weights);
        int N = 100_000;
        int[] hits = new int[weights.size()];
        for (int i = 0; i < N; i++) {
            int value = random.get();
            hits[value] = hits[value] + 1;
        }
        List<Double> ratios = Arrays.stream(hits).mapToObj(hit -> (double)hit / (double)N).collect(Collectors.toList());
        System.out.println("ratios=" + ratios);
        assertEquals(0.7, ratios.get(0), 0.01);
        assertEquals(0.25, ratios.get(1), 0.01);
        assertEquals(0.05, ratios.get(2), 0.01);
    }

    @Test
    void testZeroWeight() {
        List<Integer> weights = Arrays.asList(0, 0, 0, 7, 0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 0);
        WeightedRandom random = new WeightedRandom(weights);
        int N = 100_000;
        int[] hits = new int[weights.size()];
        for (int i = 0; i < N; i++) {
            int value = random.get();
            hits[value] = hits[value] + 1;
        }
        List<Double> ratios = Arrays.stream(hits).mapToObj(hit -> (double)hit / (double)N).collect(Collectors.toList());
        System.out.println("ratios=" + ratios);
        assertEquals(0.7, ratios.get(3), 0.01);
        assertEquals(0.2, ratios.get(9), 0.01);
        assertEquals(0.1, ratios.get(10), 0.01);
        assertEquals(0.0, ratios.get(0));
        assertEquals(0.0, ratios.get(1));
        assertEquals(0.0, ratios.get(2));
        assertEquals(0.0, ratios.get(4));
        assertEquals(0.0, ratios.get(5));
        assertEquals(0.0, ratios.get(6));
        assertEquals(0.0, ratios.get(7));
        assertEquals(0.0, ratios.get(8));
        assertEquals(0.0, ratios.get(11));
        assertEquals(0.0, ratios.get(12));
        assertEquals(0.0, ratios.get(13));
        assertEquals(0.0, ratios.get(14));
    }

    @Test
    void testNegativeWeight() {
        List<Integer> weights = Arrays.asList(7, 2, -5);
        assertException(() -> new WeightedRandom(weights), IllegalArgumentException.class, "weight cannot be negative: weight=-5");
    }
    

    @Test
    void testNoWeights() {
        List<Integer> weights = Collections.emptyList();
        assertException(() -> new WeightedRandom(weights), IllegalArgumentException.class, "there must be at least one weight");
    }
}
