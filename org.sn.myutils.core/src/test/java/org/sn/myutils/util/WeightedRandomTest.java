package org.sn.myutils.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.sn.myutils.testutils.TestUtil.assertException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.sn.myutils.testutils.LogFailureToConsoleTestWatcher;
import org.sn.myutils.testutils.TestBase;


@ExtendWith(LogFailureToConsoleTestWatcher.class)
public class WeightedRandomTest extends TestBase {
    @Test
    void testIntegers() {
        List<Integer> weights = Arrays.asList(7, 2, 1);
        WeightedRandom random = new WeightedRandom(weights);
        int numRandoms = 100_000;
        int[] hits = new int[weights.size()];
        for (int i = 0; i < numRandoms; i++) {
            int value = random.get();
            hits[value] = hits[value] + 1;
        }
        List<Double> ratios = Arrays.stream(hits).mapToObj(hit -> (double)hit / (double)numRandoms).collect(Collectors.toList());
        System.out.println("ratios=" + ratios);
        assertEquals(0.7, ratios.get(0), 0.01);
        assertEquals(0.2, ratios.get(1), 0.01);
        assertEquals(0.1, ratios.get(2), 0.01);
    }
    
    @Test
    void testDoubles() {
        List<Double> weights = Arrays.asList(7.0, 2.5, 0.5);
        WeightedRandom random = new WeightedRandom(weights);
        int numRandoms = 100_000;
        int[] hits = new int[weights.size()];
        for (int i = 0; i < numRandoms; i++) {
            int value = random.get();
            hits[value] = hits[value] + 1;
        }
        List<Double> ratios = Arrays.stream(hits).mapToObj(hit -> (double)hit / (double)numRandoms).collect(Collectors.toList());
        System.out.println("ratios=" + ratios);
        assertEquals(0.7, ratios.get(0), 0.01);
        assertEquals(0.25, ratios.get(1), 0.01);
        assertEquals(0.05, ratios.get(2), 0.01);
    }

    @Test
    void testZeroWeight() {
        List<Integer> weights = Arrays.asList(0, 0, 0, 7, 0, 0, 0, 0, 0, 2, 1, 0, 0, 0, 0);
        WeightedRandom random = new WeightedRandom(weights);
        int numRandoms = 100_000;
        int[] hits = new int[weights.size()];
        for (int i = 0; i < numRandoms; i++) {
            int value = random.get();
            hits[value] = hits[value] + 1;
        }
        List<Double> ratios = Arrays.stream(hits).mapToObj(hit -> (double)hit / (double)numRandoms).collect(Collectors.toList());
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

    /**
     * In the previous tests we use the normal random number generator that returns nextInt() as any int in the full range.
     * It is unlikely that the random integer will exactly equal the position, here [iNT_MIN, 0].
     * So use a new random number generator that is guaranteed to return one of those values.
     * This gets full code coverage on the line
     *            if (index < 0) {
     * as usually the condition is true, but in this test the condition is false.
     */
    @Test
    void testIntegers2() {
        List<Integer> weights = Arrays.asList(1, 1);
        WeightedRandom random = new WeightedRandom(weights, new TestRandom());
        int numRandoms = 100_000;
        int[] hits = new int[weights.size()];
        for (int i = 0; i < numRandoms; i++) {
            int value = random.get();
            hits[value] = hits[value] + 1;
        }
        List<Double> ratios = Arrays.stream(hits).mapToObj(hit -> (double)hit / (double)numRandoms).collect(Collectors.toList());
        System.out.println("ratios=" + ratios);
        assertEquals(0.5, ratios.get(0), 0.01);
        assertEquals(0.5, ratios.get(1), 0.01);
    }

    /**
     * The basic algorithm is to pick a random number then find this random number in the position array.
     * As there are many entries in the position array with the same value INT_MIN, and many with the value 0,
     * the algorithm would normally find the a random position.
     * But we need to find the right most position with this value.
     */
    @Test
    void testZeroWeight2() {
        List<Integer> weights = Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 0, 0, 0, 0, 0);
        WeightedRandom random = new WeightedRandom(weights, new TestRandom());
        int numRandoms = 100_000;
        int[] hits = new int[weights.size()];
        for (int i = 0; i < numRandoms; i++) {
            int value = random.get();
            hits[value] = hits[value] + 1;
        }
        List<Double> ratios = Arrays.stream(hits).mapToObj(hit -> (double)hit / (double)numRandoms).collect(Collectors.toList());
        System.out.println("ratios=" + ratios);
        assertEquals(0.5, ratios.get(9), 0.01);
        assertEquals(0.5, ratios.get(10), 0.01);
        for (int i = 0; i < weights.size(); i++) {
            if (i == 9 || i == 10) {
                continue;
            }
            assertEquals(0, ratios.get(i), "i=" + i);
        }
    }
    
    /**
     * When there are two weights [1, 1] the positions are [INT_MIN, 0, INT_MAX].
     * Modify nextInt() to return not an integer, but exactly INT_MIN or 0/INT_MAX with equal probability.
     * The reason to return 0 or INT_MAX is because both result in weight B peing picked,
     * but it tests that we remove the weights with trailing zeroes at the end (see testZeroWeight2).
     */
    private static class TestRandom extends Random {
        private static final long serialVersionUID = 1L;
        
        private Random internalRandom = new Random();
        private boolean returnZero = true;

        @Override
        public int nextInt() {
            if (internalRandom.nextInt() <= 0) {
                return Integer.MIN_VALUE;
            } else {
                boolean rz = returnZero;
                returnZero = !returnZero;
                return rz ? 0 : Integer.MAX_VALUE;
            }
        }
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
