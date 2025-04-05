package org.sn.myutils.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.Function;
import java.util.function.Supplier;
import org.sn.myutils.annotations.NotNull;
import org.sn.myutils.util.MoreCollections.FindWhich;


/**
 * Given a list of weights for integers [0, N), generate a weighted random integer.
 */
public class WeightedRandom implements Supplier<Integer> {
    private static final long INT_RANGE = (long)Integer.MAX_VALUE - (long)Integer.MIN_VALUE;
            
    private final Random internalRandom;
    private final List<Integer> positions; // weights represented as an integer in range INT_MIN to INT_MAX
    
    /**
     * Generate a weighted random generator with the given weights and the default Random generator.
     * This random number generates an integer in the range <code>[0, weights.length)</code>.
     * 
     * @param weights the weights of each integer. Can be zero.
     */
    public WeightedRandom(List<? extends Number> weights) {
        this(weights, new Random());
    }
    
    /**
     * Generate a weighted random generator with the given weights and the given internal Random generator.
     * This random number generates an integer in the range [0, weights.length).
     * 
     * <p>For example if we want a random integer either 0, 1, 2 with a 70% change of 0, 20% of 1, 10% of 2,
     * then pass in weights as [7, 2, 1].
     * 
     * @param weights the weights of each integer. Can be zero.
     * @param internalRandom the random number generator used to find a random number
     * @throws IllegalArgumentException there are no weights
     * @throws IllegalArgumentException if any weight is negative
     */
    public WeightedRandom(List<? extends Number> weights, Random internalRandom) {
        this.internalRandom = internalRandom;
        
        // compute sumOfWeights
        int numWeights = weights.size();
        if (numWeights == 0) {
            throw new IllegalArgumentException("there must be at least one weight");
        }
        int numTrailingZeroes = 0;
        double sumOfWeights = 0;
        for (Number weightAsNumber : weights) {
            double weight = weightAsNumber.doubleValue();
            if (weight < 0) {
                throw new IllegalArgumentException("weight cannot be negative: weight=" + weightAsNumber);
            }
            if (weight == 0) {
                numTrailingZeroes += 1;
            } else {
                numTrailingZeroes = 0;
            }
            sumOfWeights += weight;
        }
        
        // Compute the positions array as integers in the range [INT_MIN, INT_MAX).
        
        // To understand the algorithm, if each position was in the range [0, sumOfWeights)
        //     then for weights 7, 2, 1 positions will be [0, 7, 9, 10]
        // This allows us to find a random integer in the range [0, 10) by calling Random.nextInt(int)
        // then use binary search to find the index the number maps to.

        // To handle the case that weights may be zero, we do two things:
        // - Use a modified binary search that finds the leftmost index.
        //   For example if weights are [7, 0, 0, 2, 1] the positions are [0, 7, 7, 7, 9, 10].
        //   So if the random number is 0 <= random < 7 the weight is the first index or weight A.
        //   If the random number is 7 <= random < 9 the weight is the 4th index or D.
        //   Ordinary binary search may have said the index of 7 is 1, 2, or 3 (depending on the implementation)m corresponding to weights B, C, or D.
        //   If the random number is 9 <= random < 10 the index is the 5th element or E.
        //   The random number can never be 10 as nextInt(10) gives a number in [0, 10).
        // - Ignore trailing zeroes.
        //   If the weights are [1, 1, 0, 0] the positions are [0, 1, 2, 2, 2].
        //   So if the random number is 1 <= random < 2 we pick weight B which ia correct.
        //   But once we normalize per paragraph below the positions are [INT_MIN, 0, INT_MAX, INT_MAX, INT_MAX]
        //   and if the random integer picked by the internal random number generator was INT_MAX, then weight D would be picked.
        //   But it should never be picked as its weight is zero.

        // Because weights could be doubles random.nextInt(int) is not possible in general.
        // So let's map positions in the range [INT_MAX, INT_MIN] so that we can use Random.nextInt() instead.
        // Each integer occupies (INT_MAX - INT_MIN) / 10 of space.
        // For weights 7, 2, 1 positions will [INT_RANGE/10*0 + INT_MIN, // = INT_MIN
        //                                     INT_RANGE/10*7 + INT_MIN,
        //                                     INT_RANGE/10*9 + INT_MIN,
        //                                     INT_RANGE/10*10 + INT_MIN // = INT_MAX
        //                                    ] 
        List<Integer> positions = new ArrayList<>(numWeights);
        positions.add(Integer.MIN_VALUE);
        double currentSum = 0;
        for (int i = 0; i < numWeights - 1 - numTrailingZeroes; i++) {
            double weight = weights.get(i).doubleValue();
            currentSum += weight;
            positions.add((int)(INT_RANGE / sumOfWeights * currentSum + Integer.MIN_VALUE));
        }
        
        this.positions = positions;
    }
    
    /**
     * Return a random integer in the range </code>[0, weights.length)</code>.
     * Running time is O(lg(weights.length)).
     *
     * @return a random weighted integer
     */
    @Override
    public @NotNull Integer get() {
        int random = internalRandom.nextInt();
        int index = MoreCollections.binarySearch(positions, 0, positions.size(), Function.identity(), random, FindWhich.FIND_LAST);
        if (index < 0) {
            index = -index - 2;
        }
        return index;
    }
}
