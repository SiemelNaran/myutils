package org.sn.myutils.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;

import javax.annotation.Nonnull;


/**
 * Given a list of weights for integers [0, N), generate a weighted random integer.
 */
public class WeightedRandom implements Supplier<Integer> {
    private static final long INT_RANGE = (long)Integer.MAX_VALUE - (long)Integer.MIN_VALUE;
            
    private final Random internalRandom;
    private final List<Integer> positions; // weights represented as an integer in range INT_MIN to INT_MAX
    
    /**
     * Generate a weighted random generator with the given weights and the default Random generator.
     * This random number generates an integer in the range [0, weights.length). 
     * 
     * @param weights the weights of each integer. Can be zero.
     */
    public WeightedRandom(List<Integer> weights) {
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
     * @throws IllegalArgumentException if any weight is negative
     * @throws IllegalArgumentException if all weights are zero
     */
    public WeightedRandom(List<Integer> weights, Random internalRandom) {
        this.internalRandom = internalRandom;
        
        // compute sumOfWeights
        int sumOfWeights = 0;
        for (int weight : weights) {
            if (weight < 0) {
                throw new IllegalArgumentException("weight cannot be negative: weight=" + weight);
            }
            sumOfWeights += weight;
        }
        if (sumOfWeights == 0) {
            throw new IllegalArgumentException("all weights cannot be zero");
        }
        
        // Compute the positions array as integers in the range [INT_MIN, INT_MAX).
        
        // To understand the algorithm, if each position was in the range [0, sumOfWeights)
        //     then for weights 7, 2, 1 positions will be [0, 7, 9, 10]
        // This allows us to find a random integer in the range [0, 10) by calling Random.nextInt(int)
        // then use binary search to find the index the number maps to.
        
        // Because we weights could be doubles random.nextInt(int) is not possible in general.
        // So let's map positions in the range [INT_MAX, INT_MIN] so that we can use Random.nextInt() instead.
        // Each integer occupies (INT_MAX - INT_MIN) / 10 of space.
        // For weights 7, 2, 1 positions will [INT_RANGE/10*0 + INT_MIN, // = INT_MIN
        //                                     INT_RANGE/10*7 + INT_MIN,
        //                                     INT_RANGE/10*9 + INT_MIN,
        //                                     INT_RANGE/10*10 + INT_MIN // = INT_MAX
        //                                    ] 
        int numWeights = weights.size();
        List<Integer> positions = new ArrayList<Integer>(numWeights);
        positions.add(Integer.MIN_VALUE);
        int currentSum = 0;
        for (int i = 0; i < numWeights - 1; i++) {
            int weight = weights.get(i);
            currentSum += weight;
            positions.add((int)(INT_RANGE / sumOfWeights * currentSum + Integer.MIN_VALUE));
        }
        
        this.positions = positions;
    }
    
    /**
     * Return a random integer in the range [0, weights.length).
     * Running time is O(lg(weights.length)).
     * 
     * @return a random weighted integer
     */
    @Override
    public @Nonnull Integer get() {
        int random = internalRandom.nextInt();
        int index = Collections.binarySearch(positions, random);
        if (index < 0) {
            index = -index - 2;
        }
        return index;
    }
}
