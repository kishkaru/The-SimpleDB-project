package simpledb;

import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int numBuckets;
    private int min;
    private int max;
    private double bucketSize;
    private int tuples;
    private int[] buckets;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.numBuckets = buckets;
        this.min = min;
        this.max = max;
        this.tuples = 0;
        this.bucketSize = (max - min) / (double) buckets;
        this.buckets = new int[buckets];
    }

    private int index(int v) {
        int index = (int) ((v - min) / bucketSize);

        if(index == this.numBuckets && index > 0)
            index =  index - 1;

        return index;
    } 

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        buckets[index(v)]++;
    	tuples++;
    }

    private double eqSelect(int v) {
        if ((v < min) || (v > max)) {
            return 0.0;
        }

        return (buckets[index(v)] / bucketSize) / tuples;
    }

    private double gtSelect(int v) {
        // if value smaller than min, everything will be greater
        if (v < min) 
            return 1.0;

        // if value larger than max, nothing will be greater
        if (v > max) 
            return 0.0;

        int index = index(v);
        double b_right = (index + 1 * bucketSize);
        double b_part = (b_right - v) / bucketSize;
        double b_fs = 0.0;

        for (int i = index + 1; i < numBuckets; i++) {
            b_fs += (double) buckets[i] / tuples;
        }

        return ((buckets[index]/tuples) * b_part)  + b_fs;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double select = 0.0;

    	if (op == Predicate.Op.EQUALS)
            select = eqSelect(v);
        if (op == Predicate.Op.GREATER_THAN)
            select = gtSelect(v);
        if (op == Predicate.Op.LESS_THAN)
            select = 1.0 - gtSelect(v) - eqSelect(v);
        if (op == Predicate.Op.GREATER_THAN_OR_EQ)
            select = gtSelect(v) + eqSelect(v);
        if (op == Predicate.Op.LESS_THAN_OR_EQ)
            select =  1.0 - gtSelect(v);
        if (op == Predicate.Op.NOT_EQUALS)
            select = 1.0 - eqSelect(v);

        return select;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        String heights = "| " + buckets[0].toString() + " | ";
        String leftEdges = "| " + min.toString() + " | ";
        for (int i = 1; i < numBuckets; i++) {
            int height = buckets[i]; 
            heights = heights + height.toString() + " | ";
            int leftEdge = min + (i * range) + 1;
            leftEdges = leftEdges + leftEdge.toString() + " | ";
        }
        return heights + "\n" + leftEdges;
    }
}
