package simpledb;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int bucketCount;
    private int bucketList[];
    private int min;
    private int max;
    private int span;
    private int ntups;

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
    	//create an array to hold tuple count
        this.bucketCount = buckets;
        this.bucketList = new int[bucketCount];
        this.min = min;
        this.max = max;

        //determine the span of each histogram.
        // ie min = 1, max = 10, span = 3.
        double temp = ((max - min + 1));
        this.span = (int)Math.ceil(temp / bucketCount);

        ntups = 0;

    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        //find correct histogram bar
        int i = determineBucket(v);
        int temp = bucketList[i];
        // update value and put it back
        temp++;
        ntups++;
        bucketList[i] = temp;

    }

    public int determineBucket(int v){
        if(v < 0){
            v = v + Math.abs(min);
        }
        else{
            v = v - min;
        }
        int bi = v / span;
        if(bi<0){bi = 0;}
        else if(bi>bucketCount){bi=bucketCount - 1;}
        return bi;
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

        //find correct histogram bar
        int i = determineBucket(v);
        int b_Height;
        int b_Right;
        int b_Left;
        double bucket;
        double restBuckets;
        double selectivity;
        double ratio;

        // set height, left, right
        b_Height = bucketList[i];
        b_Right = i + 1;
        b_Left = i - 1;

    	switch(op) {
            case EQUALS:
                return equalsB(b_Height);

            case GREATER_THAN:
                // get a fraction
                if(v > max){return 0;}
                ratio = (span - Math.abs(span % v));
                selectivity = equalsB(b_Height)*ratio;
                // iterate through the rest of the buckets
                for(int j = b_Right; j < bucketCount; j++){
                    selectivity += equalsB(bucketList[j]);
                }
                return selectivity;

            case LESS_THAN:
                if(v < min){return 0;}
                // get a fraction
                ratio = (Math.abs(span % v))/ span;
                selectivity = equalsB(b_Height)*ratio;
                // iterate through the rest of the buckets
                for(int j = 0; j < i; j++){
                    selectivity += equalsB(bucketList[j]);
                }
                return selectivity;

            case LESS_THAN_OR_EQ:
                if(v < min){return 0;}
                selectivity = equalsB(b_Height);
                // iterate through the rest of the buckets
                for(int j = 0; j < i; j++){
                    selectivity += equalsB(bucketList[j]);
                }
                return selectivity;

            case GREATER_THAN_OR_EQ:
                if(v > max){return 0;}
                selectivity = equalsB(b_Height);
                // iterate through the rest of the buckets
                for(int j = b_Right; j < bucketCount; j++){
                    selectivity += equalsB(bucketList[j]);
                }
                return selectivity;

            case LIKE:
                equalsB(b_Height);
            case NOT_EQUALS:
                return 1 - equalsB(b_Height);
        }
        return 0;
    }

    private double equalsB(int height){
        double selectivity = ((double)(height)/(double)ntups);
        return selectivity;
    }


    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        double sum = 0;
        for(int j = 0; j < bucketCount; j++){
            sum += bucketList[j];
        }
        return (sum/bucketCount);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return "IntHistogram";
    }
}
