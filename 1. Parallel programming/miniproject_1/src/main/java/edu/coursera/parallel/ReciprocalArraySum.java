package edu.coursera.parallel;

import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks   The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the start of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     * nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk     The chunk to compute the end of
     * @param nChunks   The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;

        ReciprocalArraySumTask reciprocalArraySumTask = new ReciprocalArraySumTask(input);
        reciprocalArraySumTask.compute();

        return reciprocalArraySumTask.getValue();
    }

    /**
     * Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input    Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        double sum = 0;

        // create an array of the number of tasks that we need
        final ReciprocalArraySumTask[] reciprocalArraySumTasks = new ReciprocalArraySumTask[numTasks];

        // set the corresponding start and end indices to the tasks
        for (int i = 0; i < numTasks; i++) {
            final int startIndex = getChunkStartInclusive(i, numTasks, input.length);
            final int endIndex = getChunkEndExclusive(i, numTasks, input.length);

            reciprocalArraySumTasks[i] = new ReciprocalArraySumTask(startIndex, endIndex, input);
        }

        // fork all the tasks except the last one
        for (int i = 0; i < numTasks - 1; i++) {
            reciprocalArraySumTasks[i].fork();
        }

        // compute the last one
        reciprocalArraySumTasks[numTasks - 1].compute();
        sum += reciprocalArraySumTasks[numTasks - 1].getValue();

        // join the last one and the others and get the values
        for (int i = 0; i < numTasks - 1; i++) {
            reciprocalArraySumTasks[i].join();
            sum += reciprocalArraySumTasks[i].getValue();
        }

        return sum;
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        private static final int SEQUENTIAL_THRESHOLD = 1000;
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        /**
         * Constructor.
         *
         * @param setStartIndexInclusive Set the starting index to begin
         *                               parallel traversal at.
         * @param setEndIndexExclusive   Set ending index for parallel traversal.
         * @param setInput               Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            this.value = 0;
        }

        /**
         * Easier Constructor.
         *
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final double[] setInput) {
            this.input = setInput;
            this.startIndexInclusive = 0;
            this.endIndexExclusive = this.input.length;
            this.value = 0;
        }

        /**
         * Getter for the value produced by this task.
         *
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            if (endIndexExclusive - startIndexInclusive <= SEQUENTIAL_THRESHOLD) {
                // just sum these values
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    value += 1 / input[i];
                }
            } else {
                // divide into 2 arrays and start process
                int mid = ((endIndexExclusive - startIndexInclusive) / 2 + startIndexInclusive);
                ReciprocalArraySumTask leftSum = new ReciprocalArraySumTask(startIndexInclusive, mid, input);
                ReciprocalArraySumTask rightSum = new ReciprocalArraySumTask(mid, endIndexExclusive, input);
                leftSum.fork();
                rightSum.compute();
                leftSum.join();
                value = leftSum.value + rightSum.value;
            }
        }
    }
}
