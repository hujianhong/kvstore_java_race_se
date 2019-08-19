package com.huawei.hwcloud.tarus.kvstore.util;

import java.util.concurrent.Callable;

final class HashContainerUtils {
    /**
     * Maximum capacity for an array that is of power-of-two size and still allocable in Java (not a
     * negative int).
     */
    final static int MAX_CAPACITY = 0x80000000 >>> 1;

    /**
     * Minimum capacity for a hash container.
     */
    final static int MIN_CAPACITY = 4;

    /**
     * Default capacity for a hash container.
     */
    final static int DEFAULT_CAPACITY = 16;

    /**
     * Default load factor.
     */
    final static float DEFAULT_LOAD_FACTOR = 0.75f;

    /**
     * Computer static perturbations table.
     */
    final static int[] PERTURBATIONS = new Callable<int[]>() {
        public int[] call() {
            int[] result = new int[32];
            for (int i = 0; i < result.length; i++) {
                result[i] = MurmurHash3.hash(17 + i);
            }
            return result;
        }
    }.call();

    /**
     * Round the capacity to the next allowed value.
     */
    static int roundCapacity(int requestedCapacity) {
        if (requestedCapacity > MAX_CAPACITY) {
            return MAX_CAPACITY;
        }

        return Math.max(MIN_CAPACITY, nextHighestPowerOfTwo(requestedCapacity));
    }

    /**
     * returns the next highest power of two, or the current value if it's already a power of two or
     * zero
     */
    public static int nextHighestPowerOfTwo(int v) {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v;
    }

    /**
     * Return the next possible capacity, counting from the current buffers' size.
     */
    static int nextCapacity(int current) {
        assert current > 0 && Long.bitCount(current) == 1 : "Capacity must be a power of two.";

        if (current < MIN_CAPACITY / 2) {
            current = MIN_CAPACITY / 2;
        }

        current <<= 1;
        if (current < 0) {
            throw new RuntimeException("Maximum capacity exceeded.");
        }

        return current;
    }
}
