package com.huawei.hwcloud.tarus.kvstore.util;

import java.util.Arrays;

import static com.huawei.hwcloud.tarus.kvstore.util.HashContainerUtils.PERTURBATIONS;
import static com.huawei.hwcloud.tarus.kvstore.util.HashContainerUtils.nextCapacity;
import static com.huawei.hwcloud.tarus.kvstore.util.HashContainerUtils.roundCapacity;
import static com.huawei.hwcloud.tarus.kvstore.util.Internals.rehash;


/**
 * A hash map of <code>long</code> to <code>int</code>, implemented using open addressing with
 * linear probing for collision resolution.
 *
 * <p>
 * The internal buffers of this implementation ({@link #keys}, {@link #values}, {@link #allocated})
 * are always allocated to the nearest size that is a power of two. When the capacity exceeds the
 * given load factor, the buffer size is doubled.
 * </p>
 *
 * <p>See {@link ObjectObjectOpenHashMap} class for API similarities and differences against Java
 * Collections.
 *
 *
 * <p><b>Important node.</b> The implementation uses power-of-two tables and linear
 * probing, which may cause poor performance (many collisions) if hash values are not properly
 * distributed. This implementation uses rehashing using {@link MurmurHash3}.</p>
 *
 * @author This code is inspired by the collaboration and implementation in the <a
 * href="http://fastutil.dsi.unimi.it/">fastutil</a> project.
 */
@javax.annotation.Generated(date = "2013-07-09T09:54:22+0200", value = "HPPC generated from: LongIntOpenHashMap.java")
public class LongIntOpenHashMap {
    /**
     * Minimum capacity for the map.
     */
    public final static int MIN_CAPACITY = HashContainerUtils.MIN_CAPACITY;

    /**
     * Default capacity.
     */
    public final static int DEFAULT_CAPACITY = HashContainerUtils.DEFAULT_CAPACITY;

    /**
     * Default load factor.
     */
    public final static float DEFAULT_LOAD_FACTOR = HashContainerUtils.DEFAULT_LOAD_FACTOR;

    /**
     * Hash-indexed array holding all keys.
     *
     * @see #values
     */
    public long[] keys;

    /**
     * Hash-indexed array holding all values associated to the keys stored in {@link #keys}.
     *
     * @see #keys
     */
    public int[] values;

    /**
     * Information if an entry (slot) in the {@link #values} table is allocated or empty.
     *
     * @see #assigned
     */
    public boolean[] allocated;

    /**
     * Cached number of assigned slots in {@link #allocated}.
     */
    public int assigned;

    /**
     * The load factor for this map (fraction of allocated slots before the buffers must be rehashed
     * or reallocated).
     */
    public final float loadFactor;

    /**
     * Resize buffers when {@link #allocated} hits this value.
     */
    protected int resizeAt;

    /**
     * The most recent slot accessed in {@link #containsKey} (required for {@link #lget}).
     *
     * @see #containsKey
     * @see #lget
     */
    protected int lastSlot;

    /**
     * We perturb hashed values with the array size to avoid problems with nearly-sorted-by-hash
     * values on iterations.
     *
     * @see "http://issues.carrot2.org/browse/HPPC-80"
     */
    protected int perturbation;

    /**
     * Creates a hash map with the default capacity of {@value #DEFAULT_CAPACITY}, load factor of
     * {@value #DEFAULT_LOAD_FACTOR}.
     *
     * <p>See class notes about hash distribution importance.</p>
     */
    public LongIntOpenHashMap() {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Creates a hash map with the given initial capacity, default load factor of {@value
     * #DEFAULT_LOAD_FACTOR}.
     *
     * <p>See class notes about hash distribution importance.</p>
     *
     * @param initialCapacity Initial capacity (greater than zero and automatically rounded to the
     *                        next power of two).
     */
    public LongIntOpenHashMap(int initialCapacity) {
        this(initialCapacity, DEFAULT_LOAD_FACTOR);
    }

    /**
     * Creates a hash map with the given initial capacity, load factor.
     *
     * <p>See class notes about hash distribution importance.</p>
     *
     * @param initialCapacity Initial capacity (greater than zero and automatically rounded to the
     *                        next power of two).
     * @param loadFactor      The load factor (greater than zero and smaller than 1).
     */
    public LongIntOpenHashMap(int initialCapacity, float loadFactor) {
        initialCapacity = Math.max(initialCapacity, MIN_CAPACITY);

        assert initialCapacity > 0
                : "Initial capacity must be between (0, " + Integer.MAX_VALUE + "].";
        assert loadFactor > 0 && loadFactor <= 1
                : "Load factor must be between (0, 1].";

        this.loadFactor = loadFactor;
        allocateBuffers(roundCapacity(initialCapacity));
    }


    /**
     * {@inheritDoc}
     */

    public int put(long key, int value) {
        assert assigned < allocated.length;

        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        while (allocated[slot]) {
            if (((key) == (keys[slot]))) {
                final int oldValue = values[slot];
                values[slot] = value;
                return oldValue;
            }

            slot = (slot + 1) & mask;
        }

        // Check if we need to grow. If so, reallocate new data, fill in the last element 
        // and rehash.
        if (assigned == resizeAt) {
            expandAndPut(key, value, slot);
        } else {
            assigned++;
            allocated[slot] = true;
            keys[slot] = key;
            values[slot] = value;
        }
        return ((int) 0);
    }

    /**
     * <a href="http://trove4j.sourceforge.net">Trove</a>-inspired API method. An equivalent
     * of the following code:
     * <pre>
     * if (!map.containsKey(key)) map.put(value);
     * </pre>
     *
     * @param key   The key of the value to check.
     * @param value The value to put if <code>key</code> does not exist.
     * @return <code>true</code> if <code>key</code> did not exist and <code>value</code>
     * was placed in the map.
     */
    public boolean putIfAbsent(long key, int value) {
        if (!containsKey(key)) {
            put(key, value);
            return true;
        }
        return false;
    }

    /**
     * <a href="http://trove4j.sourceforge.net">Trove</a>-inspired API method. An equivalent
     * of the following code:
     * <pre>
     *  if (containsKey(key))
     *  {
     *      int v = (int) (lget() + additionValue);
     *      lset(v);
     *      return v;
     *  }
     *  else
     *  {
     *     put(key, putValue);
     *     return putValue;
     *  }
     * </pre>
     *
     * @param key           The key of the value to adjust.
     * @param putValue      The value to put if <code>key</code> does not exist.
     * @param additionValue The value to add to the existing value if <code>key</code> exists.
     * @return Returns the current value associated with <code>key</code> (after changes).
     */
    public int putOrAdd(long key, int putValue, int additionValue) {
        assert assigned < allocated.length;

        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        while (allocated[slot]) {
            if (((key) == (keys[slot]))) {
                return values[slot] = (int) (values[slot] + additionValue);
            }

            slot = (slot + 1) & mask;
        }

        if (assigned == resizeAt) {
            expandAndPut(key, putValue, slot);
        } else {
            assigned++;
            allocated[slot] = true;
            keys[slot] = key;
            values[slot] = putValue;
        }
        return putValue;
    }


    /**
     * An equivalent of calling
     * <pre>
     *  if (containsKey(key))
     *  {
     *      int v = (int) (lget() + additionValue);
     *      lset(v);
     *      return v;
     *  }
     *  else
     *  {
     *     put(key, additionValue);
     *     return additionValue;
     *  }
     * </pre>
     *
     * @param key           The key of the value to adjust.
     * @param additionValue The value to put or add to the existing value if <code>key</code>
     *                      exists.
     * @return Returns the current value associated with <code>key</code> (after changes).
     */
    public int addTo(long key, int additionValue) {
        return putOrAdd(key, additionValue, additionValue);
    }


    /**
     * Expand the internal storage buffers (capacity) and rehash.
     */
    private void expandAndPut(long pendingKey, int pendingValue, int freeSlot) {
        assert assigned == resizeAt;
        assert !allocated[freeSlot];

        // Try to allocate new buffers first. If we OOM, it'll be now without
        // leaving the data structure in an inconsistent state.
        final long[] oldKeys = this.keys;
        final int[] oldValues = this.values;
        final boolean[] oldAllocated = this.allocated;

        allocateBuffers(nextCapacity(keys.length));

        // We have succeeded at allocating new data so insert the pending key/value at
        // the free slot in the old arrays before rehashing.
        lastSlot = -1;
        assigned++;
        oldAllocated[freeSlot] = true;
        oldKeys[freeSlot] = pendingKey;
        oldValues[freeSlot] = pendingValue;

        // Rehash all stored keys into the new buffers.
        final long[] keys = this.keys;
        final int[] values = this.values;
        final boolean[] allocated = this.allocated;
        final int mask = allocated.length - 1;
        for (int i = oldAllocated.length; --i >= 0; ) {
            if (oldAllocated[i]) {
                final long k = oldKeys[i];
                final int v = oldValues[i];

                int slot = rehash(k, perturbation) & mask;
                while (allocated[slot]) {
                    slot = (slot + 1) & mask;
                }

                allocated[slot] = true;
                keys[slot] = k;
                values[slot] = v;
            }
        }

        /*  */
        /*  */
    }

    /**
     * Allocate internal buffers for a given capacity.
     *
     * @param capacity New capacity (must be a power of two).
     */
    private void allocateBuffers(int capacity) {
        long[] keys = new long[capacity];
        int[] values = new int[capacity];
        boolean[] allocated = new boolean[capacity];

        this.keys = keys;
        this.values = values;
        this.allocated = allocated;

        this.resizeAt = Math.max(2, (int) Math.ceil(capacity * loadFactor)) - 1;
        this.perturbation = computePerturbationValue(capacity);
    }

    /**
     * <p>Compute the key perturbation value applied before hashing. The returned value
     * should be non-zero and ideally different for each capacity. This matters because keys are
     * nearly-ordered by their hashed values so when adding one container's values to the other, the
     * number of collisions can skyrocket into the worst case possible.
     *
     * <p>If it is known that hash containers will not be added to each other
     * (will be used for counting only, for example) then some speed can be gained by not perturbing
     * keys before hashing and returning a value of zero for all possible capacities. The speed gain
     * is a result of faster rehash operation (keys are mostly in order).
     */
    protected int computePerturbationValue(int capacity) {
        return PERTURBATIONS[Integer.numberOfLeadingZeros(capacity)];
    }

    /**
     * {@inheritDoc}
     */

    public int remove(long key) {
        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (allocated[slot]) {
            if (((key) == (keys[slot]))) {
                assigned--;
                int v = values[slot];
                shiftConflictingKeys(slot);
                return v;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }

        return ((int) 0);
    }

    /**
     * Shift all the slot-conflicting keys allocated to (and including) <code>slot</code>.
     */
    protected void shiftConflictingKeys(int slotCurr) {
        // Copied nearly verbatim from fastutil's impl.
        final int mask = allocated.length - 1;
        int slotPrev, slotOther;
        while (true) {
            slotCurr = ((slotPrev = slotCurr) + 1) & mask;

            while (allocated[slotCurr]) {
                slotOther = rehash(keys[slotCurr], perturbation) & mask;
                if (slotPrev <= slotCurr) {
                    // we're on the right of the original slot.
                    if (slotPrev >= slotOther || slotOther > slotCurr) {
                        break;
                    }
                } else {
                    // we've wrapped around.
                    if (slotPrev >= slotOther && slotOther > slotCurr) {
                        break;
                    }
                }
                slotCurr = (slotCurr + 1) & mask;
            }

            if (!allocated[slotCurr]) {
                break;
            }

            // Shift key/value pair.
            keys[slotPrev] = keys[slotCurr];
            values[slotPrev] = values[slotCurr];
        }

        allocated[slotPrev] = false;

        /*  */
        /*  */
    }


    /**
     * {@inheritDoc}
     *
     * <p> Use the following snippet of code to check for key existence
     * first and then retrieve the value if it exists.</p>
     * <pre>
     * if (map.containsKey(key))
     *   value = map.lget();
     * </pre>
     */

    public int get(long key) {
        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (allocated[slot]) {
            if (((key) == (keys[slot]))) {
                return values[slot];
            }

            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        return ((int) 0);
    }

    /*  */

    /**
     * Returns the last value saved in a call to {@link #containsKey}.
     *
     * @see #containsKey
     */
    public int lget() {
        assert lastSlot >= 0 : "Call containsKey() first.";
        assert allocated[lastSlot] : "Last call to exists did not have any associated value.";

        return values[lastSlot];
    }

    /**
     * Sets the value corresponding to the key saved in the last call to {@link #containsKey}, if
     * and only if the key exists in the map already.
     *
     * @return Returns the previous value stored under the given key.
     * @see #containsKey
     */
    public int lset(int key) {
        assert lastSlot >= 0 : "Call containsKey() first.";
        assert allocated[lastSlot] : "Last call to exists did not have any associated value.";

        final int previous = values[lastSlot];
        values[lastSlot] = key;
        return previous;
    }

    /**
     * @return Returns the slot of the last key looked up in a call to {@link #containsKey} if it
     * returned <code>true</code>.
     * @see #containsKey
     */
    public int lslot() {
        assert lastSlot >= 0 : "Call containsKey() first.";
        return lastSlot;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Saves the associated value for fast access using {@link #lget}
     * or {@link #lset}.</p>
     * <pre>
     * if (map.containsKey(key))
     *   value = map.lget();
     * </pre>
     * or, to modify the value at the given key without looking up its slot twice:
     * <pre>
     * if (map.containsKey(key))
     *   map.lset(map.lget() + 1);
     * </pre>
     */

    public boolean containsKey(long key) {
        final int mask = allocated.length - 1;
        int slot = rehash(key, perturbation) & mask;
        final int wrappedAround = slot;
        while (allocated[slot]) {
            if (((key) == (keys[slot]))) {
                lastSlot = slot;
                return true;
            }
            slot = (slot + 1) & mask;
            if (slot == wrappedAround) {
                break;
            }
        }
        lastSlot = -1;
        return false;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Does not release internal buffers.</p>
     */

    public void clear() {
        assigned = 0;

        // States are always cleared.
        Arrays.fill(allocated, false);
    }

    /**
     * {@inheritDoc}
     */

    public int size() {
        return assigned;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Note that an empty container may still contain many deleted keys (that occupy buffer
     * space). Adding even a single element to such a container may cause rehashing.</p>
     */
    public boolean isEmpty() {
        return size() == 0;
    }

}
