/*
 * Original work Copyright 2014 The Apache Software Foundation
 * Modified work Copyright (c) 2015 Hazelcast, Inc. All rights reserved.
 */

package com.hazelcast.jet.sample.operations;

import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Maintains a partial ordering of elements such that the least one (with
 * regard to the provided {@code comparator} can be looked up in constant time.
 * Insertion and removal operations have O(log size) time complexity. Size is
 * limited to a pre-defined maximum. Adding an element when the queue is full
 * leads to dropping of the least element (see {@link #offer(Object)}).
 */
public final class PriorityQueue<T> {
    private final int maxSize;
    private final DistributedComparator<? super T> comparator;

    private T[] heap;
    private int size;

    /**
     * @param maxSize maximum number of elements the queue can take
     * @param comparator comparator to compare elements
     */
    public PriorityQueue(int maxSize, DistributedComparator<? super T> comparator) {
        this.heap = (T[]) new Object[maxSize == 0 ? 2 : maxSize + 1];
        this.maxSize = maxSize;
        this.comparator = comparator;
    }

    private boolean betterThan(T left, T right) {
        return comparator.compare(left, right) > 0;
    }

    /**
     * Adds an element in O(log size) time. If size is already {@code maxSize}
     * the minimum element will be removed.
     *
     * @return the element (if any) that was dropped off the heap because
     * {@code maxSize} was reached. This can be the supplied chunk (in case it
     * isn't better than the full heap's minimum, and couldn't be added), or
     * another element that was previously the least value in the heap and now
     * has been replaced by a higher one, or {@code null} if the size was less
     * than {@code maxSize}.
     */
    public T offer(T element) {
        if (size < maxSize) {
            size++;
            heap[size] = element;
            upHeap();
            return null;
        } else if (size > 0 && betterThan(element, heap[1])) {
            T ret = heap[1];
            heap[1] = element;
            downHeap();
            return ret;
        } else {
            return element;
        }
    }

    /**
     * Returns the least element.
     */
    public T peek() {
        return size > 0 ? heap[1] : null;
    }

    /**
     * Removes and returns the least element in O(log size) time.
     */
    public T poll() {
        if (size > 0) {
            T result = heap[1];
            heap[1] = heap[size];
            size--;
            downHeap();
            return result;
        } else {
            return null;
        }
    }

    /**
     * Returns the number of elements.
     */
    public int size() {
        return size;
    }

    /**
     * Removes all elements.
     */
    public void clear() {
        size = 0;
    }

    /**
     * Returns current elements, partially ordered from the least one.
     */
    public List<T> asList() {
        return isEmpty() ? Collections.emptyList() : Arrays.asList(heap).subList(1, size + 1);
    }

    /**
     * Returns true, if the queue contains no elements.
     */
    public boolean isEmpty() {
        return size == 0;
    }

    private void upHeap() {
        int i = size;
        // save bottom node
        T node = heap[i];
        int j = i >>> 1;
        while (j > 0 && betterThan(heap[j], node)) {
            // shift parents down
            heap[i] = heap[j];
            i = j;
            j >>>= 1;
        }
        // install saved node
        heap[i] = node;
    }

    private void downHeap() {
        int i = 1;
        // save top node
        T node = heap[i];
        // find worse child
        int j = i << 1;
        int k = j + 1;
        if (k <= size && betterThan(heap[j], heap[k])) {
            j = k;
        }
        while (j <= size && betterThan(node, heap[j])) {
            // shift up child
            heap[i] = heap[j];
            i = j;
            j = i << 1;
            k = j + 1;
            if (k <= size && betterThan(heap[j], heap[k])) {
                j = k;
            }
        }
        // install saved node
        heap[i] = node;
    }

    @Override
    public String toString() {
        return asList().toString();
    }

    void serialize(ObjectDataOutput out) throws IOException {
        out.writeInt(maxSize);
        out.writeObject(comparator);
        out.writeInt(size);
        out.writeObject(heap);
    }

    static PriorityQueue deserialize(ObjectDataInput in) throws IOException {
        PriorityQueue res = new PriorityQueue(in.readInt(), in.readObject());
        res.size = in.readInt();
        res.heap = in.readObject();
        return res;
    }
}
