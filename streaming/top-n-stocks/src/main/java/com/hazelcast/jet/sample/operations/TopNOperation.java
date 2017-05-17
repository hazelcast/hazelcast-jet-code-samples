/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.sample.operations;

import com.hazelcast.jet.function.DistributedBiFunction;
import com.hazelcast.jet.function.DistributedBinaryOperator;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.windowing.WindowOperation;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.hazelcast.jet.stream.impl.StreamUtil.checkSerializable;

public class TopNOperation<T> implements WindowOperation<T, PriorityQueue<T>, List<T>> {
    private final int n;
    private final Comparator<? super T> comparator;
    private final Comparator<? super T> comparatorReversed;


    public TopNOperation(int n, DistributedComparator<? super T> comparator) {
        checkSerializable(comparator, "comparator");
        this.n = n;
        this.comparator = comparator;
        this.comparatorReversed = comparator.reversed();
    }

    @Nonnull
    @Override
    public DistributedSupplier<PriorityQueue<T>> createAccumulatorF() {
        return () -> new PriorityQueue<>(n, comparator);
    }

    @Nonnull
    @Override
    public DistributedBiFunction<PriorityQueue<T>, T, PriorityQueue<T>> accumulateItemF() {
        return (a, i) -> {
            a.offer(i);
            return a;
        };
    }

    @Nonnull
    @Override
    public DistributedBinaryOperator<PriorityQueue<T>> combineAccumulatorsF() {
        return (a1, a2) -> {
            for (T t : a2.asList()) {
                a1.offer(t);
            }
            return a1;
        };
    }

    @Nullable
    @Override
    public DistributedBinaryOperator<PriorityQueue<T>> deductAccumulatorF() {
        return null;
    }

    @Nonnull
    @Override
    public DistributedFunction<PriorityQueue<T>, List<T>> finishAccumulationF() {
        return a -> {
            ArrayList<T> res = new ArrayList<>(a.asList());
            res.sort(comparatorReversed);
            return res;
        };
    }
}
