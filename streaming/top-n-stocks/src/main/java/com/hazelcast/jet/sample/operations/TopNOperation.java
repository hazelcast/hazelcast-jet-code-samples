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
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.stream.impl.StreamUtil.checkSerializable;

public class TopNOperation<T> implements WindowOperation<T, List<T>, List<T>> {
    private final int n;
    private final Comparator<? super T> comparator;

    public TopNOperation(int n, DistributedComparator<? super T> comparator) {
        checkSerializable(comparator, "comparator");
        this.n = n;
        this.comparator = comparator;
    }

    @Nonnull
    @Override
    public DistributedSupplier<List<T>> createAccumulatorF() {
        return () -> new ArrayList<>(n);
    }

    @Nonnull
    @Override
    public DistributedBiFunction<List<T>, T, List<T>> accumulateItemF() {
        return (a, i) -> {
            // accumulate item `i` into list `a` - insert at correct place
            int pos = Collections.binarySearch(a, i, comparator);
            if (pos < 0) {
                pos = -pos - 1;
            }
            if (pos < n) {
                if (a.size() == n) {
                    a.remove(n - 1);
                }
                a.add(pos, i);
            }
            return a;
        };
    }

    @Nonnull
    @Override
    public DistributedBinaryOperator<List<T>> combineAccumulatorsF() {
        return (a1, a2) -> {
            // merge two lists a1 and a2 into resulting aRes
            List<T> aRes = new ArrayList<>(n);
            for (int i = 0, i1 = 0, i2 = 0; i < n; i++) {
                T e1 = i1 < a1.size() ? a1.get(i1) : null;
                T e2 = i2 < a2.size() ? a2.get(i2) : null;
                if (e1 == null && e2 == null) {
                    // both collections fully merged and still less than n items in aRes
                    break;
                }
                if (e2 == null || e1 != null && comparator.compare(e1, e2) > 0) {
                    aRes.add(e1);
                    i1++;
                } else {
                    aRes.add(e2);
                    i2++;
                }
            }
            return aRes;
        };
    }

    @Nullable
    @Override
    public DistributedBinaryOperator<List<T>> deductAccumulatorF() {
        return null;
    }

    @Nonnull
    @Override
    public DistributedFunction<List<T>, List<T>> finishAccumulationF() {
        return identity();
    }
}
