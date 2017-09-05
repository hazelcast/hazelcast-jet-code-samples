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

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedSupplier;

import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;

import static com.hazelcast.jet.impl.util.Util.checkSerializable;

public class TopNOperation<T>  {

    public static <T> AggregateOperation1<T, ?, List<T>> topNOperation(
            int n, DistributedComparator<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        DistributedComparator<? super T> comparatorReversed = comparator.reversed();
        DistributedSupplier<PriorityQueue<T>> createF = () -> new PriorityQueue<>(n, comparator);
        DistributedBiConsumer<PriorityQueue<T>, T> accumulateF = (PriorityQueue<T> a, T i) -> {
            if (a.size() == n) {
                if (comparator.compare(i, a.peek()) <= 0) {
                    // the new item is smaller or equal to the smallest in queue
                    return;
                }
                a.poll();
            }
            a.offer(i);
        };
        return AggregateOperation
                .withCreate(createF)
                .andAccumulate(accumulateF)
                .andCombine((a1, a2) -> {
                    for (T t : a2) {
                        accumulateF.accept(a1, t);
                    }
                })
                .andFinish(a -> {
                    ArrayList<T> res = new ArrayList<>(a);
                    res.sort(comparatorReversed);
                    return res;
                });
    }
}
