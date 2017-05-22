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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.Traversers.traverseStream;

/**
 * A processor that performs the equivalent of an SQL OUTER JOIN of two
 * streams on a common grouping key. It does the following:
 * <ol><li>
 *     Performs a regular group-by-key operation on all the data of edge 0.
 *     Items in the same group are buffered into lists.
 * </li><li>
 *     After exhausting edge 0, starts receiving items from edge 1. For each
 *     item {@code t1} received from edge 1:
 *     <ol><li>
 *         determines its grouping key
 *     </li><li>
 *         fetches the list of buffered edge 0 items with that key
 *     </li><li>
 *         for each item {@code t0} in the list, emits pairs {@code (t0, t1)}.
 *         If the list is empty, emits a pair {@code (null, t1)}.
 *     </li></ol>
 * </li><li>
 *     After exhausting edge 1, if any grouping keys were seen on edge 0 but
 *     not on edge 1, emits all the items from those lists in pairs {@code
 *     (t0, null)}.
 * </li></ol>
 * Edge 0 must be assigned a higher priority than edge 1, otherwise the
 * processor will malfunction.
 *
 * @param <T0> type of items on ordinal 0
 * @param <T1> type of items on ordinal 1
 * @param <K> key type
 */
public class CoGroupP<T0, T1, K> extends AbstractProcessor {

    private final DistributedFunction<? super T0, ? extends K> keyExtractor0;
    private final DistributedFunction<? super T1, ? extends K> keyExtractor1;

    private final Map<K, List<T0>> unseenMap = new HashMap<>();
    private final Map<K, List<T0>> seenMap = new HashMap<>();
    private final FlatMapper<T1, Object[]> flatMapper;
    private boolean t1Received; // for fail-fast behavior
    private final Traverser<Object[]> unseenTraverser =
            traverseStream(unseenMap.values().stream()
                                    .flatMap(List::stream)
                                    .map(t0 -> new Object[]{t0, null}));

    public CoGroupP(DistributedFunction<? super T0, ? extends K> keyExtractor0,
                    DistributedFunction<? super T1, ? extends K> keyExtractor1
    ) {
        this.keyExtractor0 = keyExtractor0;
        this.keyExtractor1 = keyExtractor1;

        flatMapper = flatMapper(this::outputTraverser);
    }

    private Traverser<? extends Object[]> outputTraverser(T1 t1) {
        K key = keyExtractor1.apply(t1);
        List<T0> joinedT0 = seenMap.computeIfAbsent(key, unseenMap::remove);
        if (joinedT0 == null) {
            joinedT0 = Collections.singletonList(null);
        }
        return traverseStream(joinedT0.stream()
                                      .map(t0 -> new Object[]{t0, t1}));
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) {
        assert !t1Received :
                "Edge 0 not fully exhausted before receiving from edge 1. Edge 0 must have a higher priority.";
        K key = keyExtractor0.apply((T0) item);
        unseenMap.computeIfAbsent(key, k -> new ArrayList<>())
                 .add((T0) item);
        return true;
    }

    @Override
    protected boolean tryProcess1(@Nonnull Object item) {
        t1Received = true;
        return flatMapper.tryProcess((T1) item);
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(unseenTraverser);
    }
}
