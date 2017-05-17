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

package com.hazelcast.jet.samples.enrichment;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 *
 * @param <T>
 * @param <K>
 * @param <V>
 */
public class HashJoinP<T, K, V> extends AbstractProcessor {

    private final DistributedFunction<? super T, K> extractKeyF;
    private Map<K, V> map;

    public HashJoinP(@Nonnull DistributedFunction<? super T, K> extractKeyF) {
        this.extractKeyF = extractKeyF;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) throws Exception {
        assert map == null : "multiple maps received";
        // joined map comes to ordinal 0
        map = (Map<K, V>) item;
        return true;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        // items come to ordinals >= 1
        K key = extractKeyF.apply((T) item);
        V o = map.get(key);
        return tryEmit(new Object[] { item, o });
    }
}
