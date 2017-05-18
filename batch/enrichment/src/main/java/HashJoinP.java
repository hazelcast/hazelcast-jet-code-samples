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
import com.hazelcast.jet.function.DistributedFunction;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * A processor to join input items to a map using key extracted from items. It
 * must receive the map on ordinal 0 first, and then it can receive items on
 * other ordinals. Useful for enriching items with additional information.
 * <p>
 * Output is {@code Object[] {item, enrichment}}.
 *
 * @param <T> Input item type
 * @param <K> Key type
 */
public class HashJoinP<T, K> extends AbstractProcessor {

    private final DistributedFunction<T, K> extractKeyF;
    private Map<K, ?> map;

    public HashJoinP(@Nonnull DistributedFunction<T, K> extractKeyF) {
        this.extractKeyF = extractKeyF;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) throws Exception {
        // Joined map comes to ordinal 0
        // We support updating of the map by receiving a new instance
        map = (Map<K, ?>) item;
        return true;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        // items come to ordinals >= 1
        K key = extractKeyF.apply((T) item);
        Object o = map.get(key);
        return tryEmit(new Object[] { item, o });
    }
}
