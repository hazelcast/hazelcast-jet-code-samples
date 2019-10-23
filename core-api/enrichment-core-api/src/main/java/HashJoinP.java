/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.function.FunctionEx;

import javax.annotation.Nonnull;
import java.util.Map;

/**
 * A processor that joins input items with a map using a key extracted from
 * the items. It must receive the map on ordinal 0 first, and then it can
 * receive items on other ordinals. Useful for enriching items with
 * additional information.
 * <p>
 * The shape of output is {@code Object[] {streamItem, joinedItem}}.
 *
 * @param <T> the type of the stream item
 * @param <K> the type of the joining key
 */
public class HashJoinP<T, K> extends AbstractProcessor {

    private final FunctionEx<T, K> extractKeyFn;
    private Map<K, ?> map;

    HashJoinP(@Nonnull FunctionEx<T, K> extractKeyFn) {
        this.extractKeyFn = extractKeyFn;
    }

    @Override
    protected boolean tryProcess0(@Nonnull Object item) throws Exception {
        // Joined map comes to ordinal 0
        assert map == null;
        map = (Map<K, ?>) item;
        return true;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
        // items come to ordinals >= 1
        K key = extractKeyFn.apply((T) item);
        Object o = map.get(key);
        return tryEmit(new Object[] { item, o });
    }
}
