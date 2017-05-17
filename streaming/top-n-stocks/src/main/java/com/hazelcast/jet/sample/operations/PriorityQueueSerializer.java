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

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;

import java.io.IOException;

public final class PriorityQueueSerializer implements SerializerHook<PriorityQueue> {

    @Override
    public Class<PriorityQueue> getSerializationType() {
        return PriorityQueue.class;
    }

    @Override
    public Serializer createSerializer() {
        return new StreamSerializer<PriorityQueue>() {
            @Override
            public int getTypeId() {
                return SerializationConstants.PRIORITY_QUEUE;
            }

            @Override
            public void destroy() {
            }

            @Override
            public void write(ObjectDataOutput out, PriorityQueue object) throws IOException {
                object.serialize(out);
            }

            @Override
            public PriorityQueue read(ObjectDataInput in) throws IOException {
                return PriorityQueue.deserialize(in);
            }
        };
    }

    @Override
    public boolean isOverwritable() {
        return true;
    }
}
