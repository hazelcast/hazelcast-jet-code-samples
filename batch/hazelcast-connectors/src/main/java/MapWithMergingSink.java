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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;

/**
 * Demonstrates the usage of Hazelcast IMap as source and Hazelcast
 * IMap merging sink with the Pipeline API.
 * This will take the contents of one map and merge the updates from Jet
 * with merge function.
 */
public class MapWithMergingSink {

    private static final int ITEM_COUNT = 10;
    private static final String MAP_NAME = "targetMap";

    public static void main(String[] args) throws Exception {

        JetInstance instance = Jet.newJetInstance();

        try {
            IMap<Integer, Integer> sourceMap = instance.getMap(MAP_NAME);
            for (int i = 0; i < ITEM_COUNT; i++) {
                sourceMap.put(i, i);
            }

            Pipeline pipeline = Pipeline.create();

            pipeline.drawFrom(Sources.<Integer, Integer>map(MAP_NAME))
                    .map(e -> entry(e.getKey(), 5))
                    .drainTo(
                            Sinks.<Entry<Integer, Integer>, Integer>mapWithMerging(
                                    MAP_NAME,
                                    (oldValue, newValue) -> oldValue + newValue
                            )
                    );

            instance.newJob(pipeline).join();

            IMap<Integer, Integer> sinkMap = instance.getMap(MAP_NAME);
            System.out.println("Sink map size: " + sinkMap.size());
            System.out.println("Sink map entries: ");
            sinkMap.forEach((k, v) -> System.out.println(k + " - " + v));

        } finally {
            Jet.shutdownAll();
        }

    }
}
