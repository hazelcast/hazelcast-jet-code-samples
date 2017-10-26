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
 * Demonstrates the usage of Hazelcast IMap as source and sink
 * with the Pipeline API.
 */
public class MapSourceSink {

    private static final int ITEM_COUNT = 10;
    private static final String SOURCE_MAP_NAME = "sourceMap";
    private static final String SINK_MAP_NAME = "sinkMap";

    public static void main(String[] args) throws Exception {

        JetInstance instance = Jet.newJetInstance();

        try {
            IMap<Integer, Integer> sourceMap = instance.getMap(SOURCE_MAP_NAME);
            for (int i = 0; i < ITEM_COUNT; i++) {
                sourceMap.put(i, i);
            }

            Pipeline pipeline = Pipeline.create();

            pipeline.drawFrom(Sources.map(SOURCE_MAP_NAME,
                    (Entry<Integer, Integer> e) -> e.getValue() != 0,
                    e -> entry(e.getKey().toString(), e.getValue().toString())))
                    .drainTo(Sinks.map(SINK_MAP_NAME));

            instance.newJob(pipeline).join();

            IMap<String, String> sinkMap = instance.getMap(SINK_MAP_NAME);
            System.out.println("Sink map size: " + sinkMap.size());
            System.out.println("Sink map entries: ");
            sinkMap.forEach((k, v) -> System.out.println(k + " - " + v));

        } finally {
            Jet.shutdownAll();
        }

    }
}
