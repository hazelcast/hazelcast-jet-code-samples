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

/**
 * Demonstrates the usage of Hazelcast IMap as source and sink
 * with the Pipeline API. This will take the contents of one map
 * and write it into another map.
 */
public class MapSourceAndSink {

    private static final int ITEM_COUNT = 10;
    private static final String SOURCE_NAME = "source";
    private static final String SINK_NAME = "sink";

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();

        try {
            IMap<Integer, Integer> sourceMap = jet.getMap(SOURCE_NAME);
            for (int i = 0; i < ITEM_COUNT; i++) {
                sourceMap.put(i, i);
            }

            Pipeline p = Pipeline.create();
            p.drawFrom(Sources.<Integer, Integer>map(SOURCE_NAME))
             .drainTo(Sinks.map(SINK_NAME));
            jet.newJob(p).join();

            System.out.println("Sink map entries: " + jet.getMap(SINK_NAME).entrySet());
        } finally {
            Jet.shutdownAll();
        }

    }
}
