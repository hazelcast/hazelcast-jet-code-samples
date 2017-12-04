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
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import java.util.Map.Entry;

import static com.hazelcast.jet.function.DistributedFunctions.entryKey;

/**
 * Demonstrates the usage of Hazelcast IMap as source and Hazelcast
 * IMap Entry Processor sink with the Pipeline API.
 * This will take the contents of one map and apply entry processor to
 * increment the values by 5.
 */
public class MapWithEntryProcessorSink {

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
                    .drainTo(
                            Sinks.mapWithEntryProcessor(
                                    MAP_NAME,
                                    entryKey(),
                                    (item) -> new IncrementEntryProcessor(5)
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

    static class IncrementEntryProcessor implements EntryProcessor<Integer, Integer> {

        private int incrementBy;

        IncrementEntryProcessor(int incrementBy) {
            this.incrementBy = incrementBy;
        }

        @Override
        public Object process(Entry<Integer, Integer> entry) {
            return entry.setValue(entry.getValue() + incrementBy);
        }

        @Override
        public EntryBackupProcessor<Integer, Integer> getBackupProcessor() {
            return null;
        }
    }
}
