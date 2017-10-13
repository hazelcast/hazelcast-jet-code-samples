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

import com.hazelcast.cache.ICache;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;

import static com.hazelcast.jet.Util.entry;

/**
 * Demonstrates the usage of Hazelcast ICache as source and sink
 * with the Pipeline API.
 */
public class CacheSourceSink {

    private static final int ITEM_COUNT = 10;
    private static final String SOURCE_CACHE_NAME = "sourceCache";
    private static final String SINK_CACHE_NAME = "sinkCache";

    public static void main(String[] args) throws Exception {

        JetInstance instance = Jet.newJetInstance();

        try {
            ICache<Integer, Integer> sourceCache = instance.getCacheManager().getCache(SOURCE_CACHE_NAME);
            for (int i = 0; i < ITEM_COUNT; i++) {
                sourceCache.put(i, i);
            }

            Pipeline pipeline = Pipeline.create();

            pipeline.drawFrom(Sources.readCache(SOURCE_CACHE_NAME))
                    .map(e -> entry(e.getKey().toString(), e.getValue().toString()))
                    .drainTo(Sinks.writeCache(SINK_CACHE_NAME));

            instance.newJob(pipeline).join();

            ICache<String, String> sinkCache = instance.getCacheManager().getCache(SINK_CACHE_NAME);
            System.out.println("Sink cache size: " + sinkCache.size());
            System.out.println("Sink cache entries: ");
            sinkCache.forEach(e -> System.out.println(e.getKey() + " - " + e.getValue()));

        } finally {
            Jet.shutdownAll();
        }

    }
}
