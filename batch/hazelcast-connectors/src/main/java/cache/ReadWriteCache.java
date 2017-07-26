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

package cache;

import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.jet.processor.Sources;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.stream.IStreamCache;

import java.util.Map.Entry;

import static com.hazelcast.jet.Edge.between;

/**
 * A DAG which reads from a Hazelcast ICache,
 * converts the key/value to string,
 * and writes to another Hazelcast ICache
 */
public class ReadWriteCache {

    private static final int ITEM_COUNT = 10;
    private static final String SOURCE_CACHE_NAME = "sourceCache";
    private static final String SINK_CACHE_NAME = "sinkCache";

    public static void main(String[] args) throws Exception {
        JetConfig jetConfig = new JetConfig();
        jetConfig.getHazelcastConfig().addCacheConfig(new CacheSimpleConfig().setName("*Cache"));
        JetInstance instance = Jet.newJetInstance(jetConfig);

        try {
            IStreamCache<Integer, Integer> sourceCache = instance.getCacheManager().getCache(SOURCE_CACHE_NAME);
            for (int i = 0; i < ITEM_COUNT; i++) {
                sourceCache.put(i, i);
            }

            DAG dag = new DAG();

            Vertex source = dag.newVertex("source", Sources.readCache(SOURCE_CACHE_NAME));
            Vertex transform = dag.newVertex("transform", Processors.map((Entry<Integer, Integer> e)
                    -> Util.entry(e.getKey().toString(), e.getValue().toString())));
            Vertex sink = dag.newVertex("sink", Sinks.writeCache(SINK_CACHE_NAME));

            dag.edge(between(source, transform));
            dag.edge(between(transform, sink));

            instance.newJob(dag).join();

            IStreamCache<String, String> sinkCache = instance.getCacheManager().getCache(SINK_CACHE_NAME);
            System.out.println("Sink cache size: " + sinkCache.size());
            System.out.println("Sink cache entries: ");
            for (int i = 0; i < ITEM_COUNT; i++) {
                System.out.println(sinkCache.get(Integer.toString(i)));
            }
        } finally {
            Jet.shutdownAll();
        }
    }
}
