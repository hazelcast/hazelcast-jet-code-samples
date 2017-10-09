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

import com.hazelcast.cache.ICache;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;

import javax.cache.configuration.FactoryBuilder;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.event.CacheEntryCreatedListener;
import java.io.Serializable;

import static com.hazelcast.jet.Util.entry;

/**
 * Demonstrates the usage of Hazelcast ICache as source and sink
 * from/to a remote cluster with the Pipeline API.
 */
public class ReadWriteRemoteCache {

    static final String SOURCE_CACHE_NAME = "sourceCache";
    static final String SINK_CACHE_NAME = "sinkCache";

    public static void main(String[] args) throws Exception {
        RemoteNode remoteNode = new RemoteNode();
        remoteNode.start();

        JetInstance instance = Jet.newJetInstance();

        try {
            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getGroupConfig().setName("dev").setPassword("dev-pass");
            clientConfig.getNetworkConfig().addAddress("localhost:6701");

            Pipeline pipeline = Pipeline.create();

            pipeline.drawFrom(Sources.readRemoteCache(SOURCE_CACHE_NAME, clientConfig))
                    .map(e -> entry(e.getKey().toString(), e.getValue().toString()))
                    .drainTo(Sinks.writeRemoteCache(SINK_CACHE_NAME, clientConfig));

            instance.newJob(pipeline).join();
        } finally {
            Jet.shutdownAll();
            remoteNode.stop();
        }
    }

    private static class RemoteNode {

        private static final int ITEM_COUNT = 10;

        private HazelcastInstance instance;

        void start() throws Exception {
            Config config = new Config();
            config.getNetworkConfig().setPort(6701);
            instance = Hazelcast.newHazelcastInstance(config);
            ICache<Integer, Integer> sourceCache = instance.getCacheManager().getCache(SOURCE_CACHE_NAME);
            for (int i = 0; i < ITEM_COUNT; i++) {
                sourceCache.put(i, i);
            }
            ICache<String, String> sinkCache = instance.getCacheManager().getCache(SINK_CACHE_NAME);
            sinkCache.registerCacheEntryListener(new MutableCacheEntryListenerConfiguration<>(
                    FactoryBuilder.factoryOf(
                            (Serializable & CacheEntryCreatedListener<String, String>)
                                    iterable -> iterable.forEach(e ->
                                            System.out.println("Entry added to sink: " + e.getKey() + " - " + e.getValue()))),
                    null, false, true
            ));
        }

        void stop() {
            instance.shutdown();
        }
    }
}
