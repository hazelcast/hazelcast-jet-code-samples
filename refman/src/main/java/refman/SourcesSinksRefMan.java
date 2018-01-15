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

package refman;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.IList;
import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.projection.Projections;
import com.hazelcast.query.Predicates;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Map.Entry;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.core.WatermarkGenerationParams.noWatermarks;

public class SourcesSinksRefMan {
    static void basicIMapSourceSink() {
        Pipeline p = Pipeline.create();
        ComputeStage<Entry<String, Long>> stage =
                p.drawFrom(Sources.<String, Long>map("inputMap"));
        stage.drainTo(Sinks.map("outputMap"));
    }

    static void basicICacheSourceSink() {
        Pipeline p = Pipeline.create();
        ComputeStage<Entry<String, Long>> stage =
                p.drawFrom(Sources.<String, Long>cache("inputCache"));
        stage.drainTo(Sinks.cache("outputCache"));
    }

    static void mapCacheInAnotherCluster() {
        ClientConfig cfg = new ClientConfig();
        cfg.getGroupConfig().setName("myGroup").setPassword("pAssw0rd");
        cfg.getNetworkConfig().addAddress("node1.mydomain.com", "node2.mydomain.com");

        Pipeline p = Pipeline.create();
        ComputeStage<Entry<String, Long>> fromMap =
                p.drawFrom(Sources.<String, Long>remoteMap("inputMap", cfg));
        ComputeStage<Entry<String, Long>> fromCache =
                p.drawFrom(Sources.<String, Long>remoteCache("inputCache", cfg));
        fromMap.drainTo(Sinks.remoteCache("outputCache", cfg));
        fromCache.drainTo(Sinks.remoteMap("outputMap", cfg));
    }

    static void mapCacheWithFilteringAndMapping() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String, Person, Integer>remoteMap(
                "inputMap", clientConfig(),
                e -> e.getValue().getAge() > 21,
                e -> e.getValue().getAge()));
    }

    @SuppressWarnings("unchecked")
    static void mapCacheWithNativePredicateAndProjection() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<String, Person, Integer>remoteMap(
                "inputMap", clientConfig(),
                Predicates.greaterThan("value", 21),
                Projections.singleAttribute("value")));
    }

    static void eventJournalConfig() {
        JetConfig cfg = new JetConfig();

        cfg.getHazelcastConfig()
           .getMapEventJournalConfig("inputMap")
           .setEnabled(true)
           .setCapacity(1000)
           .setTimeToLiveSeconds(10);

        cfg.getHazelcastConfig()
           .getCacheEventJournalConfig("inputCache")
           .setEnabled(true)
           .setCapacity(1000)
           .setTimeToLiveSeconds(10);

        JetInstance jet = Jet.newJetInstance(cfg);
    }

    static void mapCacheEventJournal() {
        Pipeline p = Pipeline.create();

        ComputeStage<Entry<String, Long>> fromMap =
                p.drawFrom(Sources.<String, Long>mapJournal("inputMap", START_FROM_CURRENT, noWatermarks()));
        ComputeStage<Entry<String, Long>> fromCache =
                p.drawFrom(Sources.<String, Long>cacheJournal("inputCache", START_FROM_CURRENT, noWatermarks()));

        ComputeStage<Entry<String, Long>> fromRemoteMap = p.drawFrom(
                Sources.<String, Long>remoteMapJournal(
                        "inputMap", clientConfig(), START_FROM_CURRENT, noWatermarks())
        );
        ComputeStage<Entry<String, Long>> fromRemoteCache = p.drawFrom(
                Sources.<String, Long>remoteCacheJournal(
                        "inputCache", clientConfig(), START_FROM_CURRENT, noWatermarks())
        );
    }

    static void listSourceSink(JetInstance jet) {
        IList<Integer> inputList = jet.getList("inputList");
        for (int i = 0; i < 10; i++) {
            inputList.add(i);
        }

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Integer>list("inputList"))
         .map(i -> "item" + i)
         .drainTo(Sinks.list("resultList"));

        jet.newJob(p).join();

        IList<String> resultList = jet.getList("resultList");
        System.out.println("Results: " + new ArrayList<>(resultList));
    }

    static void remoteListSourceSink() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getGroupConfig().setName("myGroup").setPassword("pAssw0rd");
        clientConfig.getNetworkConfig().addAddress("node1.mydomain.com", "node2.mydomain.com");

        Pipeline p = Pipeline.create();
        ComputeStage<Object> stage = p.drawFrom(Sources.remoteList("inputlist", clientConfig));
        stage.drainTo(Sinks.remoteList("resultList", clientConfig));
    }

    @Nonnull
    private static ClientConfig clientConfig() {
        throw new UnsupportedOperationException("mock implementation");
    }

    @Nonnull
    private static <T> ComputeStage<T> someStage() {
        throw new UnsupportedOperationException("mock implementation");
    }
}

class Person {
    private int age;

    int getAge() {
        return age;
    }
}
