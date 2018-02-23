/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.IList;
import com.hazelcast.jet.GenericPredicates;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.projection.Projections;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.Map.Entry;

import static com.hazelcast.core.EntryEventType.ADDED;
import static com.hazelcast.core.EntryEventType.REMOVED;
import static com.hazelcast.core.EntryEventType.UPDATED;
import static com.hazelcast.jet.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.Util.entry;

public class SourcesSinksRefMan {
    static void basicIMapSourceSink() {
        Pipeline p = Pipeline.create();
        BatchStage<Entry<String, Long>> stage =
                p.drawFrom(Sources.<String, Long>map("inputMap"));
        stage.drainTo(Sinks.map("outputMap"));
    }

    static void basicICacheSourceSink() {
        Pipeline p = Pipeline.create();
        BatchStage<Entry<String, Long>> stage =
                p.drawFrom(Sources.<String, Long>cache("inputCache"));
        stage.drainTo(Sinks.cache("outputCache"));
    }

    static void mapCacheInAnotherCluster() {
        ClientConfig cfg = new ClientConfig();
        cfg.getGroupConfig().setName("myGroup").setPassword("pAssw0rd");
        cfg.getNetworkConfig().addAddress("node1.mydomain.com", "node2.mydomain.com");

        Pipeline p = Pipeline.create();
        BatchStage<Entry<String, Long>> fromMap =
                p.drawFrom(Sources.<String, Long>remoteMap("inputMap", cfg));
        BatchStage<Entry<String, Long>> fromCache =
                p.drawFrom(Sources.<String, Long>remoteCache("inputCache", cfg));
        fromMap.drainTo(Sinks.remoteCache("outputCache", cfg));
        fromCache.drainTo(Sinks.remoteMap("outputMap", cfg));
    }

    static void mapCacheWithFilteringAndMapping() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Integer, String, Person>remoteMap(
                "inputMap", clientConfig(),
                e -> e.getValue().getAge() > 21,
                e -> e.getValue().getAge()));
    }

    @SuppressWarnings("unchecked")
    static void mapCacheWithNativePredicateAndProjection() {
        Pipeline p = Pipeline.create();
        p.<Integer>drawFrom(Sources.remoteMap(
                "inputMap", clientConfig(),
                GenericPredicates.greaterThan("value", 21),
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

        StreamStage<Entry<String, Long>> fromMap = p.drawFrom(
                Sources.<String, Long>mapJournal("inputMap", START_FROM_CURRENT));
        StreamStage<Entry<String, Long>> fromCache = p.drawFrom(
                Sources.<String, Long>cacheJournal("inputCache", START_FROM_CURRENT));

        StreamStage<Entry<String, Long>> fromRemoteMap = p.drawFrom(
                Sources.<String, Long>remoteMapJournal("inputMap", clientConfig(), START_FROM_CURRENT));
        StreamStage<Entry<String, Long>> fromRemoteCache = p.drawFrom(
                Sources.<String, Long>remoteCacheJournal("inputCache", clientConfig(), START_FROM_CURRENT));

        EnumSet<EntryEventType> evTypesToAccept = EnumSet.of(ADDED, REMOVED, UPDATED);
        StreamStage<Entry<String, Long>> stage = p.drawFrom(
                Sources.<Entry<String, Long>, String, Long>mapJournal("inputMap",
                        e -> evTypesToAccept.contains(e.getType()),
                        e -> entry(e.getKey(), e.getNewValue()),
                        START_FROM_CURRENT));
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
        BatchStage<Object> stage = p.drawFrom(Sources.remoteList("inputlist", clientConfig));
        stage.drainTo(Sinks.remoteList("resultList", clientConfig));
    }

    @Nonnull
    private static ClientConfig clientConfig() {
        throw new UnsupportedOperationException("mock implementation");
    }
}

class Person {
    private int age;

    int getAge() {
        return age;
    }
}
