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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.map.journal.EventJournalMapEvent;
import com.hazelcast.nio.Address;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A DAG which streams events generated for IMap from a remote Hazelcast cluster.
 * Events which are not {@code EntryEventType.ADDED} filtered out.
 * Values are extracted from the event and emitted to downstream
 * which is an IList sink in Jet cluster.
 */
public class StreamRemoteEventJournal {

    private static final String MAP_NAME = "map";
    private static final String LIST_NAME = "list";

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        Config config = getConfig();
        HazelcastInstance hazelcast = Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();

        try {
            DAG dag = new DAG();

            ClientConfig clientConfig = new ClientConfig();
            clientConfig.getNetworkConfig().addAddress(getHostPortPair(hazelcast));
            clientConfig.setGroupConfig(config.getGroupConfig());

            Vertex source = dag.newVertex("source",
                    SourceProcessors.streamRemoteMapP(MAP_NAME,
                            clientConfig,
                            e -> e.getType() == EntryEventType.ADDED,
                            EventJournalMapEvent::getNewValue, false));
            Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP(LIST_NAME));

            dag.edge(Edge.between(source, sink));

            Future<Void> future = jet.newJob(dag).getFuture();

            IMap<Integer, Integer> map = hazelcast.getMap(MAP_NAME);
            for (int i = 0; i < 1000; i++) {
                map.set(i, i);
            }

            TimeUnit.SECONDS.sleep(3);

            System.out.println(jet.getList(LIST_NAME).size());
            future.cancel(true);
        } finally {
            Hazelcast.shutdownAll();
            Jet.shutdownAll();
        }

    }

    private static Config getConfig() {
        Config config = new Config();
        // Add an event journal config for map which has custom capacity of 1000 (default 10_000)
        // and time to live seconds as 10 seconds (default 0 which means infinite)
        config.addEventJournalConfig(new EventJournalConfig().setEnabled(true)
                                                             .setMapName(MAP_NAME)
                                                             .setCapacity(1000)
                                                             .setTimeToLiveSeconds(10));
        return config;
    }

    private static String getHostPortPair(HazelcastInstance hazelcast) {
        Address address = hazelcast.getCluster().getLocalMember().getAddress();
        return address.getHost() + ":" + address.getPort();
    }

}
