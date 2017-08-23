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

import com.hazelcast.config.Config;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.processor.Sinks;
import com.hazelcast.jet.processor.Sources;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.map.journal.EventJournalMapEvent;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * A DAG which streams events generated for IMap.
 * Events which are not {@code EntryEventType.ADDED} filtered out.
 * Values are extracted from the event and emitted to downstream
 * which is an IList sink.
 *
 */
public class StreamEventJournal {

    private static final String MAP_NAME = "map";
    private static final String LIST_NAME = "list";

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetConfig jetConfig = getJetConfig();
        JetInstance jet = Jet.newJetInstance(jetConfig);
        Jet.newJetInstance(jetConfig);

        try {
            DAG dag = new DAG();

            Vertex source = dag.newVertex("source",
                    Sources.streamMap(MAP_NAME,
                            e -> e.getType() == EntryEventType.ADDED,
                            EventJournalMapEvent::getNewValue, false));
            Vertex sink = dag.newVertex("sink", Sinks.writeList(LIST_NAME));

            dag.edge(Edge.between(source, sink));

            Future<Void> future = jet.newJob(dag).getFuture();

            IStreamMap<Integer, Integer> map = jet.getMap(MAP_NAME);
            for (int i = 0; i < 1000; i++) {
                map.set(i, i);
            }

            TimeUnit.SECONDS.sleep(3);

            System.out.println(jet.getList(LIST_NAME).size());
            future.cancel(true);
        } finally {
            Jet.shutdownAll();
        }

    }

    private static JetConfig getJetConfig() {
        Config config = new Config();
        // Add an event journal config for map which has custom capacity of 1000 (default 10_000)
        // and time to live seconds as 10 seconds (default 0 which means infinite)
        config.addEventJournalConfig(new EventJournalConfig().setEnabled(true)
                                                             .setMapName(MAP_NAME)
                                                             .setCapacity(1000)
                                                             .setTimeToLiveSeconds(10));
        return new JetConfig().setHazelcastConfig(config);
    }

}
