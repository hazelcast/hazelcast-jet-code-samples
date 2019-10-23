/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.map.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import trades.GenerateTradesP;
import trades.TickerInfo;
import trades.Trade;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.processor.Processors.mapUsingServiceAsyncP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.pipeline.ServiceFactories.iMapService;

/**
 * This sample shows, how to enrich batch or stream of items with additional
 * information by matching them by key. This version shows how to use {@link
 * IMap} from Hazelcast IMDG.
 * <p>
 * {@code IMap} has an advantage in the ability to update the map, however it
 * does have small performance penalty. It is suitable if it is managed
 * separately from the job.
 * <p>
 * The DAG is as follows:
 * <pre>{@code
 * +------------------+
 * |  Trades source   |
 * +---------+--------+
 *           |
 *           | Trade
 *           |
 *     +-----v------+
 *     | Enrichment |
 *     +-----+------+
 *           |
 *           | Tuple2{trade, tradeInfo}
 *           |
 *     +-----v-----+
 *     |   Sink    |
 *     +-----------+
 * }</pre>
 */
public class IMapEnrichment {

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            TickerInfo.populateMap(instance.getHazelcastInstance().getMap("tickersInfo"));

            DAG dag = new DAG();

            Vertex tradesSource = dag.newVertex("tradesSource", GenerateTradesP::new);
            Vertex enrichment = dag.newVertex("enrichment", mapUsingServiceAsyncP(
                    iMapService("tickersInfo"),
                    Object::hashCode, // function to extract keys for the snapshot
                    (IMap<String, TickerInfo> map, Trade item) -> map.getAsync(item.getTicker()).toCompletableFuture()
                            .thenApply(ti -> tuple2(item, ti))));
            Vertex sink = dag.newVertex("sink", DiagnosticProcessors.writeLoggerP());

            tradesSource.localParallelism(1);

            dag
               .edge(between(tradesSource, enrichment))
               .edge(between(enrichment, sink));

            instance.newJob(dag).join();
        } finally {
            Jet.shutdownAll();
        }
    }
}
