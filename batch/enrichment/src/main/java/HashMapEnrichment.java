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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.samples.enrichment.GenerateTradesP;
import com.hazelcast.jet.samples.enrichment.SquashP;
import com.hazelcast.jet.samples.enrichment.TickerInfo;
import com.hazelcast.jet.samples.enrichment.Trade;

import java.util.Arrays;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.Processors.filter;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.Processors.writeLogger;
import static com.hazelcast.jet.function.DistributedFunctions.alwaysTrue;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;

/**
 * This sample shows, how to enrich batch or stream of items with additional
 * information by matching them by key. This version first builds a HashMap
 * that is distributed to {@link HashJoinP}.
 * <p>
 * The {@link HashJoinP} expects enrichment table on input ordinal 0 and items
 * to enrich on all other ordinals. The edge at ordinal 0 must have {@link
 * com.hazelcast.jet.Edge#priority(int) priority} set to -1 to ensure, that
 * items on this edge are processed before items to enrich.
 * <p>
 * The {@code readTickerInfoMap} reads the items in distributed way. In order
 * to have full copy on each member we need to broadcast and distribute to a
 * processor with local parallelism of 1. From here, we partition the table to
 * local processors, so that each can process part of the data. Items from
 * "Trades source" to "Joiner" are partitioned locally by the same key.
 * <p>
 * The DAG is as follows:
 * <pre>{@code
 *                                    +--------------------+
 *                                    | Read tickerInfoMap |
 *                                    +---------+----------+
 *                                              |
 *                                              | TickerInfo
 *                                              |  (broadcast, distributed edge)
 *                                      +-------+-------+
 *                                      | Pass through  |
 *                                      +-------+-------+ (localParallelism = 1)
 *                                              |
 * +-----------------+                          | TickerInfo
 * |  Trades source  |                          |  (local partitioned edge)
 * +--------+--------+                 +--------+--------+
 *          |                          |  Squash to map  |
 *          | Trade                    +--------+--------+
 *          |  (local partitioned edge)         |
 * +--------+--------+                          | Map<ticker, tickerInfo>
 * |      Joiner     +--------------------------+  (one-to-one, priority edge)
 * +--------+--------+
 *          |
 *          | Object[]{trade, tradeInfo}
 *          |
 * +--------+--------+
 * |      Sink       |
 * +-----------------+
 * }</pre>
 */
public class HashMapEnrichment {

    private static final String TICKER_INFO_MAP_NAME = "tickerInfoMap";

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            TickerInfo.populateMap(instance.getMap(TICKER_INFO_MAP_NAME));

            DAG dag = new DAG();

            Vertex tradesSource = dag.newVertex("tradesSource", GenerateTradesP::new)
                    .localParallelism(1);
            Vertex readTickerInfoMap = dag.newVertex("readTickerInfoMap", readMap(TICKER_INFO_MAP_NAME));
            Vertex passThrough = dag.newVertex("passThrough", filter(alwaysTrue()))
                                    .localParallelism(1);
            Vertex squashToMap = dag.newVertex("squashToMap", SquashP::new);
            Vertex joiner = dag.newVertex("joiner", () -> new HashJoinP<>(Trade::getTicker));
            Vertex sink = dag.newVertex("sink", writeLogger(o -> Arrays.toString((Object[]) o)))
                    .localParallelism(1);

            dag.edge(between(readTickerInfoMap, passThrough)
                    .broadcast()
                    .distributed())
               .edge(between(passThrough, squashToMap)
                    .partitioned(entryKey()))
               .edge(from(squashToMap).to(joiner, 0)
                    .oneToMany()
                    .priority(-1))
               .edge(from(tradesSource).to(joiner, 1)
                    .partitioned(Trade::getTicker))
               .edge(between(joiner, sink));

            instance.newJob(dag).execute().get();
        } finally {
            Jet.shutdownAll();
        }
    }


}
