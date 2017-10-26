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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import trades.GenerateTradesP;
import trades.TickerInfo;
import trades.Trade;

import java.util.Arrays;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.writeLoggerP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;

/**
 * This sample shows, how to enrich batch or stream of items with additional
 * information by matching them by key. This version first builds a HashMap
 * that is distributed to {@link HashJoinP}.
 * <p>
 * The {@link HashJoinP} expects enrichment table on input ordinal 0 and items
 * to enrich on all other ordinals. The edge at ordinal 0 must have {@link
 * com.hazelcast.jet.core.Edge#priority(int) priority} set to -1 to ensure, that
 * items on this edge are processed before items to enrich.
 * <p>
 * The {@code readTickerInfoMap} reads the items in distributed way. In order
 * to have full copy on each node we need to broadcast and distribute to a
 * processor with local parallelism of 1. From there, we broadcast the same
 * {@code Map} instance to all local processors. Sending locally does not copy
 * the object, thus this will not increase memory usage.
 * <p>
 * The DAG is as follows:
 * <pre>{@code
 *                             +--------------------+
 *                             | Read tickerInfoMap |
 *                             +---------+----------+
 *                                       |
 * +-----------------+                   | TickerInfo
 * |  Trades source  |                   |  (broadcast, distributed edge)
 * +--------+--------+          +--------+--------+
 *          |                   |  Squash to map  |
 *          | Trade             +---------------+-+    (localParallelism = 1)
 *          |  (local edge)                     |
 * +--------+--------+                          |
 * |      Joiner     +--------------------------+
 * +--------+--------+                    Map<ticker, tickerInfo>
 *          |                              (local, broadcast, priority edge)
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

            Vertex tradesSource = dag.newVertex("tradesSource", GenerateTradesP::new);
            Vertex readTickerInfoMap = dag.newVertex("readTickerInfoMap", readMapP(TICKER_INFO_MAP_NAME));
            Vertex collectToMap = dag.newVertex("collectToMap",
                    Processors.aggregateP(AggregateOperations.toMap(entryKey(), entryValue())));
            Vertex hashJoin = dag.newVertex("hashJoin", () -> new HashJoinP<>(Trade::getTicker));
            Vertex sink = dag.newVertex("sink", writeLoggerP(o -> Arrays.toString((Object[]) o)));

            tradesSource.localParallelism(1);
            collectToMap.localParallelism(1);
            sink.localParallelism(1);

            dag.edge(between(readTickerInfoMap, collectToMap)
                    .broadcast()
                    .distributed())
               .edge(from(collectToMap).to(hashJoin, 0)
                    .broadcast()
                    .priority(-1))
               .edge(from(tradesSource).to(hashJoin, 1)
                    .partitioned(Trade::getTicker))
               .edge(between(hashJoin, sink));

            instance.newJob(dag).join();
        } finally {
            Jet.shutdownAll();
        }
    }
}
