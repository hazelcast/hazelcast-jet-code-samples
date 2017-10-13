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

import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import trades.GenerateTradesP;
import trades.TickerInfo;
import trades.Trade;

import javax.annotation.Nonnull;
import java.util.Arrays;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;

/**
 * This sample shows, how to enrich batch or stream of items with additional
 * information by matching them by key. This version shows how to use {@link
 * ReplicatedMap} from Hazelcast IMDG.
 * <p>
 * {@code ReplicatedMap} has an advantage in the ability to update the map,
 * however it does have small performance penalty. It is suitable if it is
 * managed separately from the job.
 * <p>
 * The {@link HashJoinP} expects enrichment table on input ordinal 0 and items
 * to enrich on all other ordinals. The edge at ordinal 0 must have {@link
 * Edge#priority(int) priority} set to -1 to ensure, that items on this edge
 * are processed before items to enrich.
 * <p>
 * The DAG is as follows:
 * <pre>{@code
 * +------------------+
 * |  Trades source   |       +--------------------+
 * +---------+--------+       | Send ReplicatedMap |
 *           |                +---------+----------+               |
 *           | Trade                    |
 *           |                          | Map<ticker, tickerInfo>
 *     +-----v-----+                    |  (broadcast edge)
 *     |  Joiner   <--------------------+
 *     +-----+-----+
 *           |
 *           | Object[]{trade, tradeInfo}
 *           |
 *     +-----v-----+
 *     |   Sink    |
 *     +-----------+
 * }</pre>
 */
public class ReplicatedMapEnrichment {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            TickerInfo.populateMap(instance.getHazelcastInstance().getReplicatedMap("tickersInfo"));

            DAG dag = new DAG();

            Vertex tradesSource = dag.newVertex("tradesSource", GenerateTradesP::new);
            Vertex tickersInfoSource = dag.newVertex("tickersInfoSource", () -> new SendReplicatedMapP("tickersInfo"));
            Vertex joiner = dag.newVertex("joiner", () -> new HashJoinP<>(Trade::getTicker));
            Vertex sink = dag.newVertex("sink", DiagnosticProcessors.writeLogger(o -> Arrays.toString((Object[]) o)));

            tradesSource.localParallelism(1);
            tickersInfoSource.localParallelism(1);

            dag.edge(from(tickersInfoSource).to(joiner, 0)
                    .broadcast()
                    .priority(-1))
               .edge(from(tradesSource).to(joiner, 1))
               .edge(between(joiner, sink));

            instance.newJob(dag).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Processor that emits {@link ReplicatedMap} as a single item. It must
     * be used with {@link Vertex#localParallelism(int) local parallelism} of 1
     * and followed by an {@link Edge#broadcast() broadcast} edge so that each
     * downstream processor gets one instance.
     */
    private static final class SendReplicatedMapP extends AbstractProcessor {
        private final String mapName;
        private ReplicatedMap map;

        private SendReplicatedMapP(String mapName) {
            this.mapName = mapName;
        }

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            map = context.jetInstance().getHazelcastInstance().getReplicatedMap(mapName);
        }

        @Override
        public boolean complete() {
            return tryEmit(map);
        }
    }

}
