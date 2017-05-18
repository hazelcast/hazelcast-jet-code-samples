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
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.samples.enrichment.HashJoinP;

import javax.annotation.Nonnull;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.Processors.writeLogger;

/**
 * This sample shows, how to enrich batch or stream of items with additional
 * information by matching them by key. It has two versions:
 * <ol>
 *     <li>Distribute a {@code Map} to all processor instances and use it to
 *     look up information.
 *     <li>Use {@link ReplicatedMap} from Hazelcast IMDG
 * </ol>
 *
 * {@code ReplicatedMap} has an advantage in the ability to update the map,
 * however it does have small performance penalty.
 * <p>
 * The {@link HashJoinP} expects enrichment table on input ordinal 0 and items
 * to enrich on all other ordinals. The edge at ordinal 0 must have {@link
 * Edge#priority(int) priority} set to -1 to ensure, that items on this edge
 * are processed before items to enrich.
 * <p>
 * The DAG for case 1 is:
 * <pre>{@code
 *                                  +----------------+
 *                                  |  Read source   |
 *                                  +---------+------+
 *                                            |
 *                                            |
 *                                            |
 * +------------------+             +---------v------+
 * |  Trades source   |             |  Sqash to map  |
 * +---------+--------+             +---------+------+
 *           |                                |
 *           |(Trade)                         |
 *           |                                |
 *     +-----v-----+  Map<ticker, tickerInfo> |
 *     |  Joiner   <--------------------------+
 *     +-----+-----+
 *           |
 *           |
 *           |
 *     +-----v-----+
 *     |   Sink    |
 *     +-----------+
 * }</pre>
 *
 * The DAG for case 2 is:
 * <pre>{@code
 * +------------------+             +--------------------+
 * |  Trades source   |             | Send ReplicatedMap |
 * +---------+--------+             +---------+----------+
 *           |                                |
 *           |(Trade)                         |
 *           |                                |
 *     +-----v-----+  Map<ticker, tickerInfo> |
 *     |  Joiner   <--------------------------+
 *     +-----+-----+
 *           |
 *           |
 *           |
 *     +-----v-----+
 *     |   Sink    |
 *     +-----------+
 * }</pre>
 */
public class Enrichment {

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            usage();
        }

        boolean useReplicatedMap = "replicatedMap".equalsIgnoreCase(args[0]);
        if (!useReplicatedMap && !"hashMap".equalsIgnoreCase(args[0])) {
            usage();
        }

        System.setProperty("hazelcast.logging.type", "log4j");

        JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            DAG dag = new DAG();

            Vertex tradesSource = dag.newVertex("tradesSource", GenerateTradesP::new)
                    .localParallelism(1);
            Vertex joiner = dag.newVertex("joiner", () -> new HashJoinP<>(Trade::getTicker));
            Vertex sink = dag.newVertex("sink", writeLogger(o -> Arrays.toString((Object[]) o)));

            // add the enrichment info source vertex to the DAG
            Vertex tickersInfoSource;
            if (useReplicatedMap) {
                populateMap(instance.getHazelcastInstance().getReplicatedMap("tickersInfo"));
                tickersInfoSource = dag.newVertex("tickersInfoSource", () -> new SendReplicatedMapP("tickersInfo"))
                                       .localParallelism(1);
            } else {
                populateMap(instance.getMap("tickersInfo"));
                Vertex mapReader = dag.newVertex("mapReader", readMap("tickersInfo"));
                tickersInfoSource = dag.newVertex("squashToMap", SquashP::new)
                                       .localParallelism(1);

                dag.edge(between(mapReader, tickersInfoSource));
            }

            dag.edge(from(tickersInfoSource).to(joiner, 0)
                    .broadcast()
                    .priority(-1))
               .edge(from(tradesSource).to(joiner, 1))
               .edge(between(joiner, sink));

            instance.newJob(dag).execute().get();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static void usage() {
        System.err.println("Usage:");
        System.err.println("  " + Enrichment.class.getSimpleName() + " (replicatedMap|hashMap)");
        System.exit(1);
    }

    private static void populateMap(Map<String, TickerInfo> map) {
        map.put("AAAP", new TickerInfo("AAAP", "Advanced Accelerator Applications S.A. - American Depositary Shares"));
        map.put("BABY", new TickerInfo("BABY", "Natus Medical Incorporated - Common Stock"));
        map.put("CA", new TickerInfo("CA", "CA Inc. - Common Stock"));
    }

    /**
     * DTO for a trade.
     */
    private static final class Trade {
        private final long time;
        private final String ticker;
        private final int quantity;
        private final int price; // in cents

        Trade(long time, String ticker, int quantity, int price) {
            this.time = time;
            this.ticker = ticker;
            this.quantity = quantity;
            this.price = price;
        }

        public long getTime() {
            return time;
        }

        public String getTicker() {
            return ticker;
        }

        public int getQuantity() {
            return quantity;
        }

        public int getPrice() {
            return price;
        }

        @Override
        public String toString() {
            return "Trade{time=" + time + ", ticker='" + ticker + '\'' + ", quantity=" + quantity + ", price=" + price + '}';
        }
    }

    /**
     * DTO for additional information about the ticker.
     */
    private static final class TickerInfo implements Serializable {
        public final String symbol;
        public final String securityName;

        TickerInfo(String symbol, String securityName) {
            this.symbol = symbol;
            this.securityName = securityName;
        }

        @Override
        public String toString() {
            return "TickerInfo{symbol='" + symbol + '\'' + ", securityName='" + securityName + '\'' + '}';
        }
    }

    /**
     * Generate fixed number of sample trades. Note that every instance of this
     * processor will emit the same trades.
     */
    private static final class GenerateTradesP extends AbstractProcessor {
        @Override
        public boolean complete() {
            emit(new Trade(1, "AAAP", 1, 1));
            emit(new Trade(1, "BABY", 1, 1));
            emit(new Trade(1, "CA", 1, 1));
            emit(new Trade(2, "AAAP", 1, 1));
            emit(new Trade(2, "BABY", 1, 1));
            emit(new Trade(2, "CA", 1, 1));
            emit(new Trade(3, "AAAP", 1, 1));
            emit(new Trade(3, "BABY", 1, 1));
            emit(new Trade(3, "CA", 1, 1));
            return true;
        }

        @Override
        public boolean isCooperative() {
            return false;
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

    /**
     * Processor, that takes {@link Entry} items and emits them as single
     * {@link Map} item.
     */
    private static final class SquashP extends AbstractProcessor {
        Map map = new HashMap();

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            Entry e = (Entry) item;
            map.put(e.getKey(), e.getValue());
            return true;
        }

        @Override
        public boolean complete() {
            return tryEmit(map);
        }
    }
}
