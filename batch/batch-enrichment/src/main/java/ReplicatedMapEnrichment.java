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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.ContextFactories;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import trades.TickerInfo;
import trades.Trade;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 * This sample shows, how to enrich batch or stream of items with additional
 * information by matching them by key. This version shows how to use {@link
 * com.hazelcast.core.ReplicatedMap} from Hazelcast IMDG.
 * <p>
 * {@code ReplicatedMap} has an advantage in the ability to update the map while
 * the job is running, however it does have a very small performance penalty. It
 * is suitable if it is managed separately from the job or when used in a
 * streaming job and you need to mutate it over time.
 * <p>
 * The {@code ReplicatedMap} can be even easily replaced with an {@code IMap}:
 * it has considerably higher performance penalty, but might be suitable if the
 * volume of the data is high.
 */
public class ReplicatedMapEnrichment {

    private static final String TICKER_INFO_MAP_NAME = "tickerInfoMap";
    private static final String TRADES_LIST_NAME = "tickerInfoMap";

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
         JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            TickerInfo.populateMap(instance.getHazelcastInstance().getReplicatedMap(TICKER_INFO_MAP_NAME));
            Trade.populateTrades(instance.getList(TRADES_LIST_NAME));

            Pipeline p = Pipeline.create();
            BatchSource<Trade> tradesSource = Sources.list(TRADES_LIST_NAME);

            p.drawFrom(tradesSource)
             .mapUsingContext(ContextFactories.replicatedMapContext(TICKER_INFO_MAP_NAME),
                     (map, trade) -> tuple2(trade, map.get(trade.getTicker())))
             .drainTo(Sinks.logger());

            instance.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }
}
