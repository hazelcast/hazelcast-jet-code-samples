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
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import trades.GenerateTradesP;
import trades.TickerInfo;
import trades.Trade;

import java.util.Map.Entry;

/**
 * This sample shows, how to enrich batch or stream of items with additional
 * information by matching them by key. The enrichment data are stored in
 * {@code tickerInfoMap}. The map is read and the items from batch of trades
 * are enriched with the information from the map. The resulting type is
 * {@code Tuple2(Trade, TickerInfo)}.
 */
public class HashMapEnrichment {

    private static final String TICKER_INFO_MAP_NAME = "tickerInfoMap";

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            TickerInfo.populateMap(instance.getMap(TICKER_INFO_MAP_NAME));

            Pipeline p = Pipeline.create();
            BatchSource<Trade> tradesSource = Sources.batchFromProcessor("tradesSource",
                    ProcessorMetaSupplier.of(GenerateTradesP::new, 1));
            BatchSource<Entry<String, TickerInfo>> tickerInfoSource = Sources.map(TICKER_INFO_MAP_NAME);

            p.drawFrom(tradesSource)
             .hashJoin(p.drawFrom(tickerInfoSource), JoinClause.joinMapEntries(Trade::getTicker), Tuple2::tuple2)
             .drainTo(Sinks.logger());

            instance.newJob(p).join();
        } finally {
            Jet.shutdownAll();
        }
    }
}
