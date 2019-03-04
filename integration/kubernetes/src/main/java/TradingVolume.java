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

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.function.PredicateEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.server.JetBootstrap;
import com.hazelcast.map.journal.EventJournalMapEvent;
import support.TradeGenerator;

import java.util.Map.Entry;

import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_CURRENT;
import static support.Util.startConsolePrinterThread;
import static support.Util.stopConsolePrinterThread;

/**
 * Showcases the Rolling Aggregation operator of the Pipeline API.
 * <p>
 * The sample starts a thread that randomly generates trade events and
 * puts them into a Hazelcast Map. The Jet job receives these events and
 * calculates for each stock the rolling sum of money that changed hands
 * trading it. The sample also starts a GUI window that visualizes the
 * rising traded volume of all stocks.
 */
public class TradingVolume {

    private static final String TRADES_MAP_NAME = "trades";
    private static final String VOLUME_MAP_NAME = "volume-by-stock";
    private static final int TRADES_PER_SEC = 3_000;
    private static final int NUMBER_OF_TICKERS = 20;


    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Entry<String, Integer>, Integer, Entry<String, Integer>>mapJournal(TRADES_MAP_NAME,
                PredicateEx.alwaysTrue(), EventJournalMapEvent::getNewValue, START_FROM_CURRENT))
         .withoutTimestamps()
         .groupingKey(Entry::getKey)
         .rollingAggregate(summingLong(Entry::getValue))
         .drainTo(Sinks.map(VOLUME_MAP_NAME));
        return p;
    }

    public static void main(String[] args) {
        JetInstance jet = JetBootstrap.getInstance();

        jet.getHazelcastInstance().getConfig().addEventJournalConfig(new EventJournalConfig()
                .setMapName(TRADES_MAP_NAME)
                .setCapacity(TRADES_PER_SEC * 10));

        startConsolePrinterThread(jet, VOLUME_MAP_NAME);
        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.setName("Trade Volume");
            jet.newJob(buildPipeline(), jobConfig);
            TradeGenerator.generate(NUMBER_OF_TICKERS, jet.getMap(TRADES_MAP_NAME), TRADES_PER_SEC);
        } finally {
            stopConsolePrinterThread();
            Jet.shutdownAll();
        }
    }
}
