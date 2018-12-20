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

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.pipeline.JournalInitialPosition;
import com.hazelcast.map.journal.EventJournalMapEvent;
import tradegenerator.Trade;
import tradegenerator.TradeGenerator;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.EventTimePolicy.eventTimePolicy;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;
import static java.util.Collections.singletonList;

/**
 * A simple demonstration of Jet's continuous operators on an infinite
 * stream. Initially a Hazelcast map is populated with some stock
 * ticker names; the job reads the map and feeds the data to the vertex
 * that simulates an event stream coming from a stock market. The job
 * then computes the number of trades per ticker within a sliding window
 * of a given duration and dumps the results to a set of files.
 * <p>
 * This class shows a single-pipeline aggregation setup. For identical
 * functionality with two-pipeline setup, see {@link StockExchangeCoreApi}. For
 * discussion regarding single- and two-pipeline setup, see javadoc for the
 * {@link Processors} class.
 *
 * <pre>
 *             ---------------
 *            | ticker-source |
 *             ---------------
 *                    |
 *                 (ticker)
 *                    V
 *            -----------------
 *           | generate-trades |
 *            -----------------
 *                    |
 *    (timestamp, ticker, quantity, price)
 *                    V
 *           -------------------
 *          | insert-watermarks |
 *           -------------------
 *                    |
 *                    |              partitioned-distributed
 *                    V
 *       --------------------------
 *      | aggregate-to-sliding-win |
 *       --------------------------
 *                    |
 *          (ticker, time, count)
 *                    V
 *            ---------------
 *           | format-output |
 *            ---------------
 *                    |
 *          "time ticker count"
 *                    V
 *                 ------
 *                | sink |
 *                 ------
 * </pre>
 */
public class StockExchangeSingleStage {

    private static final String TRADES_MAP_NAME = "trades";
    private static final String OUTPUT_DIR_NAME = "stock-exchange";
    private static final int SLIDING_WINDOW_LENGTH_MILLIS = 1000;
    private static final int SLIDE_STEP_MILLIS = 10;
    private static final int TRADES_PER_SECOND = 4_000_000;
    private static final int JOB_DURATION = 10;

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().addEventJournalConfig(new EventJournalConfig()
                .setMapName(TRADES_MAP_NAME));
        JetInstance jet = Jet.newJetInstance(config);
        Jet.newJetInstance(config);
        try {
            jet.newJob(buildDag());
            TradeGenerator.generate(100, jet.getMap(TRADES_MAP_NAME), TRADES_PER_SECOND, JOB_DURATION);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag() {
        DAG dag = new DAG();
        SlidingWindowPolicy winPolicy = slidingWinPolicy(SLIDING_WINDOW_LENGTH_MILLIS, SLIDE_STEP_MILLIS);

        Vertex streamTrades = dag.newVertex("stream-trades",
                SourceProcessors.<Trade, Long, Trade>streamMapP(TRADES_MAP_NAME, DistributedPredicate.alwaysTrue(),
                        EventJournalMapEvent::getNewValue, JournalInitialPosition.START_FROM_OLDEST,
                        eventTimePolicy(
                                Trade::getTime,
                                limitingLag(3000),
                                winPolicy.frameSize(),
                                winPolicy.frameOffset(),
                                30000L
                        )));
        Vertex slidingWindow = dag.newVertex("aggregate-to-sliding-win",
                aggregateToSlidingWindowP(
                        singletonList((DistributedFunction<Trade, String>) Trade::getTicker),
                        singletonList((DistributedToLongFunction<Trade>) Trade::getTime),
                        TimestampKind.EVENT,
                        winPolicy,
                        counting(),
                        TimestampedEntry::fromWindowResult));
        Vertex formatOutput = dag.newVertex("format-output", formatOutput());
        Vertex sink = dag.newVertex("sink",
                writeFileP(OUTPUT_DIR_NAME, Object::toString, StandardCharsets.UTF_8, false));

        streamTrades.localParallelism(1);

        return dag
                .edge(between(streamTrades, slidingWindow)
                        .partitioned(Trade::getTicker, HASH_CODE)
                        .distributed())
                .edge(between(slidingWindow, formatOutput).isolated())
                .edge(between(formatOutput, sink));
    }

    private static DistributedSupplier<Processor> formatOutput() {
        return () -> {
            // If DateTimeFormatter was serializable, it could be created in
            // buildDag() and simply captured by the serializable lambda below. Since
            // it isn't, we need this long-hand approach that explicitly creates the
            // formatter at the use site instead of having it implicitly deserialized.
            DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
            return Processors.mapP((TimestampedEntry<String, Long> f) -> String.format("%s %5s %4d",
                    timeFormat.format(Instant.ofEpochMilli(f.getTimestamp()).atZone(ZoneId.systemDefault())),
                    f.getKey(), f.getValue())).get();
        };
    }
}
