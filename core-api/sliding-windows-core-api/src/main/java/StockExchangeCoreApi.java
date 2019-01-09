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
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedPredicate;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.pipeline.ContextFactory;
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
import static com.hazelcast.jet.core.WatermarkPolicy.limitingLag;
import static com.hazelcast.jet.core.processor.Processors.mapUsingContextP;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static java.util.Collections.singletonList;

/**
 * A simple demonstration of Jet's continuous operators on an infinite
 * stream. Initially a Hazelcast map is populated with some stock
 * ticker names; the job reads the map and feeds the data to the vertex
 * that simulates an event stream coming from a stock market. The job
 * then computes the number of trades per ticker within a sliding window
 * of a given duration and dumps the results to a set of files.
 * <p>
 * This class shows a two-pipeline aggregation setup. For identical functionality
 * with single-pipeline setup, see {@link StockExchangeSingleStage}. For
 * discussion regarding single- and two-pipeline setup, see javadoc for the
 * {@link Processors} class.
 *
 * <pre>
 *            -----------------
 *           |  trade-source   |
 *            -----------------
 *                    |
 *    (timestamp, ticker, quantity, price)
 *                    |
 *                    |              partitioned-local
 *                    V
 *          ---------------------
 *         | accumulate-by-frame |
 *          ---------------------
 *                    |
 *          (ticker, time, count)    partitioned-distributed
 *                    V
 *        ------------------------
 *       | combine-to-sliding-win |
 *        ------------------------
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
public class StockExchangeCoreApi {

    private static final String TRADES_MAP_NAME = "trades";
    private static final String OUTPUT_DIR_NAME = "stock-exchange";
    private static final int SLIDING_WINDOW_LENGTH_MILLIS = 1000;
    private static final int SLIDE_STEP_MILLIS = 10;
    private static final int TRADES_PER_SECOND = 4_000;
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
        DistributedToLongFunction<? super Trade> timestampFn = Trade::getTime;
        DistributedFunction<? super Trade, ?> keyFn = Trade::getTicker;
        SlidingWindowPolicy winPolicy = slidingWinPolicy(
                SLIDING_WINDOW_LENGTH_MILLIS, SLIDE_STEP_MILLIS);

        Vertex tradeSource = dag.newVertex("trade-source",
                SourceProcessors.<Trade, Long, Trade>streamMapP(TRADES_MAP_NAME, DistributedPredicate.alwaysTrue(),
                        EventJournalMapEvent::getNewValue,
                        JournalInitialPosition.START_FROM_OLDEST,
                        eventTimePolicy(
                                Trade::getTime,
                                limitingLag(3000),
                                winPolicy.frameSize(),
                                winPolicy.frameOffset(),
                                30000L
                        )));
        Vertex slidingStage1 = dag.newVertex("sliding-stage-1",
                Processors.accumulateByFrameP(
                        singletonList(keyFn), singletonList(timestampFn), TimestampKind.EVENT,
                        winPolicy, counting()
                ));
        Vertex slidingStage2 = dag.newVertex("sliding-stage-2",
                Processors.combineToSlidingWindowP(winPolicy, counting(), TimestampedEntry::fromWindowResult));
        Vertex formatOutput = dag.newVertex("format-output", mapUsingContextP(
                ContextFactory.withCreateFn(x -> DateTimeFormatter.ofPattern("HH:mm:ss.SSS")),
                (DateTimeFormatter timeFormat, TimestampedEntry<String, Long> tse) ->
                        String.format("%s %5s %4d",
                                timeFormat.format(Instant.ofEpochMilli(tse.getTimestamp())
                                                         .atZone(ZoneId.systemDefault())),
                                tse.getKey(), tse.getValue())
        ));
        Vertex sink = dag.newVertex("sink",
                SinkProcessors.writeFileP(OUTPUT_DIR_NAME, Object::toString, StandardCharsets.UTF_8, false));

        tradeSource.localParallelism(1);

        return dag
                .edge(between(tradeSource, slidingStage1)
                        .partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(slidingStage1, slidingStage2)
                        .partitioned(entryKey(), HASH_CODE)
                        .distributed())
                .edge(between(slidingStage2, formatOutput)
                        .isolated())
                .edge(between(formatOutput, sink));
    }

}
