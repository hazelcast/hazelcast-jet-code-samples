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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP;
import trades.tradegenerator.GenerateTradesP;
import trades.tradegenerator.Trade;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLagAndDelay;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.SinkProcessors.writeFileP;
import static java.util.concurrent.TimeUnit.SECONDS;
import static trades.tradegenerator.GenerateTradesP.MAX_LAG;
import static trades.tradegenerator.GenerateTradesP.TICKER_MAP_NAME;
import static trades.tradegenerator.GenerateTradesP.generateTradesP;

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
public class StockExchange {

    private static final String OUTPUT_DIR_NAME = "stock-exchange";
    private static final int SLIDING_WINDOW_LENGTH_MILLIS = 1000;
    private static final int SLIDE_STEP_MILLIS = 10;
    private static final int TRADES_PER_SEC_PER_MEMBER = 4_000_000;
    private static final int JOB_DURATION = 10;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            GenerateTradesP.loadTickers(jet, 100);
            jet.newJob(buildDag());
            Thread.sleep(SECONDS.toMillis(JOB_DURATION));
            System.out.format("%n%nGenerated %,.1f trade events per second%n%n",
                    (double) GenerateTradesP.TOTAL_EVENT_COUNT.get() / JOB_DURATION);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag() {
        DAG dag = new DAG();
        SlidingWindowPolicy winPolicy = slidingWinPolicy(SLIDING_WINDOW_LENGTH_MILLIS, SLIDE_STEP_MILLIS);

        Vertex tickerSource = dag.newVertex("ticker-source", ReadWithPartitionIteratorP.readMapP(TICKER_MAP_NAME));
        Vertex generateTrades = dag.newVertex("generate-trades", generateTradesP(TRADES_PER_SEC_PER_MEMBER));
        Vertex insertWatermarks = dag.newVertex("insert-watermarks", insertWatermarksP(wmGenParams(
                Trade::getTime,
                limitingLagAndDelay(MAX_LAG, 100),
                emitByFrame(winPolicy),
                30000L
        )));
        Vertex accumulateByFrame = dag.newVertex("accumulate-by-frame",
                accumulateByFrameP(
                        Trade::getTicker,
                        Trade::getTime, TimestampKind.EVENT,
                        winPolicy,
                        counting()));
        Vertex combineToSlidingWin = dag.newVertex("combine-to-sliding-win",
                combineToSlidingWindowP(winPolicy, counting()));
        Vertex formatOutput = dag.newVertex("format-output", formatOutput());
        Vertex sink = dag.newVertex("sink", writeFileP(OUTPUT_DIR_NAME));

        tickerSource.localParallelism(1);
        generateTrades.localParallelism(1);

        return dag
                .edge(between(tickerSource, generateTrades).distributed().broadcast())
                .edge(between(generateTrades, insertWatermarks).isolated())
                .edge(between(insertWatermarks, accumulateByFrame)
                        .partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(accumulateByFrame, combineToSlidingWin)
                        .partitioned(TimestampedEntry<String, Long>::getKey, HASH_CODE)
                        .distributed())
                .edge(between(combineToSlidingWin, formatOutput).isolated())
                .edge(between(formatOutput, sink).isolated());
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
