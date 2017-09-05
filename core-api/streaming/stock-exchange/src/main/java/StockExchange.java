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
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.TimestampKind;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.sample.tradegenerator.GenerateTradesP;
import com.hazelcast.jet.sample.tradegenerator.Trade;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import static com.hazelcast.jet.AggregateOperations.counting;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.processor.SinkProcessors.writeFile;
import static com.hazelcast.jet.WatermarkPolicies.limitingLagAndDelay;
import static com.hazelcast.jet.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.processor.Processors.combineToSlidingWindow;
import static com.hazelcast.jet.processor.Processors.accumulateByFrame;
import static com.hazelcast.jet.processor.Processors.insertWatermarks;
import static com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP.readMap;
import static com.hazelcast.jet.sample.tradegenerator.GenerateTradesP.MAX_LAG;
import static com.hazelcast.jet.sample.tradegenerator.GenerateTradesP.TICKER_MAP_NAME;
import static com.hazelcast.jet.sample.tradegenerator.GenerateTradesP.generateTrades;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A simple demonstration of Jet's continuous operators on an infinite
 * stream. Initially a Hazelcast map is populated with some stock
 * ticker names; the job reads the map and feeds the data to the vertex
 * that simulates an event stream coming from a stock market. The job
 * then computes the number of trades per ticker within a sliding window
 * of a given duration and dumps the results to a set of files.
 * <p>
 * This class shows a two-stage aggregation setup. For identical functionality
 * with single-stage setup, see {@link StockExchangeSingleStage}. For
 * discussion regarding single- and two-stage setup, see javadoc for the
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
        WindowDefinition windowDef = slidingWindowDef(SLIDING_WINDOW_LENGTH_MILLIS, SLIDE_STEP_MILLIS);

        Vertex tickerSource = dag.newVertex("ticker-source", readMap(TICKER_MAP_NAME));
        Vertex generateTrades = dag.newVertex("generate-trades", generateTrades(TRADES_PER_SEC_PER_MEMBER));
        Vertex insertWatermarks = dag.newVertex("insert-watermarks", insertWatermarks(Trade::getTime,
                limitingLagAndDelay(MAX_LAG, 100), emitByFrame(windowDef)));
        Vertex accumulateByFrame = dag.newVertex("accumulate-by-frame",
                accumulateByFrame(
                        Trade::getTicker,
                        Trade::getTime, TimestampKind.EVENT,
                        windowDef,
                        counting()));
        Vertex combineToSlidingWin = dag.newVertex("combine-to-sliding-win",
                combineToSlidingWindow(windowDef, counting()));
        Vertex formatOutput = dag.newVertex("format-output", formatOutput());
        Vertex sink = dag.newVertex("sink", writeFile(OUTPUT_DIR_NAME));

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
            return Processors.map((TimestampedEntry<String, Long> f) -> String.format("%s %5s %4d",
                    timeFormat.format(Instant.ofEpochMilli(f.getTimestamp()).atZone(ZoneId.systemDefault())),
                    f.getKey(), f.getValue())).get();
        };
    }
}
