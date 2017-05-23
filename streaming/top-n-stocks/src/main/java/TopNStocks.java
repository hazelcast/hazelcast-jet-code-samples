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

import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.AggregateOperations;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.PunctuationPolicies;
import com.hazelcast.jet.TimestampKind;
import com.hazelcast.jet.TimestampedEntry;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.WindowDefinition;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.sample.operations.PriorityQueueSerializer;
import com.hazelcast.jet.sample.operations.TopNOperation;
import com.hazelcast.jet.sample.tradegenerator.GenerateTradesP;
import com.hazelcast.jet.sample.tradegenerator.Trade;

import java.util.List;
import java.util.PriorityQueue;

import static com.hazelcast.jet.AggregateOperations.allOf;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeLogger;
import static com.hazelcast.jet.WindowingProcessors.combineToSlidingWindow;
import static com.hazelcast.jet.WindowingProcessors.groupByFrameAndAccumulate;
import static com.hazelcast.jet.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.function.DistributedFunctions.constantKey;
import static com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP.readMap;
import static com.hazelcast.jet.sample.tradegenerator.GenerateTradesP.TICKER_MAP_NAME;
import static com.hazelcast.jet.sample.tradegenerator.GenerateTradesP.generateTrades;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This sample shows how to nest accumulations. It first calculates linear
 * trend for each stock, then finds top 5 stocks with highest price growth and
 * top 5 stocks with highest price drop.
 * <p>
 * It uses two two-stage accumulations into window. First accumulation uses
 * sliding window (to smooth the input), the second one has to use tumbling
 * window with length equal to the slide length of the first accumulation.
 * <p>
 * The DAG is as follows:
 *
 * <pre>
 *      +---------------+
 *      | ticker source |
 *      +-------+-------+
 *              |
 *              |(ticker)
 *              |
 *     +--------v--------+
 *     | generate trades |
 *     +--------+--------+
 *              |
 *              |(timestamp, ticker, quantity, price)
 *              |
 *   +----------v---------+
 *   | insert punctuation |
 *   +----------+---------+
 *              |
 *              |                              partitioned
 *              |
 * +------------v------------+
 * | sliding window stage 1  |
 * |     calculate trend     |
 * +------------+------------+
 *              |
 *              |(ticker, time, trend)         partitioned + distributed
 *              |
 * +------------v------------+
 * |  sliding window stage 2 |
 * |     calculate trend     |
 * +------------+------------+
 *              |
 *              |(ticker, time, trend)         all-to-one
 *              |
 * +------------v------------+
 * | sliding window stage 1  |
 * |     calculate top-n     |
 * +------------+------------+
 *              |
 *              |(ticker, time, top-n(trend))  all-to-one + distributed
 *              |
 * +------------v------------+
 * |  sliding window stage 2 |
 * |     calculate top-n     |
 * +------------+------------+
 *              |
 *              |(ticker, time, top-n(trend))
 *              |
 *         +----+----+
 *         |  sink   |
 *         +---------+
 * </pre>
 *
 * Since the trade price is generated randomly the trend tends to be pretty
 * close to 0. The more trades are accumulated into the window the closer to 0
 * the trend is.
 *
 * <h3>Serialization</h3>
 *
 * This sample also demonstrates the use of Hazelcast serialization. The class
 * {@link java.util.PriorityQueue} is not supported by Hazelcast serialization
 * out of the box, so Java serialization would be used.
 */
public class TopNStocks {

    private static final int JOB_DURATION = 60;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        SerializerConfig serializerConfig = new SerializerConfig()
                .setImplementation(new PriorityQueueSerializer())
                .setTypeClass(PriorityQueue.class);
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getSerializationConfig().addSerializerConfig(serializerConfig);

        JetInstance jet = Jet.newJetInstance(config);
        Jet.newJetInstance(config);
        try {
            GenerateTradesP.loadTickers(jet, Integer.MAX_VALUE);
            jet.newJob(buildDag()).execute();
            Thread.sleep(SECONDS.toMillis(JOB_DURATION));
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag() {
        // WindowDefinition and AggregateOperation for linear trend
        WindowDefinition wDefTrend = WindowDefinition.slidingWindowDef(10_000, 1_000);
        AggregateOperation<Trade, LinTrendAccumulator, Double> aggrOpTrend =
                AggregateOperations.linearTrend(Trade::getTime, Trade::getPrice);

        // WindowDefinition and AggregateOperation for top-n aggregation
        WindowDefinition wDefTopN = wDefTrend.toTumblingByFrame();
        DistributedComparator<TimestampedEntry<String, Double>> comparingValue =
                DistributedComparator.comparing(TimestampedEntry<String, Double>::getValue);
        // Calculate two operations in single step: top-n largest and top-n smallest values
        AggregateOperation<TimestampedEntry<String, Double>, List<Object>, List<Object>> aggrOpTopN =
                allOf(topNOperation(5, comparingValue), topNOperation(5, comparingValue.reversed()));

        DAG dag = new DAG();
        Vertex tickerSource = dag.newVertex("ticker-source", readMap(TICKER_MAP_NAME));
        Vertex generateTrades = dag.newVertex("generateTrades", generateTrades(6000));
        Vertex insertPunc = dag.newVertex("insertPunc",
                insertPunctuation(Trade::getTime, () -> PunctuationPolicies.withFixedLag(1000)));

        // First accumulation: calculate price trend
        Vertex trendStage1 = dag.newVertex("trendStage1",
                groupByFrameAndAccumulate(
                        Trade::getTicker,
                        Trade::getTime, TimestampKind.EVENT,
                        wDefTrend,
                        aggrOpTrend));
        Vertex trendStage2 = dag.newVertex("trendStage2", combineToSlidingWindow(wDefTrend, aggrOpTrend));

        // Second accumulation: calculate top-n price growth and fall.
        Vertex topNStage1 = dag.newVertex("topNStage1", groupByFrameAndAccumulate(
                constantKey(),
                TimestampedEntry::getTimestamp, TimestampKind.FRAME,
                wDefTopN,
                aggrOpTopN));
        Vertex topNStage2 = dag.newVertex("topNStage2", combineToSlidingWindow(wDefTopN, aggrOpTopN));

        Vertex sink = dag.newVertex("sink", writeLogger()).localParallelism(1);

        // These vertices are connected with all-to-one edges, therefore use parallelism 1:
        topNStage1.localParallelism(1);
        topNStage2.localParallelism(1);

        dag.edge(between(tickerSource, generateTrades)
                   .distributed()
                   .broadcast())
           .edge(between(generateTrades, insertPunc)
                   .oneToMany())
           .edge(between(insertPunc, trendStage1)
                   .partitioned(Trade::getTicker))
           .edge(between(trendStage1, trendStage2)
                   .partitioned((TimestampedEntry e) -> e.getKey()).distributed())
           .edge(between(trendStage2, topNStage1)
                   .allToOne())
           .edge(between(topNStage1, topNStage2)
                   .allToOne().distributed())
           .edge(between(topNStage2, sink));

        return dag;
    }

    private static <T> TopNOperation<T> topNOperation(int n, DistributedComparator<? super T> comparator) {
        return new TopNOperation<>(n, comparator);
    }

}
