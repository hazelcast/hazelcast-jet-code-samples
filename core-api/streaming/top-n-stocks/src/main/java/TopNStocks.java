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

import com.hazelcast.config.SerializerConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import trades.operations.PriorityQueueSerializer;
import trades.tradegenerator.GenerateTradesP;
import trades.tradegenerator.Trade;

import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.function.DistributedFunctions.constantKey;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static trades.operations.TopNOperation.topNOperation;
import static trades.tradegenerator.GenerateTradesP.TICKER_MAP_NAME;
import static trades.tradegenerator.GenerateTradesP.generateTradesP;

/**
 * This sample shows how to cascade aggregations. It first calculates the
 * linear trend of each stock's price over time, then finds 5 stocks
 * with the highest price growth and 5 stocks with the highest price drop.
 * <p>
 * It uses two two-stage windowing aggregations. First one uses a sliding
 * window (to smoothen the input) and the second one has uses a tumbling
 * window with length equal to the sliding step of the first aggregation.
 * <p>
 * The DAG is as follows:
 *
 * <pre>
 *               +---------------+
 *               | ticker source |
 *               +-------+-------+
 *                       |
 *                       |(ticker)
 *                       |
 *              +--------v--------+
 *              | generate trades |
 *              +--------+--------+
 *                       |
 *                       |(timestamp, ticker, quantity, price)
 *                       |
 *             +----------v--------+
 *             | insert watermarks |
 *             +----------+--------+
 *                       |
 *                       |                              partitioned
 *                       |
 *          +------------v------------+
 *          | sliding window pipeline 1  |
 *          |     calculate trend     |
 *          +------------+------------+
 *                       |
 *                       |(ticker, time, trend)         partitioned + distributed
 *                       |
 *          +------------v------------+
 *          |  sliding window pipeline 2 |
 *          |     calculate trend     |
 *          +------------+------------+
 *                       |
 *                       |(ticker, time, trend)         all-to-one
 *                       |
 *          +------------v------------+
 *          | sliding window pipeline 1  |
 *          |     calculate top-n     |
 *          +------------+------------+
 *                       |
 *                       |(ticker, time, top-n(trend))  all-to-one + distributed
 *                       |
 *          +------------v------------+
 *          |  sliding window pipeline 2 |
 *          |     calculate top-n     |
 *          +------------+------------+
 *                       |
 *                       |(ticker, time, top-n(trend))
 *                       |
 *                  +----+----+
 *                  |  sink   |
 *                  +---------+
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

        JetInstance[] instances = new JetInstance[2];
        Arrays.setAll(instances, i -> Jet.newJetInstance(config));
        try {
            GenerateTradesP.loadTickers(instances[0], Integer.MAX_VALUE);
            instances[0].newJob(buildDag());
            Thread.sleep(SECONDS.toMillis(JOB_DURATION));
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag() {
        // SlidingWindowPolicy and operation for linear trend
        SlidingWindowPolicy wPolTrend = slidingWinPolicy(10_000, 1_000);
        AggregateOperation1<Trade, LinTrendAccumulator, Double> aggrOpTrend =
                AggregateOperations.linearTrend(Trade::getTime, Trade::getPrice);

        // SlidingWindowPolicy and operation for top-n aggregation
        SlidingWindowPolicy wDefTopN = wPolTrend.toTumblingByFrame();
        DistributedComparator<TimestampedEntry<String, Double>> comparingValue =
                DistributedComparator.comparing(TimestampedEntry<String, Double>::getValue);
        // Calculate two operations in single step: top-n largest and top-n smallest values
        AggregateOperation1<TimestampedEntry<String, Double>, ?, TopNResult> aggrOpTopN = allOf(
                topNOperation(5, comparingValue),
                topNOperation(5, comparingValue.reversed()),
                TopNResult::new);

        DAG dag = new DAG();
        Vertex tickerSource = dag.newVertex("ticker-source", readMapP(TICKER_MAP_NAME));
        Vertex generateTrades = dag.newVertex("generateTrades", generateTradesP(6000));
        Vertex insertWm = dag.newVertex("insertWm",
                insertWatermarksP(wmGenParams(
                        Trade::getTime,
                        limitingLag(1000),
                        emitByFrame(wPolTrend),
                        30000L))
        );

        // First accumulation: calculate price trend
        Vertex trendStage1 = dag.newVertex("trendStage1",
                accumulateByFrameP(
                        singletonList((DistributedFunction<Trade, ?>) Trade::getTicker),
                        singletonList((DistributedToLongFunction<Trade>) Trade::getTime),
                        TimestampKind.EVENT,
                        wPolTrend,
                        aggrOpTrend.withFinishFn(identity())
                ));
        Vertex trendStage2 = dag.newVertex("trendStage2",
                combineToSlidingWindowP(wPolTrend, aggrOpTrend, TimestampedEntry::new));

        // Second accumulation: calculate top 20 stocks with highest price growth and fall.
        Vertex topNStage1 = dag.newVertex("topNStage1", accumulateByFrameP(
                singletonList(constantKey()),
                singletonList((DistributedToLongFunction<TimestampedEntry<String, Double>>) TimestampedEntry::getTimestamp),
                TimestampKind.FRAME,
                wDefTopN,
                aggrOpTopN.withFinishFn(identity())
        ));
        Vertex topNStage2 = dag.newVertex("topNStage2",
                combineToSlidingWindowP(wDefTopN, aggrOpTopN, TimestampedEntry::new));

        Vertex sink = dag.newVertex("sink", DiagnosticProcessors.writeLoggerP()).localParallelism(1);

        // These vertices are connected with all-to-one edges, therefore use parallelism 1:
        topNStage1.localParallelism(1);
        topNStage2.localParallelism(1);

        dag.edge(between(tickerSource, generateTrades)
                   .distributed()
                   .broadcast())
           .edge(between(generateTrades, insertWm)
                   .isolated())
           .edge(between(insertWm, trendStage1)
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

    public static final class TopNResult {
        private final List<TimestampedEntry<String, Double>> topIncrease;
        private final List<TimestampedEntry<String, Double>> topDecrease;

        public TopNResult(List<TimestampedEntry<String, Double>> topIncrease,
                          List<TimestampedEntry<String, Double>> topDecrease) {
            this.topIncrease = topIncrease;
            this.topDecrease = topDecrease;
        }

        public List<TimestampedEntry<String, Double>> getTopIncrease() {
            return topIncrease;
        }

        public List<TimestampedEntry<String, Double>> getTopDecrease() {
            return topDecrease;
        }

        @Override
        public String toString() {
            return "TopNResult{" +
                    "topIncrease=" + topIncrease +
                    ", topDecrease=" + topDecrease +
                    '}';
        }
    }
}
