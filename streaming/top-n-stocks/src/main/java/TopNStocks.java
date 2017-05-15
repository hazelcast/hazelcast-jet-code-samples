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
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.accumulator.LinTrendAccumulator;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.sample.operations.TopNOperation;
import com.hazelcast.jet.sample.tradegenerator.GenerateTradesP;
import com.hazelcast.jet.sample.tradegenerator.Trade;
import com.hazelcast.jet.windowing.PunctuationPolicies;
import com.hazelcast.jet.windowing.TimestampedEntry;
import com.hazelcast.jet.windowing.WindowDefinition;
import com.hazelcast.jet.windowing.WindowOperation;
import com.hazelcast.jet.windowing.WindowOperations;

import java.util.List;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeLogger;
import static com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP.readMap;
import static com.hazelcast.jet.sample.tradegenerator.GenerateTradesP.TICKER_MAP_NAME;
import static com.hazelcast.jet.sample.tradegenerator.GenerateTradesP.generateTrades;
import static com.hazelcast.jet.windowing.WindowDefinition.tumblingWindowDef;
import static com.hazelcast.jet.windowing.WindowOperations.allOf;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindowStage1;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindowStage2;
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
 *              |(ticker, time, trend)         partitioned+distributed
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
 * |     calculate top+n     |
 * +------------+------------+
 *              |
 *              |(ticker, time, top+n(trend))       all-to-one+distributed
 *              |
 * +------------v------------+
 * |  sliding window stage 2 |
 * |     calculate top+n     |
 * +------------+------------+
 *              |
 *              |(ticker, time, top+n(trend))
 *              |
 *         +----+----+
 *         |  sink   |
 *         +---------+
 * </pre>
 *
 * Since the trade price is generated randomly the trend tends to be pretty
 * close to 0. The more trades are accumulated into the window the closer to 0
 * the trend is.
 */
public class TopNStocks {

    private static final int JOB_DURATION = 60;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            GenerateTradesP.loadTickers(jet);
            jet.newJob(buildDag()).execute();
            Thread.sleep(SECONDS.toMillis(JOB_DURATION));
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag() {
        WindowDefinition wDefTrend = WindowDefinition.slidingWindowDef(10_000, 1_000);
        WindowOperation<Trade, LinTrendAccumulator, Double> wOperTrend =
                WindowOperations.linearTrend(Trade::getTime, Trade::getPrice);

        WindowDefinition wDefTopN = WindowDefinition.tumblingWindowDef(1_000);
        DistributedComparator<TimestampedEntry<String, Double>> entryValueComparator =
                DistributedComparator.comparing((TimestampedEntry<String, Double> e) -> e.getValue());
        // Calculate two operations in single step: top-n largest and top-n smallest values
        WindowOperation<TimestampedEntry<String, Double>, List<Object>, List<Object>> wOperTopN =
                allOf(topNOperation(5, entryValueComparator), topNOperation(5, entryValueComparator.reversed()));

        DAG dag = new DAG();
        Vertex tickerSource = dag.newVertex("ticker-source", readMap(TICKER_MAP_NAME));
        Vertex generateTrades = dag.newVertex("generateTrades", generateTrades(300));
        Vertex insertPunc = dag.newVertex("insertPunc",
                insertPunctuation(Trade::getTime, () -> PunctuationPolicies.withFixedLag(1000)));

        // First accumulation: calculate price trend
        Vertex trendStage1 = dag.newVertex("trendStage1",
                slidingWindowStage1(Trade::getTicker, Trade::getTime, wDefTrend, wOperTrend));
        Vertex trendStage2 = dag.newVertex("trendStage2", slidingWindowStage2(wDefTrend, wOperTrend));

        // Second accumulation: calculate top-n price growth and fall.
        // The TimestampedEntry's timestamp is window end time (exclusive). In order for it to fall
        Vertex topNStage1 = dag.newVertex("topNStage1",
                slidingWindowStage1((TimestampedEntry en) -> en.getTimestamp() - 1, tumblingWindowDef(1), wOperTopN));
        Vertex topNStage2 = dag.newVertex("topNStage2", slidingWindowStage2(wDefTopN, wOperTopN));

        Vertex sink = dag.newVertex("sink", writeLogger()).localParallelism(1);

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
