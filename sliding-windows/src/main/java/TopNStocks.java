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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.function.ComparatorEx;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import tradegenerator.Trade;

import java.util.Arrays;
import java.util.List;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.aggregate.AggregateOperations.topN;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static tradegenerator.TradeGenerator.tradeSource;

/**
 * This sample shows how to cascade aggregations. It first calculates the
 * linear trend of each stock's price over time, then finds 5 stocks
 * with the highest price growth and 5 stocks with the highest price drop.
 * <p>
 * It uses two windowing aggregations. First one uses a sliding window (to
 * smoothen the input) and the second one uses a tumbling window with
 * length equal to the sliding step of the first aggregation.
 * <p>
 * Since the trade price is generated randomly, the trend tends to be
 * pretty close to 0. The more trades are accumulated into the window the
 * closer to 0 the trend is.
 *
 * <h3>Serialization</h3>
 *
 * This sample also demonstrates the use of Hazelcast serialization. The
 * class {@link java.util.PriorityQueue} is not supported by Hazelcast
 * serialization out of the box, so Java serialization would be used. We
 * add a custom serializer to the config.
 */
public class TopNStocks {

    private static final int JOB_DURATION = 60;
    private static final String TRADES = "trades";

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        ComparatorEx<KeyedWindowResult<String, Double>> comparingValue =
                ComparatorEx.comparing(KeyedWindowResult<String, Double>::result);
        // Calculate two operations in single step: top-n largest and top-n smallest values
        AggregateOperation1<KeyedWindowResult<String, Double>, ?, TopNResult> aggrOpTopN = allOf(
                topN(5, comparingValue),
                topN(5, comparingValue.reversed()),
                TopNResult::new);

        p.drawFrom(tradeSource(500, 6_000, JOB_DURATION))
         .withNativeTimestamps(1_000)
         .groupingKey(Trade::getTicker)
         .window(sliding(10_000, 1_000))
         // aggregate to create trend for each ticker
         .aggregate(linearTrend(Trade::getTime, Trade::getPrice))
         .window(tumbling(1_000))
         // 2nd aggregation: choose top-N trends from previous aggregation
         .aggregate(aggrOpTopN)
         .drainTo(Sinks.logger());

        return p;
    }

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetInstance[] instances = new JetInstance[2];
        Arrays.parallelSetAll(instances, i -> Jet.newJetInstance());
        try {
            instances[0].newJob(buildPipeline()).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    static final class TopNResult {
        private final List<KeyedWindowResult<String, Double>> topIncrease;
        private final List<KeyedWindowResult<String, Double>> topDecrease;

        TopNResult(
                List<KeyedWindowResult<String, Double>> topIncrease,
                List<KeyedWindowResult<String, Double>> topDecrease
        ) {
            this.topIncrease = topIncrease;
            this.topDecrease = topDecrease;
        }

        @Override
        public String toString() {
            return "TopNResult{topIncrease=" + topIncrease + ", topDecrease=" + topDecrease + '}';
        }
    }
}
