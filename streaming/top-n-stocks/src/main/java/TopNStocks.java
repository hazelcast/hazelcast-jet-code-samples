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
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedBiConsumer;
import com.hazelcast.jet.function.DistributedComparator;
import com.hazelcast.jet.function.DistributedSupplier;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import serializer.PriorityQueueSerializer;
import trades.tradegenerator.GenerateTradesP;
import trades.tradegenerator.Trade;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.PriorityQueue;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.linearTrend;
import static com.hazelcast.jet.impl.util.Util.checkSerializable;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static com.hazelcast.jet.pipeline.WindowDefinition.tumbling;
import static java.util.concurrent.TimeUnit.SECONDS;
import static trades.tradegenerator.GenerateTradesP.TICKER_MAP_NAME;
import static trades.tradegenerator.GenerateTradesP.generateTradesP;

/**
 * This sample shows how to cascade aggregations. It first calculates the
 * linear trend of each stock's price over time, then finds 5 stocks
 * with the highest price growth and 5 stocks with the highest price drop.
 * <p>
 * It uses two windowing aggregations. First one uses a sliding window (to
 * smoothen the input) and the second one uses a tumbling window with length
 * equal to the sliding step of the first aggregation.
 * <p>
 * Since the trade price is generated randomly, the trend tends to be pretty
 * close to 0. The more trades are accumulated into the window the closer to 0
 * the trend is.
 *
 * <h3>Serialization</h3>
 *
 * This sample also demonstrates the use of Hazelcast serialization. The class
 * {@link java.util.PriorityQueue} is not supported by Hazelcast serialization
 * out of the box, so Java serialization would be used. We add a custom
 * serializer to the config.
 */
public class TopNStocks {

    private static final int JOB_DURATION = 60;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getSerializationConfig().addSerializerConfig(
                new SerializerConfig()
                        .setImplementation(new PriorityQueueSerializer())
                        .setTypeClass(PriorityQueue.class));

        JetInstance[] instances = new JetInstance[2];
        Arrays.parallelSetAll(instances, i -> Jet.newJetInstance(config));
        try {
            GenerateTradesP.loadTickers(instances[0], Integer.MAX_VALUE);
            instances[0].newJob(buildPipeline());
            Thread.sleep(SECONDS.toMillis(JOB_DURATION));
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        // Calculate two operations in single step: top-n largest and top-n smallest values
        DistributedComparator<TimestampedEntry<String, Double>> comparingValue =
                DistributedComparator.comparing(TimestampedEntry<String, Double>::getValue);
        AggregateOperation1<TimestampedEntry<String, Double>, ?, TopNResult> aggrOpTopN = allOf(
                topNAggregation(5, comparingValue),
                topNAggregation(5, comparingValue.reversed()),
                TopNResult::new);

        p.drawFrom(Sources.<String, Integer>map(TICKER_MAP_NAME))
         .<Trade>customTransform("generateTrades", generateTradesP(6000))
         .setTimestampWithEventTime(Trade::getTime, 1_000)
         .groupingKey(Trade::getTicker)
         .window(sliding(10_000, 1_000))
         .aggregate(linearTrend(Trade::getTime, Trade::getPrice)) // aggregate to create trend for each ticker
         .window(tumbling(1_000))
         .aggregate(aggrOpTopN) // aggregate to choose top-N trends
         .drainTo(Sinks.logger());

        return p;
    }

    public static <T> AggregateOperation1<T, ?, List<T>> topNAggregation(
            int n, DistributedComparator<? super T> comparator
    ) {
        checkSerializable(comparator, "comparator");
        DistributedComparator<? super T> comparatorReversed = comparator.reversed();
        DistributedBiConsumer<PriorityQueue<T>, T> accumulateFn = (PriorityQueue<T> a, T i) -> {
            if (a.size() == n) {
                if (comparator.compare(i, a.peek()) <= 0) {
                    // the new item is smaller or equal to the smallest in queue
                    return;
                }
                a.poll();
            }
            a.offer(i);
        };
        return AggregateOperation
                .withCreate((DistributedSupplier<PriorityQueue<T>>) () -> new PriorityQueue<>(n, comparator))
                .andAccumulate(accumulateFn)
                .andCombine((a1, a2) -> {
                    for (T t : a2) {
                        accumulateFn.accept(a1, t);
                    }
                })
                .andFinish(a -> {
                    ArrayList<T> res = new ArrayList<>(a);
                    res.sort(comparatorReversed);
                    return res;
                });
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
