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

import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.StageWithGrouping;
import com.hazelcast.jet.StageWithGroupingAndTimestamp;
import com.hazelcast.jet.StageWithGroupingAndWindow;
import com.hazelcast.jet.StageWithTimestamp;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import datamodel.Broker;
import datamodel.Product;
import datamodel.Trade;

import java.util.Map.Entry;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.WindowDefinition.sliding;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

/**
 * Javadoc pending.
 */
public class StreamingCoGroup {
    private static final String TRADES = "trades";
    private static final String PRODUCTS = "products";
    private static final String BROKERS = "brokers";
    private static final String RESULT = "result";

    static void windowedWordCount() {
        Pipeline p = Pipeline.create();
        ComputeStage<String> src = p.drawFrom(Sources.<String, Long, String>mapJournal(
                "map", mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST));

        ComputeStage<Entry<String, Long>> wordCountsVariant1 =
                src.groupingKey(wholeItem())
                   .timestamp(Long::valueOf)
                   .window(sliding(10, 1))
                   .aggregate(counting());

        ComputeStage<Entry<String, Long>> wordCountsVariant2 =
                src.timestamp(Long::valueOf)
                   .window(sliding(10, 1))
                   .groupingKey(wholeItem())
                   .aggregate(counting());
    }

    static void windowedCount() {
        Pipeline p = Pipeline.create();
        ComputeStage<String> src = p.drawFrom(Sources.<String, Long, String>mapJournal(
                "map", mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST));
        ComputeStage<TimestampedEntry<Void, Long>> counts =
                src.timestamp(Long::valueOf)
                   .window(sliding(10, 1))
                   .aggregate(counting());
    }

    private static Pipeline coGroupDirect() {
        Pipeline p = Pipeline.create();

        // Create three source streams
        StageWithGroupingAndTimestamp<Trade, Integer> trades =
                p.drawFrom(Sources.<Trade, Integer, Trade>mapJournal(TRADES,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                 .groupingKey(Trade::classId)
                 .timestamp(Trade::id);
        StageWithTimestamp<Product> x = p
                .drawFrom(Sources.<Product, Integer, Product>mapJournal(PRODUCTS, mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .timestamp(Product::id);
        StageWithGrouping<Product, Integer> y = x.groupingKey(Product::classId);
        StageWithGroupingAndTimestamp<Broker, Integer> brokers =
                p.drawFrom(Sources.<Broker, Integer, Broker>mapJournal(BROKERS,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                 .groupingKey(Broker::classId)
                 .timestamp(Broker::id);

        // Specify the sliding window. You have to specify it on the leftmost stage
        // of the co-group operation, the one you invoke `aggregate3()` on.
        StageWithGroupingAndWindow<Trade, Integer> windowStage = trades
                .timestamp(Trade::id)
                .window(sliding(10, 1));

        // Construct the co-group transform. The aggregate operation collects all
        // the stream items inside an accumulator class called ThreeBags.
        ComputeStage<Entry<Integer, ThreeBags<Trade, Product, Broker>>> coGrouped = windowStage
                .aggregate3(products, brokers,
                        AggregateOperation
                                .withCreate(ThreeBags::<Trade, Product, Broker>threeBags)
                                .<Trade>andAccumulate0((acc, trade) -> acc.bag0().add(trade))
                                .<Product>andAccumulate1((acc, product) -> acc.bag1().add(product))
                                .<Broker>andAccumulate2((acc, broker) -> acc.bag2().add(broker))
                                .andCombine(ThreeBags::combineWith)
                                .andDeduct(ThreeBags::deduct)
                                .andFinish(x -> x));

        // Store the results in the output map
        coGrouped.drainTo(Sinks.map(RESULT));
        return p;
    }

}
