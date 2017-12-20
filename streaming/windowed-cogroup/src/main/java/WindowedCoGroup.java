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
import com.hazelcast.jet.GroupAggregateBuilder;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.StageWithGrouping;
import com.hazelcast.jet.StageWithGroupingAndTimestamp;
import com.hazelcast.jet.StageWithGroupingAndWindow;
import com.hazelcast.jet.WindowGroupAggregateBuilder;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.BagsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import datamodel.Payment;
import datamodel.AddToCart;
import datamodel.PageVisit;

import java.util.Map.Entry;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.WindowDefinition.sliding;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

public class WindowedCoGroup {
    private static final String PAGE_VISIT = "pageVisit";
    private static final String ADD_TO_CART = "addToCart";
    private static final String PAYMENT = "payment";
    private static final String RESULT = "result";

    static void windowedWordCount() {
        Pipeline p = Pipeline.create();
        ComputeStage<String> src = p.drawFrom(Sources.<String, Long, String>mapJournal(
                "map", mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST));

        ComputeStage<Entry<String, Long>> wordCountsGTW =
                src.groupingKey(wholeItem())
                   .timestamp(Long::valueOf)
                   .window(sliding(10, 1))
                   .aggregate(counting());

        ComputeStage<Entry<String, Long>> wordCountsTGW =
                src.timestamp(Long::valueOf)
                   .groupingKey(wholeItem())
                   .window(sliding(10, 1))
                   .aggregate(counting());

        ComputeStage<Entry<String, Long>> wordCountsTWG =
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

        StageWithGroupingAndTimestamp<PageVisit, Integer> pageVisits =
                p.drawFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                 .groupingKey(PageVisit::userId)
                 .timestamp(PageVisit::timestamp);
        StageWithGroupingAndTimestamp<AddToCart, Integer> addToCarts = p
                .drawFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .timestamp(AddToCart::timestamp)
                .groupingKey(AddToCart::userId);
        StageWithGroupingAndTimestamp<Payment, Integer> payments =
                p.drawFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                 .groupingKey(Payment::userId)
                 .timestamp(Payment::timestamp);

        StageWithGroupingAndWindow<PageVisit, Integer> windowStage = pageVisits.window(sliding(10, 1));

        ComputeStage<Entry<Integer, ThreeBags<PageVisit, AddToCart, Payment>>> coGrouped = windowStage
                .aggregate3(addToCarts, payments,
                        AggregateOperation
                                .withCreate(ThreeBags::<PageVisit, AddToCart, Payment>threeBags)
                                .<PageVisit>andAccumulate0((acc, pageVisit) -> acc.bag0().add(pageVisit))
                                .<AddToCart>andAccumulate1((acc, addToCart) -> acc.bag1().add(addToCart))
                                .<Payment>andAccumulate2((acc, payment) -> acc.bag2().add(payment))
                                .andCombine(ThreeBags::combineWith)
                                .andDeduct(ThreeBags::deduct)
                                .andFinish(x -> x));

        // Store the results in the output map
        coGrouped.drainTo(Sinks.map(RESULT));
        return p;
    }

    private static Pipeline coGroupBuild() {
        Pipeline p = Pipeline.create();

        // Create three source streams
        StageWithGroupingAndTimestamp<PageVisit, Integer> pageVisits =
                p.drawFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                 .groupingKey(PageVisit::userId)
                 .timestamp(PageVisit::timestamp);
        StageWithGroupingAndTimestamp<AddToCart, Integer> addToCarts = p
                .drawFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .timestamp(AddToCart::timestamp)
                .groupingKey(AddToCart::userId);
        StageWithGroupingAndTimestamp<Payment, Integer> payments =
                p.drawFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                 .groupingKey(Payment::userId)
                 .timestamp(Payment::timestamp);

        StageWithGroupingAndWindow<PageVisit, Integer> windowStage = pageVisits.window(sliding(10, 1));

        WindowGroupAggregateBuilder<PageVisit, Integer> builder = windowStage.aggregateBuilder();
        Tag<PageVisit> pageVisitTag = builder.tag0();
        Tag<AddToCart> addToCartTag = builder.add(addToCarts);
        Tag<Payment> paymentTag = builder.add(payments);

        ComputeStage<Entry<Integer, BagsByTag>> coGrouped = builder.build(AggregateOperation
                .withCreate(BagsByTag::new)
                .andAccumulate(pageVisitTag, (acc, pageVisit) -> acc.ensureBag(pageVisitTag).add(pageVisit))
                .andAccumulate(addToCartTag, (acc, addToCart) -> acc.ensureBag(addToCartTag).add(addToCart))
                .andAccumulate(paymentTag, (acc, payment) -> acc.ensureBag(paymentTag).add(payment))
                .andCombine(BagsByTag::combineWith)
                .andFinish(x -> x)
        );

        // Store the results in the output map
        coGrouped.drainTo(Sinks.map(RESULT));

        return p;
    }
}
