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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.ComputeStageWM;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Source;
import com.hazelcast.jet.SourceWithWatermark;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.StageWithGroupingAndWindow;
import com.hazelcast.jet.StageWithGroupingWM;
import com.hazelcast.jet.WindowGroupAggregateBuilder;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.BagsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import datamodel.AddToCart;
import datamodel.PageVisit;
import datamodel.Payment;

import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.WindowDefinition.sliding;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class WindowedCoGroup {
    private static final String PAGE_VISIT = "pageVisit";
    private static final String ADD_TO_CART = "addToCart";
    private static final String PAYMENT = "payment";
    private static final String RESULT = "result";

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hazelcast.logging.type", "log4j");
        String srcName = "word-stream";
        String sinkName = "sink";

        SourceWithWatermark<Entry<String, Long>> wmSrc =
                Sources.<String, Long>mapJournal(srcName, START_FROM_OLDEST)
                        .withWatermark(Entry::getValue, limitingLag(1000));

        Pipeline p = Pipeline.create();
        ComputeStageWM<Entry<String, Long>> srcStage = p.drawFrom(wmSrc);

        ComputeStage<TimestampedEntry<String, Long>> wordCounts =
                srcStage.groupingKey(Entry::getKey)
                        .window(sliding(10, 10))
                        .aggregate(counting());

        wordCounts.drainTo(Sinks.list(sinkName));

        JetConfig cfg = new JetConfig();
        cfg.getHazelcastConfig().getMapEventJournalConfig(srcName).setEnabled(true);
        JetInstance jet = Jet.newJetInstance(cfg);
        try {
            Job job = jet.newJob(p);
            Thread.sleep(100);
            IMap<String, Long> srcMap = jet.getMap(srcName);
            AtomicBoolean keepGoing = new AtomicBoolean(true);
            new Thread(() -> {
                long now = 0;
                while (keepGoing.get()) {
                    for (char ch = 'A'; ch <= 'Z'; ch++) {
                        srcMap.set(String.valueOf(ch), now);
                    }
                    LockSupport.parkNanos(MILLISECONDS.toNanos(1));
                    now++;
                }
            }).start();
            Thread.sleep(5000);
            keepGoing.set(false);
            job.cancel();
            jet.getList(sinkName).forEach(System.out::println);
        } finally {
            Jet.shutdownAll();
        }
    }

    static void windowedWordCount() {
        Source<Entry<String, Long>> src = Sources.mapJournal("map", START_FROM_OLDEST);

        SourceWithWatermark<Entry<String, Long>> wmSrc =
                src.withWatermark(Entry::getValue, limitingLag(1000));

        Pipeline p = Pipeline.create();
        ComputeStageWM<Entry<String, Long>> srcStage = p.drawFrom(wmSrc);

        ComputeStage<TimestampedEntry<String, Long>> wordCountsTGW =
                srcStage.groupingKey(Entry::getKey)
                        .window(sliding(10, 1))
                        .aggregate(counting());

        ComputeStage<TimestampedEntry<String, Long>> wordCountsTWG =
                srcStage.window(sliding(10, 1))
                        .groupingKey(Entry::getKey)
                        .aggregate(counting());
    }

    static void windowedCount() {
        Pipeline p = Pipeline.create();
        ComputeStageWM<Entry<String, Long>> src = p.drawFrom(Sources.<String, Long>mapJournal(
                "map", START_FROM_OLDEST)
                .withWatermark(Entry::getValue, limitingLag(1000)));

        ComputeStage<TimestampedEntry<Void, Long>> counts =
                src.window(sliding(10, 1))
                   .aggregate(counting());
    }

    private static Pipeline coGroupDirect() {
        Pipeline p = Pipeline.create();

        StageWithGroupingWM<PageVisit, Integer> pageVisits =
                p.drawFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST)
                        .withWatermark(PageVisit::timestamp, limitingLag(1000)))
                 .groupingKey(PageVisit::userId);
        StageWithGroupingWM<AddToCart, Integer> addToCarts = p
                .drawFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST)
                        .withWatermark(AddToCart::timestamp, limitingLag(1000)))
                .groupingKey(AddToCart::userId);
        StageWithGroupingWM<Payment, Integer> payments =
                p.drawFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST)
                        .withWatermark(Payment::timestamp, limitingLag(1000)))
                 .groupingKey(Payment::userId);

        StageWithGroupingAndWindow<PageVisit, Integer> windowStage = pageVisits.window(sliding(10, 1));

        ComputeStage<TimestampedEntry<Integer, ThreeBags<PageVisit, AddToCart, Payment>>> coGrouped = windowStage
                .aggregate3(addToCarts, payments,
                        AggregateOperation
                                .withCreate(ThreeBags::<PageVisit, AddToCart, Payment>threeBags)
                                .<PageVisit>andAccumulate0((acc, pageVisit) -> acc.bag0().add(pageVisit))
                                .<AddToCart>andAccumulate1((acc, addToCart) -> acc.bag1().add(addToCart))
                                .<Payment>andAccumulate2((acc, payment) -> acc.bag2().add(payment))
                                .andCombine(ThreeBags::combineWith)
                                .andDeduct(ThreeBags::deduct)
                                .andFinish(x -> x));

        coGrouped.drainTo(Sinks.map(RESULT));
        return p;
    }

    private static Pipeline coGroupBuild() {
        Pipeline p = Pipeline.create();

        StageWithGroupingWM<PageVisit, Integer> pageVisits =
                p.drawFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST)
                        .withWatermark(PageVisit::timestamp, limitingLag(1000)))
                 .groupingKey(PageVisit::userId);
        StageWithGroupingWM<AddToCart, Integer> addToCarts = p
                .drawFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST)
                        .withWatermark(AddToCart::timestamp, limitingLag(1000)))
                .groupingKey(AddToCart::userId);
        StageWithGroupingWM<Payment, Integer> payments =
                p.drawFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST)
                        .withWatermark(Payment::timestamp, limitingLag(1000)))
                 .groupingKey(Payment::userId);

        StageWithGroupingAndWindow<PageVisit, Integer> windowStage = pageVisits.window(sliding(10, 1));

        WindowGroupAggregateBuilder<PageVisit, Integer> builder = windowStage.aggregateBuilder();
        Tag<PageVisit> pageVisitTag = builder.tag0();
        Tag<AddToCart> addToCartTag = builder.add(addToCarts);
        Tag<Payment> paymentTag = builder.add(payments);

        ComputeStage<TimestampedEntry<Integer, BagsByTag>> coGrouped = builder.build(AggregateOperation
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
