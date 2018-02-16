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
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.WatermarkGenerationParams;
import com.hazelcast.jet.datamodel.BagsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.pipeline.*;
import datamodel.AddToCart;
import datamodel.Event;
import datamodel.PageVisit;
import datamodel.Payment;

import javax.annotation.Nonnull;
import java.util.Map.Entry;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.*;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.suppressDuplicates;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.pipeline.WindowDefinition.session;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class WindowedCoGroup {
    private static final String TOPIC = "topic";
    private static final String PAGE_VISIT = "pageVisit";
    private static final String ADD_TO_CART = "addToCart";
    private static final String PAYMENT = "payment";

    private IMap<Object, PageVisit> pageVisit;
    private IMap<Object, AddToCart> addToCart;
    private IMap<Object, Payment> payment;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        System.setProperty("hazelcast.partition.count", "1");
        new WindowedCoGroup().go();
    }

    private void go() throws Exception {
        JetConfig cfg = new JetConfig();
        cfg.getHazelcastConfig().getMapEventJournalConfig("*").setEnabled(true);
        JetInstance jet = Jet.newJetInstance(cfg);
        pageVisit = jet.getMap(PAGE_VISIT);
        addToCart = jet.getMap(ADD_TO_CART);
        payment = jet.getMap(PAYMENT);

        ProducerTask producer = new ProducerTask();
        try {
            Pipeline p = coGroupBuild();
            System.out.println("Running pipeline " + p);
            Job job = jet.newJob(p);
            Thread.sleep(5000);
            producer.keepGoing = false;
            job.cancel();
            jet.getList("sink").forEach(System.out::println);
        } finally {
            producer.keepGoing = false;
            Jet.shutdownAll();
        }
    }

    private class ProducerTask implements Runnable {
        { new Thread(this, "WindowedCoGroup Producer").start(); }

        volatile boolean keepGoing = true;

        private int loadTime = 1;
        private int quantity = 21;
        private int amount = 31;
        private long now = System.currentTimeMillis();

        @Override
        public void run() {
            LockSupport.parkNanos(MILLISECONDS.toNanos(100));
            while (keepGoing) {
                produceSampleData();
                LockSupport.parkNanos(MILLISECONDS.toNanos(1));
                now++;
            }
        }

        private void produceSampleData() {
            for (int userId = 11; userId < 13; userId++) {
                for (int i = 0; i < 2; i++) {
                    pageVisit.set(TOPIC, new PageVisit(now, userId, loadTime));
                    addToCart.set(TOPIC, new AddToCart(now, userId, quantity));
                    payment.set(TOPIC, new Payment(now, userId, amount));

                    loadTime++;
                    quantity++;
                    amount++;
                }
            }
        }
    }

    private static <T> WatermarkGenerationParams<T> wmParams(
            @Nonnull DistributedToLongFunction<T> timestampFn
    ) {
        return wmGenParams(timestampFn, limitingLag(100), suppressDuplicates(), 10);
    }

    private static Pipeline coGroupDirect() {
        Pipeline p = Pipeline.create();
        StreamStageWithGrouping<PageVisit, Integer> pageVisits = p
                .drawFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST)
                .timestampWithEventTime(Event::timestamp, 100))
                .groupingKey(Event::userId);
        StreamStageWithGrouping<Payment, Integer> payments = p
                .drawFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST)
                        .timestampWithEventTime(Event::timestamp, 100))
                .groupingKey(Event::userId);
        StreamStageWithGrouping<AddToCart, Integer> addToCarts = p
                .drawFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST)
                        .timestampWithEventTime(Event::timestamp, 100))
                .groupingKey(Event::userId);

        StageWithGroupingAndWindow<PageVisit, Integer> windowStage = pageVisits.window(sliding(10, 1));

        StreamStage<TimestampedEntry<Integer, ThreeBags<PageVisit, AddToCart, Payment>>> coGrouped = windowStage
                .aggregate3(addToCarts, payments, toThreeBags());

        coGrouped.drainTo(Sinks.logger());
        return p;
    }

    private static Pipeline coGroupBuild() {
        Pipeline p = Pipeline.create();

        StreamStageWithGrouping<PageVisit, Integer> pageVisits = p
                .drawFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .timestamp(pv -> pv.timestamp(), limitingLag(1000))
                .groupingKey(pv -> pv.userId());
        StreamStageWithGrouping<AddToCart, Integer> addToCarts = p
                .drawFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .timestamp(atc -> atc.timestamp(), limitingLag(1000))
                .groupingKey(atc -> atc.userId());
        StreamStageWithGrouping<Payment, Integer> payments = p
                .drawFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .timestamp(pm -> pm.timestamp(), limitingLag(1000))
                .groupingKey(pm -> pm.userId());

        StageWithGroupingAndWindow<PageVisit, Integer> windowStage = pageVisits.window(sliding(10, 1));

        WindowGroupAggregateBuilder<PageVisit, Integer> builder = windowStage.aggregateBuilder();
        Tag<PageVisit> pageVisitTag = builder.tag0();
        Tag<AddToCart> addToCartTag = builder.add(addToCarts);
        Tag<Payment> paymentTag = builder.add(payments);

        StreamStage<TimestampedEntry<Integer, BagsByTag>> coGrouped = builder.build(
                toBagsByTag(pageVisitTag, addToCartTag, paymentTag));

        coGrouped.drainTo(Sinks.logger());
        return p;
    }

    private static Pipeline slidingWindow(String srcName, String sinkName) {
        Pipeline p = Pipeline.create();
        StreamSource<Entry<String, Long>> wmSrc = Sources.mapJournal(srcName, START_FROM_OLDEST);

        StreamStage<Entry<String, Long>> srcStage = p
                .drawFrom(wmSrc)
                .timestamp(Entry::getValue, limitingLag(1000));

        StreamStage<TimestampedEntry<String, Long>> wordCounts =
                srcStage.window(session(1))
                        .groupingKey(Entry::getKey)
                        .aggregate(counting());

        wordCounts.drainTo(Sinks.list(sinkName));
        return p;
    }
}
