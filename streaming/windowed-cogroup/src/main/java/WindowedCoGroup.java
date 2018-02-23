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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.BagsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.StageWithGroupingAndWindow;
import com.hazelcast.jet.pipeline.StreamStage;
import com.hazelcast.jet.pipeline.StreamStageWithGrouping;
import com.hazelcast.jet.pipeline.WindowGroupAggregateBuilder;
import datamodel.AddToCart;
import datamodel.PageVisit;
import datamodel.Payment;

import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.Util.mapEventNewValue;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.toBagsByTag;
import static com.hazelcast.jet.aggregate.AggregateOperations.toThreeBags;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class WindowedCoGroup {
    private static final String TOPIC = "topic";
    private static final String PAGE_VISIT = "pageVisit";
    private static final String ADD_TO_CART = "addToCart";
    private static final String PAYMENT = "payment";

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        // All IMap partitions must receive updates for the watermark to advance
        // correctly. Since we use just a handful of keys in this sample, we set a
        // low partition count.
        System.setProperty("hazelcast.partition.count", "1");

        JetConfig cfg = new JetConfig();
        cfg.getHazelcastConfig().getMapEventJournalConfig("*").setEnabled(true);
        JetInstance jet = Jet.newJetInstance(cfg);
        ProducerTask producer = new ProducerTask(jet);

        try {
            // uncomment one of these
            Pipeline p = coGroupDirect();
            //Pipeline p = coGroupBuild();

            System.out.println("Running pipeline " + p);
            Job job = jet.newJob(p);
            Thread.sleep(5000);
            producer.keepGoing = false;
            job.cancel();
        } finally {
            producer.keepGoing = false;
            Jet.shutdownAll();
        }
    }

    @SuppressWarnings("Convert2MethodRef") // https://bugs.openjdk.java.net/browse/JDK-8154236
    private static Pipeline coGroupDirect() {
        Pipeline p = Pipeline.create();

        StreamStageWithGrouping<PageVisit, Integer> pageVisits = p
                .drawFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .addTimestamps(pv -> pv.timestamp(), 100)
                .groupingKey(pv -> pv.userId());
        StreamStageWithGrouping<Payment, Integer> payments = p
                .drawFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .addTimestamps(pm -> pm.timestamp(), 100)
                .groupingKey(pm -> pm.userId());
        StreamStageWithGrouping<AddToCart, Integer> addToCarts = p
                .drawFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .addTimestamps(atc -> atc.timestamp(), 100)
                .groupingKey(atc -> atc.userId());

        StageWithGroupingAndWindow<PageVisit, Integer> windowStage = pageVisits.window(sliding(10, 1));

        StreamStage<TimestampedEntry<Integer, ThreeBags<PageVisit, AddToCart, Payment>>> coGrouped = windowStage
                .aggregate3(addToCarts, payments, toThreeBags());

        coGrouped.drainTo(Sinks.logger());
        return p;
    }

    @SuppressWarnings("Convert2MethodRef") // https://bugs.openjdk.java.net/browse/JDK-8154236
    private static Pipeline coGroupBuild() {
        Pipeline p = Pipeline.create();

        StreamStageWithGrouping<PageVisit, Integer> pageVisits = p
                .drawFrom(Sources.<PageVisit, Integer, PageVisit>mapJournal(PAGE_VISIT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .addTimestamps(pv -> pv.timestamp(), 100)
                .groupingKey(pv -> pv.userId());
        StreamStageWithGrouping<AddToCart, Integer> addToCarts = p
                .drawFrom(Sources.<AddToCart, Integer, AddToCart>mapJournal(ADD_TO_CART,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .addTimestamps(atc -> atc.timestamp(), 100)
                .groupingKey(atc -> atc.userId());
        StreamStageWithGrouping<Payment, Integer> payments = p
                .drawFrom(Sources.<Payment, Integer, Payment>mapJournal(PAYMENT,
                        mapPutEvents(), mapEventNewValue(), START_FROM_OLDEST))
                .addTimestamps(pm -> pm.timestamp(), 100)
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

    private static class ProducerTask implements Runnable {

        { new Thread(this, "WindowedCoGroup Producer").start(); }

        volatile boolean keepGoing = true;

        private final IMap<Object, PageVisit> pageVisit;
        private final IMap<Object, AddToCart> addToCart;
        private final IMap<Object, Payment> payment;

        private int loadTime = 1;
        private int quantity = 21;
        private int amount = 31;
        private long now = System.currentTimeMillis();

        ProducerTask(JetInstance jet) {
            pageVisit = jet.getMap(PAGE_VISIT);
            addToCart = jet.getMap(ADD_TO_CART);
            payment = jet.getMap(PAYMENT);
        }

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
}

