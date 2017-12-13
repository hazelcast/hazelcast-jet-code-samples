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
import com.hazelcast.jet.HashJoinBuilder;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import datamodel.Broker;
import datamodel.Product;
import datamodel.Trade;

import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.JoinClause.joinMapEntries;
import static com.hazelcast.jet.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Demonstrates the usage of the Pipeline API's hash-join transform to
 * enrich a data stream. We generate a stream of stock trade events. Each
 * event has an associated product ID and broker ID. We use a hash-join
 * to enrich the trade with the product and broker objects coming from the
 * enriching streams.
 * <p>
 * All sources are Hazelcast {@code IMap}s. We generate the stream of trade
 * events by updating a single key in the {@code trades} map, which has the
 * Event Journal enabled, so the Jet job receives its update event stream.
 */
public final class Enrichment {
    private static final String TRADES = "trades";
    private static final String PRODUCTS = "products";
    private static final String BROKERS = "brokers";
    private static final int PRODUCT_ID_BASE = 21;
    private static final int BROKER_ID_BASE = 31;
    private static final int PRODUCT_BROKER_COUNT = 4;

    private final JetInstance jet;

    private Enrichment(JetInstance jet) {
        this.jet = jet;
    }

    // Demonstrates the use of the simple, fully typesafe API to construct
    // a hash join with up to two enriching streams
    private static Pipeline joinDirect() {
        Pipeline p = Pipeline.create();

        // The stream to be enriched: trades
        ComputeStage<Trade> trades =
                p.drawFrom(Sources.<Object, Trade>mapJournal(TRADES, START_FROM_CURRENT))
                 .map(entryValue());

        // The enriching streams: products and brokers
        ComputeStage<Entry<Integer, Product>> prodEntries = p.drawFrom(Sources.<Integer, Product>map(PRODUCTS));
        ComputeStage<Entry<Integer, Broker>> brokEntries = p.drawFrom(Sources.<Integer, Broker>map(BROKERS));

        // Join the trade stream with the product and broker streams
        ComputeStage<Tuple3<Trade, Product, Broker>> joined = trades.hashJoin(
                prodEntries, joinMapEntries(Trade::productId),
                brokEntries, joinMapEntries(Trade::brokerId)
        );

        // Validates the joined tuples and sends them to the logging sink
        joined.map(Enrichment::validateDirectJoinedItem)
              .drainTo(Sinks.logger());

        return p;
    }

    // Demonstrates the use of the more general, but less typesafe API
    // that can construct a hash join with arbitrarily many enriching streams
    private static Pipeline joinBuild() {

        Pipeline p = Pipeline.create();

        // The stream to be enriched: trades
        ComputeStage<Trade> trades =
                p.drawFrom(Sources.<Object, Trade>mapJournal(TRADES, START_FROM_CURRENT))
                 .map(entryValue());

        // The enriching streams: products and brokers
        ComputeStage<Entry<Integer, Product>> prodEntries = p.drawFrom(Sources.<Integer, Product>map(PRODUCTS));
        ComputeStage<Entry<Integer, Broker>> brokEntries = p.drawFrom(Sources.<Integer, Broker>map(BROKERS));

        // Obtain a hash-join builder object from the stream to be enriched
        HashJoinBuilder<Trade> builder = trades.hashJoinBuilder();

        // Add enriching streams to the builder. Here we add just two, but
        // any number of them could be added.
        Tag<Product> productTag = builder.add(prodEntries, joinMapEntries(Trade::productId));
        Tag<Broker> brokerTag = builder.add(brokEntries, joinMapEntries(Trade::brokerId));

        // Build the hash join stage
        ComputeStage<Tuple2<Trade, ItemsByTag>> joined = builder.build();

        // Validates the joined tuples and sends them to the logging sink
        joined.map(item -> validateBuildJoinedItem(item, productTag, brokerTag))
              .drainTo(Sinks.logger());
        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        // Lower operation timeout to speed up job cancellation
        System.setProperty("hazelcast.operation.call.timeout.millis", "1000");

        JetConfig cfg = new JetConfig();
        cfg.getHazelcastConfig().getMapEventJournalConfig(TRADES).setEnabled(true);
        JetInstance jet = Jet.newJetInstance(cfg);
        Jet.newJetInstance(cfg);

        new Enrichment(jet).go();
    }

    private void go() throws Exception {
        prepareEnrichingData();
        EventGenerator eventGenerator = new EventGenerator(jet.getMap(TRADES));
        eventGenerator.start();
        try {
            Job job1 = jet.newJob(joinDirect());
            eventGenerator.generateEventsForFiveSeconds();
            job1.cancel();
            Thread.sleep(2000);

            Job job2 = jet.newJob(joinBuild());
            eventGenerator.generateEventsForFiveSeconds();
            job2.cancel();
            Thread.sleep(2000);
        } finally {
            eventGenerator.shutdown();
            Jet.shutdownAll();
        }
    }

    private static <T extends Tuple3<Trade, Product, Broker>> T validateDirectJoinedItem(T item) {
        Trade trade = item.f0();
        Product product = item.f1();
        Broker broker = item.f2();
        if (trade.productId() != product.id() || trade.brokerId() != broker.id()) {
            throw new AssertionError("Invalid join: " + item);
        }
        return item;
    }

    private static <T extends Tuple2<Trade, ItemsByTag>> T validateBuildJoinedItem(
            T item, Tag<Product> productTag, Tag<Broker> brokerTag) {
        Trade trade = item.f0();
        Product product = item.f1().get(productTag);
        Broker broker = item.f1().get(brokerTag);
        if (product == null || trade.productId() != product.id()
            || broker == null || trade.brokerId() != broker.id()
        ) {
            throw new AssertionError("Invalid join: " + item);
        }
        return item;
    }

    private void prepareEnrichingData() {
        IMap<Integer, Product> productMap = jet.getMap(PRODUCTS);
        IMap<Integer, Broker> brokerMap = jet.getMap(BROKERS);

        int productId = PRODUCT_ID_BASE;
        int brokerId = BROKER_ID_BASE;
        for (int i = 0; i < PRODUCT_BROKER_COUNT; i++) {
            Product prod = new Product(productId);
            Broker brok = new Broker(brokerId);
            productMap.put(productId, prod);
            brokerMap.put(brokerId, brok);
            productId++;
            brokerId++;
        }
        printImap(productMap);
        printImap(brokerMap);
    }

    private static <K, V> void printImap(IMap<K, V> imap) {
        StringBuilder sb = new StringBuilder();
        System.out.println(imap.getName() + ':');
        imap.forEach((k, v) -> sb.append(k).append("->").append(v).append('\n'));
        System.out.println(sb);
    }

    private static class EventGenerator extends Thread {
        private volatile boolean enabled;
        private volatile boolean keepRunning = true;

        private final IMap<Object, Trade> trades;

        EventGenerator(IMap<Object, Trade> trades) {
            this.trades = trades;
        }

        @Override
        public void run() {
            Random rnd = ThreadLocalRandom.current();
            int tradeId = 1;
            while (keepRunning) {
                LockSupport.parkNanos(MILLISECONDS.toNanos(50));
                if (!enabled) {
                    continue;
                }
                Trade trad = new Trade(tradeId,
                        PRODUCT_ID_BASE + rnd.nextInt(PRODUCT_BROKER_COUNT),
                        BROKER_ID_BASE + rnd.nextInt(PRODUCT_BROKER_COUNT));
                trades.put(42, trad);
                tradeId++;
            }
        }

        void generateEventsForFiveSeconds() throws InterruptedException {
            enabled = true;
            System.out.println("\n\nGenerating trade events\n");
            Thread.sleep(5000);
            System.out.println("\n\nStopped trade events\n");
            enabled = false;
        }

        void shutdown() {
            keepRunning = false;
        }
    }
}
