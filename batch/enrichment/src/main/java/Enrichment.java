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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.HashJoinBuilder;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import datamodel.Broker;
import datamodel.Product;
import datamodel.Trade;

import java.util.Map.Entry;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.JoinClause.joinMapEntries;

/**
 * Demonstrates the usage of the Pipeline API's hash join transform to
 * enrich a data stream by attaching to each item additional data received
 * from other streams. In the example the source for the enriching streams
 * are Hazelcast IMaps.
 */
public final class Enrichment {
    private static final String TRADES = "trades";
    private static final String PRODUCTS = "products";
    private static final String BROKERS = "brokers";
    private static final String RESULT = "result";

    private final JetInstance jet;

    private Tag<Product> productTag;
    private Tag<Broker> brokerTag;

    private Enrichment(JetInstance jet) {
        this.jet = jet;
    }

    // Demonstrates the use of the simple, fully typesafe API to construct
    // a hash join with up to two enriching streams
    private static Pipeline joinDirect() {
        Pipeline p = Pipeline.create();

        // The stream to be enriched: trades
        ComputeStage<Trade> trades = p.drawFrom(Sources.<Trade>readList(TRADES));

        // The enriching streams: products and brokers
        ComputeStage<Entry<Integer, Product>> prodEntries = p.drawFrom(Sources.<Integer, Product>readMap(PRODUCTS));
        ComputeStage<Entry<Integer, Broker>> brokEntries = p.drawFrom(Sources.<Integer, Broker>readMap(BROKERS));

        // Join the trade stream with the product and broker streams
        ComputeStage<Tuple3<Trade, Product, Broker>> joined = trades.hashJoin(
                prodEntries, joinMapEntries(Trade::productId),
                brokEntries, joinMapEntries(Trade::brokerId)
        );

        // Transform the tuples of the hash join output into map entries
        // and store them in the output map
        joined.map(t -> entry(t.f0().id(), t))
              .drainTo(Sinks.writeMap(RESULT));

        return p;
    }

    // Demonstrates the use of the more general, but less typesafe API
    // that can construct a hash join with arbitrarily many enriching streams
    private Pipeline joinBuild() {

        Pipeline p = Pipeline.create();

        // The stream to be enriched: trades
        ComputeStage<Trade> trades = p.drawFrom(Sources.<Trade>readList(TRADES));

        // The enriching streams: products and brokers
        ComputeStage<Entry<Integer, Product>> prodEntries = p.drawFrom(Sources.<Integer, Product>readMap(PRODUCTS));
        ComputeStage<Entry<Integer, Broker>> brokEntries = p.drawFrom(Sources.<Integer, Broker>readMap(BROKERS));

        // Obtain a hash-join builder object from the stream to be enriched
        HashJoinBuilder<Trade> builder = trades.hashJoinBuilder();

        // Add enriching streams to the builder. Here we add just two, but
        // any number of them could be added.
        productTag = builder.add(prodEntries, joinMapEntries(Trade::productId));
        brokerTag = builder.add(brokEntries, joinMapEntries(Trade::brokerId));

        // Build the hash join stage
        ComputeStage<Tuple2<Trade, ItemsByTag>> joined = builder.build();

        // Transform the tuples of the hash join output into map entries
        // and store them in the output map
        joined.map(t -> entry(t.f0().id(), t))
              .drainTo(Sinks.writeMap(RESULT));
        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        new Enrichment(jet).go();
    }

    private void go() throws Exception {
        prepareSampleData();
        try {
            execute(joinDirect());
            validateJoinDirectResults();

            jet.getMap(RESULT).clear();

            execute(joinBuild());
            validateJoinBuildResults();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void validateJoinDirectResults() {
        IMap<Integer, Tuple3<Trade, Product, Broker>> result = jet.getMap(RESULT);
        printImap(result);
        for (int tradeId = 1; tradeId < 5; tradeId++) {
            Tuple3<Trade, Product, Broker> value = result.get(tradeId);
            Trade trade = value.f0();
            Product product = value.f1();
            Broker broker = value.f2();
            assertEquals(trade.productId(), product.id());
            assertEquals(trade.brokerId(), broker.id());
        }
        System.out.println("JoinDirect results are valid");
    }

    private void validateJoinBuildResults() {
        IMap<Integer, Tuple2<Trade, ItemsByTag>> result = jet.getMap(RESULT);
        printImap(result);
        for (int tradeId = 1; tradeId < 5; tradeId++) {
            Tuple2<Trade, ItemsByTag> value = result.get(tradeId);
            Trade trade = value.f0();
            ItemsByTag ibt = value.f1();
            Product product = ibt.get(productTag);
            Broker broker = ibt.get(brokerTag);
            assertEquals(trade.productId(), product.id());
            assertEquals(trade.brokerId(), broker.id());
        }
        System.out.println("JoinBuild results are valid");
    }

    private void prepareSampleData() {
        IMap<Integer, Product> productMap = jet.getMap(PRODUCTS);
        IMap<Integer, Broker> brokerMap = jet.getMap(BROKERS);
        IList<Trade> tradeList = jet.getList(TRADES);

        int productId = 21;
        int brokerId = 31;
        int tradeId = 1;
        for (int i = 0; i < 4; i++) {
            Product prod = new Product(productId);
            Broker brok = new Broker(brokerId);
            Trade trad = new Trade(tradeId, productId, brokerId);

            productMap.put(productId, prod);
            brokerMap.put(brokerId, brok);
            tradeList.add(trad);

            tradeId++;
            productId++;
            brokerId++;
        }
        printImap(productMap);
        printImap(brokerMap);
        printIlist(tradeList);
    }

    private void execute(Pipeline p) throws Exception {
        jet.newJob(p).join();
    }

    private static void assertEquals(long expected, long actual) {
        if (expected != actual) {
            throw new AssertionError("Expected != actual: " + expected + " != " + actual);
        }
    }

    private static <K, V> void printImap(IMap<K, V> imap) {
        StringBuilder sb = new StringBuilder();
        System.out.println(imap.getName() + ':');
        imap.forEach((k, v) -> sb.append(k).append("->").append(v).append('\n'));
        System.out.println(sb);
    }

    private static void printIlist(IList<?> list) {
        StringBuilder sb = new StringBuilder();
        System.out.println(list.getName() + ':');
        list.forEach(e -> sb.append(e).append('\n'));
        System.out.println(sb);
    }
}
