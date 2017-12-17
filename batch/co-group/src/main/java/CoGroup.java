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
import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.GroupAggregateBuilder;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.StageWithGrouping;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.datamodel.BagsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.ThreeBags;
import datamodel.Broker;
import datamodel.Product;
import datamodel.Trade;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Demonstrates the usage of Pipeline API's co-group transformation, which
 * joins two or more streams on a common key and performs a user-specified
 * aggregate operation on the co-grouped items.
 */
public final class CoGroup {
    private static final String TRADES = "trades";
    private static final String PRODUCTS = "products";
    private static final String BROKERS = "brokers";
    private static final String RESULT = "result";
    private final JetInstance jet;

    private Tag<Trade> tradeTag;
    private Tag<Product> productTag;
    private Tag<Broker> brokerTag;

    private final Map<Integer, Set<Trade>> class2trade = new HashMap<>();
    private final Map<Integer, Set<Product>> class2product = new HashMap<>();
    private final Map<Integer, Set<Broker>> class2broker = new HashMap<>();

    private CoGroup(JetInstance jet) {
        this.jet = jet;
    }

    private static Pipeline coGroupDirect() {
        Pipeline p = Pipeline.create();

        // Create three source streams
        StageWithGrouping<Trade, Integer> trades =
                p.drawFrom(Sources.<Trade>list(TRADES))
                 .groupingKey(Trade::classId);
        StageWithGrouping<Product, Integer> products =
                p.drawFrom(Sources.<Product>list(PRODUCTS))
                 .groupingKey(Product::classId);
        StageWithGrouping<Broker, Integer> brokers =
                p.drawFrom(Sources.<Broker>list(BROKERS))
                 .groupingKey(Broker::classId);

        // Construct the co-group transform. The aggregate operation collects all
        // the stream items inside an accumulator class called ThreeBags.
        ComputeStage<Entry<Integer, ThreeBags<Trade, Product, Broker>>> coGrouped = trades.aggregate3(
                products, brokers,
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

    private Pipeline coGroupBuild() {
        Pipeline p = Pipeline.create();

        // Create three source streams
        StageWithGrouping<Trade, Integer> trades =
                p.drawFrom(Sources.<Trade>list(TRADES))
                 .groupingKey(Trade::classId);
        StageWithGrouping<Product, Integer> products =
                p.drawFrom(Sources.<Product>list(PRODUCTS))
                 .groupingKey(Product::classId);
        StageWithGrouping<Broker, Integer> brokers =
                p.drawFrom(Sources.<Broker>list(BROKERS))
                 .groupingKey(Broker::classId);

        // Obtain a builder object for the co-group transform
        GroupAggregateBuilder<Trade, Integer> builder = trades.aggregateBuilder();
        Tag<Trade> tradeTag = this.tradeTag = builder.tag0();

        // Add the co-grouped streams to the builder. Here we add just two, but
        // any number of them could be added.
        Tag<Product> productTag = this.productTag = builder.add(products);
        Tag<Broker> brokerTag = this.brokerTag = builder.add(brokers);

        // Build the co-group transform. The aggregate operation collects all
        // the stream items inside an accumulator class called BagsByTag.
        ComputeStage<Entry<Integer, BagsByTag>> coGrouped = builder.build(AggregateOperation
                .withCreate(BagsByTag::new)
                .andAccumulate(tradeTag, (acc, trade) -> acc.ensureBag(tradeTag).add(trade))
                .andAccumulate(productTag, (acc, product) -> acc.ensureBag(productTag).add(product))
                .andAccumulate(brokerTag, (acc, broker) -> acc.ensureBag(brokerTag).add(broker))
                .andCombine(BagsByTag::combineWith)
                .andFinish(x -> x)
        );

        // Store the results in the output map
        coGrouped.drainTo(Sinks.map(RESULT));

        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        new CoGroup(jet).go();
    }

    private void go() throws Exception {
        prepareSampleData();
        try {
            jet.newJob(coGroupDirect()).join();
            validateCogroupDirectResults();

            jet.getMap(RESULT).clear();

            jet.newJob(coGroupBuild()).join();
            validateCoGroupBuildResults();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void validateCogroupDirectResults() {
        IMap<Integer, ThreeBags<Trade, Product, Broker>> result = jet.getMap(RESULT);
        printImap(result);
        for (int classId = 11; classId < 13; classId++) {
            ThreeBags<Trade, Product, Broker> bags = result.get(classId);
            assertEqual(class2trade.get(classId), bags.bag0());
            assertEqual(class2product.get(classId), bags.bag1());
            assertEqual(class2broker.get(classId), bags.bag2());
        }
        System.out.println("CoGroupDirect results are valid");
    }

    private void validateCoGroupBuildResults() {
        IMap<Integer, BagsByTag> result = jet.getMap(RESULT);
        printImap(result);
        for (int classId = 11; classId < 13; classId++) {
            BagsByTag bags = result.get(classId);
            assertEqual(class2trade.get(classId), bags.bag(tradeTag));
            assertEqual(class2product.get(classId), bags.bag(productTag));
            assertEqual(class2broker.get(classId), bags.bag(brokerTag));
        }
        System.out.println("CoGroupBuild results are valid");
    }

    private void prepareSampleData() {
        IList<Product> productList = jet.getList(PRODUCTS);
        IList<Broker> brokerList = jet.getList(BROKERS);
        IList<Trade> tradeList = jet.getList(TRADES);

        int productId = 21;
        int brokerId = 31;
        int tradeId = 1;
        for (int classId = 11; classId < 13; classId++) {
            class2product.put(classId, new HashSet<>());
            class2broker.put(classId, new HashSet<>());
            class2trade.put(classId, new HashSet<>());
            for (int i = 0; i < 2; i++) {
                Product prod = new Product(classId, productId);
                Broker brok = new Broker(classId, brokerId);
                Trade trad = new Trade(tradeId, classId, productId, brokerId);

                productList.add(prod);
                brokerList.add(brok);
                tradeList.add(trad);

                class2product.get(classId).add(prod);
                class2broker.get(classId).add(brok);
                class2trade.get(classId).add(trad);

                tradeId++;
                productId++;
                brokerId++;
            }
        }
        printIlist(productList);
        printIlist(brokerList);
        printIlist(tradeList);
    }

    private static <T> void assertEqual(Set<T> expected, Collection<T> actual) {
        if (actual.size() != expected.size() || !expected.containsAll(actual)) {
            throw new AssertionError("Mismatch: expected " + expected + "; actual " + actual);
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
