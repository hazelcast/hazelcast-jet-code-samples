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
import datamodel.Payment;
import datamodel.AddToCart;
import datamodel.PageVisit;

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
    private static final String PAGE_VISIT = "pageVisit";
    private static final String ADD_TO_CART = "addToCart";
    private static final String PAYMENT = "payment";
    private static final String RESULT = "result";
    private final JetInstance jet;

    private Tag<PageVisit> visitTag;
    private Tag<AddToCart> cartTag;
    private Tag<Payment> payTag;

    private final Map<Integer, Set<PageVisit>> userid2pageVisit = new HashMap<>();
    private final Map<Integer, Set<AddToCart>> userid2addToCart = new HashMap<>();
    private final Map<Integer, Set<Payment>> userid2payment = new HashMap<>();

    private CoGroup(JetInstance jet) {
        this.jet = jet;
    }

    private static Pipeline coGroupDirect() {
        Pipeline p = Pipeline.create();

        // Create three source streams
        StageWithGrouping<PageVisit, Integer> pageVisits =
                p.drawFrom(Sources.<PageVisit>list(PAGE_VISIT))
                 .groupingKey(PageVisit::userId);
        StageWithGrouping<AddToCart, Integer> addToCarts =
                p.drawFrom(Sources.<AddToCart>list(ADD_TO_CART))
                 .groupingKey(AddToCart::userId);
        StageWithGrouping<Payment, Integer> payments =
                p.drawFrom(Sources.<Payment>list(PAYMENT))
                 .groupingKey(Payment::userId);

        // Construct the co-group transform. The aggregate operation collects all
        // the stream items inside an accumulator class called ThreeBags.
        ComputeStage<Entry<Integer, ThreeBags<PageVisit, AddToCart, Payment>>> coGrouped = pageVisits.aggregate3(
                addToCarts, payments,
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

    private Pipeline coGroupBuild() {
        Pipeline p = Pipeline.create();

        // Create three source streams
        StageWithGrouping<PageVisit, Integer> pageVisits =
                p.drawFrom(Sources.<PageVisit>list(PAGE_VISIT))
                 .groupingKey(PageVisit::userId);
        StageWithGrouping<AddToCart, Integer> addToCarts =
                p.drawFrom(Sources.<AddToCart>list(ADD_TO_CART))
                 .groupingKey(AddToCart::userId);
        StageWithGrouping<Payment, Integer> payments =
                p.drawFrom(Sources.<Payment>list(PAYMENT))
                 .groupingKey(Payment::userId);

        // Obtain a builder object for the co-group transform
        GroupAggregateBuilder<PageVisit, Integer> builder = pageVisits.aggregateBuilder();
        Tag<PageVisit> visitTag = this.visitTag = builder.tag0();

        // Add the co-grouped streams to the builder. Here we add just two, but
        // any number of them could be added.
        Tag<AddToCart> cartTag = this.cartTag = builder.add(addToCarts);
        Tag<Payment> payTag = this.payTag = builder.add(payments);

        // Build the co-group transform. The aggregate operation collects all
        // the stream items inside an accumulator class called BagsByTag.
        ComputeStage<Entry<Integer, BagsByTag>> coGrouped = builder.build(AggregateOperation
                .withCreate(BagsByTag::new)
                .andAccumulate(visitTag, (acc, pageVisit) -> acc.ensureBag(visitTag).add(pageVisit))
                .andAccumulate(cartTag, (acc, addToCart) -> acc.ensureBag(cartTag).add(addToCart))
                .andAccumulate(payTag, (acc, payment) -> acc.ensureBag(payTag).add(payment))
                .andCombine(BagsByTag::combineWith)
                .andFinish(x -> x)
        );

        // Store the results in the output map
        coGrouped.drainTo(Sinks.map(RESULT));

        return p;
    }

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        new CoGroup(jet).go();
    }

    private void go() {
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
        IMap<Integer, ThreeBags<PageVisit, AddToCart, Payment>> result = jet.getMap(RESULT);
        printImap(result);
        for (int userId = 11; userId < 13; userId++) {
            ThreeBags<PageVisit, AddToCart, Payment> bags = result.get(userId);
            assertEqual(userid2pageVisit.get(userId), bags.bag0());
            assertEqual(userid2addToCart.get(userId), bags.bag1());
            assertEqual(userid2payment.get(userId), bags.bag2());
        }
        System.out.println("CoGroupDirect results are valid");
    }

    private void validateCoGroupBuildResults() {
        IMap<Integer, BagsByTag> result = jet.getMap(RESULT);
        printImap(result);
        for (int userId = 11; userId < 13; userId++) {
            BagsByTag bags = result.get(userId);
            assertEqual(userid2pageVisit.get(userId), bags.bag(visitTag));
            assertEqual(userid2addToCart.get(userId), bags.bag(cartTag));
            assertEqual(userid2payment.get(userId), bags.bag(payTag));
        }
        System.out.println("CoGroupBuild results are valid");
    }

    private void prepareSampleData() {
        IList<AddToCart> addToCartList = jet.getList(ADD_TO_CART);
        IList<Payment> paymentList = jet.getList(PAYMENT);
        IList<PageVisit> pageVisitList = jet.getList(PAGE_VISIT);

        int quantity = 21;
        int amount = 31;
        int loadTime = 1;
        for (int userId = 11; userId < 13; userId++) {
            userid2addToCart.put(userId, new HashSet<>());
            userid2payment.put(userId, new HashSet<>());
            userid2pageVisit.put(userId, new HashSet<>());
            for (int i = 0; i < 2; i++) {
                PageVisit visit = new PageVisit(userId, loadTime);
                AddToCart atc = new AddToCart(userId, quantity);
                Payment pay = new Payment(userId, amount);

                addToCartList.add(atc);
                paymentList.add(pay);
                pageVisitList.add(visit);

                userid2addToCart.get(userId).add(atc);
                userid2payment.get(userId).add(pay);
                userid2pageVisit.get(userId).add(visit);

                loadTime++;
                quantity++;
                amount++;
            }
        }
        printIlist(addToCartList);
        printIlist(paymentList);
        printIlist(pageVisitList);
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
