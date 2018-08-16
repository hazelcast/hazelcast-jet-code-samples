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

import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.Tuple3;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.BatchStageWithKey;
import com.hazelcast.jet.server.JetBootstrap;
import datamodel.AddToCart;
import datamodel.PageVisit;
import datamodel.Payment;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static com.hazelcast.jet.aggregate.AggregateOperations.toList;

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

    private final Map<Integer, Set<PageVisit>> userId2PageVisit = new HashMap<>();
    private final Map<Integer, Set<AddToCart>> userId2AddToCart = new HashMap<>();
    private final Map<Integer, Set<Payment>> userId2Payment = new HashMap<>();

    private CoGroup(JetInstance jet) {
        this.jet = jet;
    }

    private static Pipeline buildCoGroupPipeline() {
        Pipeline p = Pipeline.create();

        // Create three source streams
        BatchStageWithKey<PageVisit, Integer> pageVisits =
                p.drawFrom(Sources.<PageVisit>list(PAGE_VISIT))
                 .groupingKey(pageVisit -> pageVisit.userId());
        BatchStageWithKey<AddToCart, Integer> addToCarts =
                p.drawFrom(Sources.<AddToCart>list(ADD_TO_CART))
                 .groupingKey(addToCart -> addToCart.userId());
        BatchStageWithKey<Payment, Integer> payments =
                p.drawFrom(Sources.<Payment>list(PAYMENT))
                 .groupingKey(payment -> payment.userId());

        // Construct the co-group transform. The aggregate operation collects all
        // the stream items inside an accumulator class called ThreeBags.
        BatchStage<Entry<Integer, Tuple3<List<PageVisit>, List<AddToCart>, List<Payment>>>> coGrouped =
                pageVisits.aggregate3(toList(), addToCarts, toList(), payments, toList());

        // Store the results in the output map
        coGrouped.drainTo(Sinks.map(RESULT));
        return p;
    }

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = JetBootstrap.getInstance();
        new CoGroup(jet).go();
    }

    private void go() {
        prepareSampleData();
        try {
            jet.newJob(buildCoGroupPipeline()).join();
            validateCoGroupResults();
        } finally {
            clearSampleDataAndResults();
            Jet.shutdownAll();
        }
    }

    private void clearSampleDataAndResults() {
        jet.getMap(RESULT).clear();
        jet.getList(PAGE_VISIT).clear();
        jet.getList(ADD_TO_CART).clear();
        jet.getList(PAYMENT).clear();
    }

    private void validateCoGroupResults() {
        IMap<Integer, Tuple3<List<PageVisit>, List<AddToCart>, List<Payment>>> result = jet.getMap(RESULT);
        printImap(result);
        for (int userId = 11; userId < 13; userId++) {
            Tuple3<List<PageVisit>, List<AddToCart>, List<Payment>> bags = result.get(userId);
            assertEqual(userId2PageVisit.get(userId), bags.f0());
            assertEqual(userId2AddToCart.get(userId), bags.f1());
            assertEqual(userId2Payment.get(userId), bags.f2());
        }
        System.out.println("CoGroup results are valid");
    }

    private void prepareSampleData() {
        clearSampleDataAndResults();
        IList<AddToCart> addToCartList = jet.getList(ADD_TO_CART);
        IList<Payment> paymentList = jet.getList(PAYMENT);
        IList<PageVisit> pageVisitList = jet.getList(PAGE_VISIT);

        int quantity = 21;
        int amount = 31;
        int loadTime = 1;
        for (int userId = 11; userId < 13; userId++) {
            userId2AddToCart.put(userId, new HashSet<>());
            userId2Payment.put(userId, new HashSet<>());
            userId2PageVisit.put(userId, new HashSet<>());
            for (int i = 0; i < 2; i++) {
                PageVisit visit = new PageVisit(userId, loadTime);
                AddToCart atc = new AddToCart(userId, quantity);
                Payment pay = new Payment(userId, amount);

                addToCartList.add(atc);
                paymentList.add(pay);
                pageVisitList.add(visit);

                userId2AddToCart.get(userId).add(atc);
                userId2Payment.get(userId).add(pay);
                userId2PageVisit.get(userId).add(visit);

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
