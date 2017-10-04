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

package refman;

import com.hazelcast.jet.CoGroupBuilder;
import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import refman.datamodel.cogroup.AddToCart;
import refman.datamodel.cogroup.Delivery;
import refman.datamodel.cogroup.PageVisit;
import refman.datamodel.cogroup.Payment;

import java.util.Map.Entry;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.hazelcast.jet.Sources.readList;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

public class CoGroupRefMan {
    static void coGroupDirect() {
        Pipeline p = Pipeline.create();
        ComputeStage<String> src1 = p.drawFrom(Sources.readList("src1"));
        ComputeStage<String> src2 = p.drawFrom(Sources.readList("src2"));
        ComputeStage<Entry<String, Long>> coGrouped =
                src1.coGroup(wholeItem(), src2, wholeItem(), counting2());
    }

    private static AggregateOperation2<Object, Object, LongAccumulator, Long> counting2() {
        return AggregateOperation
                .withCreate(LongAccumulator::new)
                .andAccumulate0((count, item) -> count.add(1))
                .andAccumulate1((count, item) -> count.add(10))
                .andCombine(LongAccumulator::add)
                .andFinish(LongAccumulator::get);
    }

    static void coGroupThree() {
        Pipeline p = Pipeline.create();
        ComputeStage<PageVisit> pageVisit = p.drawFrom(readList("pageVisit"));
        ComputeStage<AddToCart> addToCart = p.drawFrom(readList("addToCart"));
        ComputeStage<Payment> payment = p.drawFrom(readList("payment"));

        AggregateOperation3<PageVisit, AddToCart, Payment, LongAccumulator[], long[]> aggrOp =
                AggregateOperation
                        .withCreate(() -> Stream.generate(LongAccumulator::new)
                                                .limit(3)
                                                .toArray(LongAccumulator[]::new))
                        .<PageVisit>andAccumulate0((accs, pv) -> accs[0].add(pv.loadTime()))
                        .<AddToCart>andAccumulate1((accs, atc) -> accs[1].add(atc.quantity()))
                        .<Payment>andAccumulate2((accs, pm) -> accs[2].add(pm.amount()))
                        .andCombine((accs1, accs2) -> IntStream.range(0, 2)
                                                               .forEach(i -> accs1[i].add(accs2[i])))
                        .andFinish(accs -> Stream.of(accs)
                                                 .mapToLong(LongAccumulator::get)
                                                 .toArray());
        ComputeStage<Entry<Long, long[]>> coGrouped = pageVisit.coGroup(PageVisit::userId,
                addToCart, AddToCart::userId,
                payment, Payment::userId,
                aggrOp);
    }

    static void coGroupBuild() {
        Pipeline p = Pipeline.create();
        ComputeStage<PageVisit> pageVisit = p.drawFrom(readList("pageVisit"));
        ComputeStage<AddToCart> addToCart = p.drawFrom(readList("addToCart"));
        ComputeStage<Payment> payment = p.drawFrom(readList("payment"));
        ComputeStage<Delivery> delivery = p.drawFrom(readList("delivery"));

        CoGroupBuilder<Long, PageVisit> b = pageVisit.coGroupBuilder(PageVisit::userId);
        Tag<PageVisit> pageVisitTag = b.tag0();
        Tag<AddToCart> addToCartTag = b.add(addToCart, AddToCart::userId);
        Tag<Payment> paymentTag = b.add(payment, Payment::userId);
        Tag<Delivery> deliveryTag = b.add(delivery, Delivery::userId);

        ComputeStage<Tuple2<Long, long[]>> coGrouped = b.build(AggregateOperation
                .withCreate(() -> Stream.generate(LongAccumulator::new)
                                        .limit(4)
                                        .toArray(LongAccumulator[]::new))
                .andAccumulate(pageVisitTag, (accs, pv) -> accs[0].add(pv.loadTime()))
                .andAccumulate(addToCartTag, (accs, atc) -> accs[1].add(atc.quantity()))
                .andAccumulate(paymentTag, (accs, pm) -> accs[2].add(pm.amount()))
                .andAccumulate(deliveryTag, (accs, d) -> accs[3].add(d.days()))
                .andCombine((accs1, accs2) -> IntStream.range(0, 3)
                                                       .forEach(i -> accs1[i].add(accs2[i])))
                .andFinish(accs -> Stream.of(accs)
                                         .mapToLong(LongAccumulator::get)
                                         .toArray())
        );
    }
}
