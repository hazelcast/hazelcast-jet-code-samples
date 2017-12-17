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

import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.GroupAggregateBuilder;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.StageWithGrouping;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation2;
import com.hazelcast.jet.aggregate.AggregateOperation3;
import com.hazelcast.jet.datamodel.Tag;
import refman.datamodel.cogroup.AddToCart;
import refman.datamodel.cogroup.Delivery;
import refman.datamodel.cogroup.PageVisit;
import refman.datamodel.cogroup.Payment;

import java.util.Map.Entry;

import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

public class CoGroupRefMan {
    static void coGroupDirect() {
        Pipeline p = Pipeline.create();
        ComputeStage<String> src1 = p.drawFrom(Sources.list("src1"));
        ComputeStage<String> src2 = p.drawFrom(Sources.list("src2"));

        StageWithGrouping<String, String> keyedSrc1 = src1.groupingKey(wholeItem());
        StageWithGrouping<String, String> keyedSrc2 = src2.groupingKey(wholeItem());

        ComputeStage<Entry<String, Long>> coGrouped = keyedSrc1.aggregate2(keyedSrc2, counting2());
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
        StageWithGrouping<PageVisit, Long> pageVisit =
                p.drawFrom(Sources.<PageVisit>list("pageVisit"))
                 .groupingKey(PageVisit::userId);
        StageWithGrouping<AddToCart, Long> addToCart =
                p.drawFrom(Sources.<AddToCart>list("addToCart"))
                 .groupingKey(AddToCart::userId);
        StageWithGrouping<Payment, Long> payment =
                p.drawFrom(Sources.<Payment>list("payment"))
                 .groupingKey(Payment::userId);

        AggregateOperation3<PageVisit, AddToCart, Payment, LongAccumulator[], long[]> aggrOp =
                AggregateOperation
                        .withCreate(() -> new LongAccumulator[] {
                                new LongAccumulator(),
                                new LongAccumulator(),
                                new LongAccumulator()
                        })
                        .<PageVisit>andAccumulate0((accs, pv) -> accs[0].add(pv.loadTime()))
                        .<AddToCart>andAccumulate1((accs, atc) -> accs[1].add(atc.quantity()))
                        .<Payment>andAccumulate2((accs, pm) -> accs[2].add(pm.amount()))
                        .andCombine((accs1, accs2) -> {
                            accs1[0].add(accs2[0]);
                            accs1[1].add(accs2[1]);
                            accs1[2].add(accs2[2]);
                        })
                        .andFinish(accs -> new long[] {
                                accs[0].get(),
                                accs[1].get(),
                                accs[2].get()
                        });
        ComputeStage<Entry<Long, long[]>> coGrouped = pageVisit.aggregate3(addToCart, payment, aggrOp);
    }

    static void coGroupBuild() {
        Pipeline p = Pipeline.create();
        StageWithGrouping<PageVisit, Long> pageVisit =
                p.drawFrom(Sources.<PageVisit>list("pageVisit"))
                 .groupingKey(PageVisit::userId);
        StageWithGrouping<AddToCart, Long> addToCart =
                p.drawFrom(Sources.<AddToCart>list("addToCart"))
                 .groupingKey(AddToCart::userId);
        StageWithGrouping<Payment, Long> payment =
                p.drawFrom(Sources.<Payment>list("payment"))
                 .groupingKey(Payment::userId);
        StageWithGrouping<Delivery, Long> delivery =
                p.drawFrom(Sources.<Delivery>list("delivery"))
                 .groupingKey(Delivery::userId);

        GroupAggregateBuilder<PageVisit, Long> b = pageVisit.aggregateBuilder();
        Tag<PageVisit> pvTag = b.tag0();
        Tag<AddToCart> atcTag = b.add(addToCart);
        Tag<Payment> pmtTag = b.add(payment);
        Tag<Delivery> delTag = b.add(delivery);

        ComputeStage<Entry<Long, long[]>> coGrouped = b.build(AggregateOperation
                .withCreate(() -> new LongAccumulator[]{
                        new LongAccumulator(),
                        new LongAccumulator(),
                        new LongAccumulator(),
                        new LongAccumulator()
                })
                .andAccumulate(pvTag, (accs, pv) -> accs[0].add(pv.loadTime()))
                .andAccumulate(atcTag, (accs, atc) -> accs[1].add(atc.quantity()))
                .andAccumulate(pmtTag, (accs, pm) -> accs[2].add(pm.amount()))
                .andAccumulate(delTag, (accs, d) -> accs[3].add(d.days()))
                .andCombine((accs1, accs2) -> {
                    accs1[0].add(accs2[0]);
                    accs1[1].add(accs2[1]);
                    accs1[2].add(accs2[2]);
                    accs1[3].add(accs2[3]);
                })
                .andFinish(accs -> new long[]{
                        accs[0].get(),
                        accs[1].get(),
                        accs[2].get(),
                        accs[3].get()
                })
        );
    }
}
