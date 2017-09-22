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
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation2;

import java.util.List;

import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.util.Arrays.asList;

public class CoGroupRefMan {
    public static void main(String[] args) throws Exception {
        Pipeline p = Pipeline.create();
        ComputeStage<String> s1 = p.drawFrom(Sources.readList("src1"));
        ComputeStage<String> s2 = p.drawFrom(Sources.readList("src2"));
        s1.coGroup(wholeItem(), s2, wholeItem(), counting2())
            .drainTo(Sinks.writeMap("result"));

        JetInstance jet = Jet.newJetInstance();
        try {
            List<String> src1 = jet.getList("src1");
            List<String> src2 = jet.getList("src2");
            src1.addAll(asList("a", "b", "c"));
            src2.addAll(asList("b", "c", "d"));
            p.execute(jet).get();
            System.out.println(jet.getMap("result").entrySet());
        } finally {
            Jet.shutdownAll();
        }
    }

    private static AggregateOperation2<String, String, LongAccumulator, Long> counting2() {
        return AggregateOperation
                .withCreate(LongAccumulator::new)
                .<String>andAccumulate0((count, item) -> count.add(1))
                .<String>andAccumulate1((count, item) -> count.add(1))
                .andCombine(LongAccumulator::add)
                .andFinish(LongAccumulator::get);
    }
}
