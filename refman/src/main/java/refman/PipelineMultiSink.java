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

package refman;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.ArrayList;

import static java.util.Arrays.asList;

public class PipelineMultiSink {
    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        Pipeline p = Pipeline.create();
        BatchStage<String> src = p.drawFrom(Sources.list("src"));
        src.map(String::toUpperCase)
           .drainTo(Sinks.list("uppercase"));
        src.map(String::toLowerCase)
           .drainTo(Sinks.list("lowercase"));

        JetInstance jet = Jet.newJetInstance();
        try {
            jet.getList("src").addAll(asList("aA", "bB", "cC"));
            for (int i = 0; i < 20; i++) {
                jet.newJob(p).join();
                System.out.println(new ArrayList<>(jet.getList("uppercase")));
                System.out.println(new ArrayList<>(jet.getList("lowercase")));
            }
        } finally {
            Jet.shutdownAll();
        }

    }
}
