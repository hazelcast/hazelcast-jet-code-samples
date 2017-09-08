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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Partitioner;
import com.hazelcast.jet.Traversers;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.SinkProcessors;
import com.hazelcast.jet.processor.SourceProcessors;

import java.util.Map;
import java.util.regex.Pattern;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.processor.Processors.accumulateByKey;
import static com.hazelcast.jet.processor.Processors.combineByKey;

public class DagBuilder {

    public static DAG buildDag(String sourceName, String sinkName) {
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", SourceProcessors.readMap(sourceName));
        // (lineNum, line) -> words
        Pattern delimiter = Pattern.compile("\\W+");
        Vertex tokenizer = dag.newVertex("tokenizer",
                Processors.flatMap((Map.Entry<Integer, String> e) ->
                        Traversers.traverseArray(delimiter.split(e.getValue().toLowerCase()))
                                  .filter(word -> !word.isEmpty()))
        );

        // word -> (word, count)
        Vertex accumulator = dag.newVertex("accumulator", accumulateByKey(wholeItem(), counting()));

        // (word, count) -> (word, count)
        Vertex combiner = dag.newVertex("combiner", combineByKey(counting()));

        Vertex sink = dag.newVertex("sink", SinkProcessors.writeMap(sinkName));

        dag.edge(between(source, tokenizer))
           .edge(between(tokenizer, accumulator)
                   .partitioned(wholeItem(), Partitioner.HASH_CODE))
           .edge(between(accumulator, combiner)
                   .distributed()
                   .partitioned(entryKey()))
           .edge(between(combiner, sink));
        return dag;
    }

}
