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

import com.hazelcast.jet.AggregateOperations;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;

import java.util.Map.Entry;

import static com.hazelcast.jet.AggregateOperations.summingToLong;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;

/**
 * Word count sample implemented with Hazelcast Jet's Core API.
 */
public class WordCountCoreApi {
    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        Jet.newJetInstance();
        JetInstance jet = Jet.newJetInstance();
        try {
            DAG dag = new DAG();
            Vertex source = dag.newVertex("source", Processors.readMap("sourceMap"));
            Vertex map = dag.newVertex("map", Processors.flatMap((String line) ->
                    traverseArray(line.toLowerCase().split("\\W+"))
                            .filter(word -> !word.isEmpty())));
            Vertex reduce = dag.newVertex("reduce", Processors.groupByKey(
                    wholeItem(), AggregateOperations.counting()));
            Vertex combine = dag.newVertex("combine", Processors.groupByKey(
                    entryKey(), summingToLong(Entry<String, Long>::getValue)));
            Vertex sink = dag.newVertex("sink", Processors.writeMap("sinkMap"));
            dag.edge(between(source, map))
               .edge(between(map, reduce).partitioned(wholeItem(), HASH_CODE))
               .edge(between(reduce, combine).partitioned(entryKey()).distributed())
               .edge(between(combine, sink.localParallelism(1)));
            jet.newJob(dag).execute().get();
        } finally {
            Jet.shutdownAll();
        }
    }
}
