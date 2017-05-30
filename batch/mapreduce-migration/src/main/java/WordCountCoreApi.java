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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.AggregateOperations;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.Sources;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.processor.Sinks.writeMap;

/**
 * Word count sample implemented without relying on out-of-the-box processors.
 * Demonstrates the lower-level convenience API to implement a cooperative
 * processor.
 */
public class WordCountCoreApi {
    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        Jet.newJetInstance();
        JetInstance jet = Jet.newJetInstance();
        try {
            DAG dag = new DAG();
            Vertex source = dag.newVertex("source", Sources.readMap("documents"));
            Vertex map = dag.newVertex("map", Processors.flatMap(
                    (String document) -> traverseArray(document.split("\\W+"))));
            Vertex reduce = dag.newVertex("reduce", Processors.accumulateByKey(
                    wholeItem(), AggregateOperations.counting()));
            Vertex combine = dag.newVertex("combine", Processors.combineByKey(
                    AggregateOperations.counting()));
            Vertex sink = dag.newVertex("sink", writeMap("counts"));

            source.localParallelism(1);

            dag.edge(between(source, map))
               .edge(between(map, reduce).partitioned(wholeItem(), HASH_CODE))
               .edge(between(reduce, combine).partitioned(entryKey()).distributed())
               .edge(between(combine, sink));

            jet.newJob(dag).execute().get();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static class MapP extends AbstractProcessor {
        private final FlatMapper<Entry<Long, String>, String> flatMapper = flatMapper(
                (Entry<Long, String> e) -> new WordTraverser(e.getValue())
        );

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            return flatMapper.tryProcess((Entry<Long, String>) item);
        }
    }

    private static class WordTraverser implements Traverser<String> {

        private final StringTokenizer tokenizer;

        WordTraverser(String document) {
            this.tokenizer = new StringTokenizer(document.toLowerCase());
        }

        @Override
        public String next() {
            return tokenizer.hasMoreTokens() ? tokenizer.nextToken() : null;
        }
    }

    private static class ReduceP extends AbstractProcessor {
        private final Map<String, Long> wordToCount = new HashMap<>();
        private final Traverser<Entry<String, Long>> resultTraverser =
                lazy(() -> traverseIterable(wordToCount.entrySet()));

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            wordToCount.compute((String) item, (x, count) -> 1 + (count != null ? count : 0L));
            return true;
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(resultTraverser);
        }
    }

    private static class CombineP extends AbstractProcessor {
        private final Map<String, Long> wordToCount = new HashMap<>();
        private final Traverser<Entry<String, Long>> resultTraverser =
                lazy(() -> traverseIterable(wordToCount.entrySet()));

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            final Entry<String, Long> e = (Entry<String, Long>) item;
            wordToCount.compute(e.getKey(),
                    (x, count) -> e.getValue() + (count != null ? count : 0L));
            return true;
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(resultTraverser);
        }
    }
}
