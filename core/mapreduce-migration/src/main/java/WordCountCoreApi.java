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
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.KeyExtractors.entryKey;
import static com.hazelcast.jet.KeyExtractors.wholeItem;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.Processors.writeMap;
import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseIterable;

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
            Vertex source = dag.newVertex("source", readMap("sourceMap"));
            Vertex map = dag.newVertex("map", MapP::new);
            Vertex reduce = dag.newVertex("reduce", ReduceP::new);
            Vertex combine = dag.newVertex("combine", CombineP::new);
            Vertex sink = dag.newVertex("sink", writeMap("sinkMap"));
            dag.edge(between(source, map))
               .edge(between(map, reduce).partitioned(wholeItem(), HASH_CODE))
               .edge(between(reduce, combine).partitioned(entryKey()).distributed())
               .edge(between(combine, sink.localParallelism(1)));
            jet.newJob(dag).execute().get();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static class MapP extends AbstractProcessor {
        private final FlatMapper<String> flatMapper = new FlatMapper<>();

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            return flatMapper.tryProcess((Entry<Long, String>) item, e -> new WordTraverser(e.getValue()));
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
            return emitCooperatively(resultTraverser);
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
            return emitCooperatively(resultTraverser);
        }
    }
}
