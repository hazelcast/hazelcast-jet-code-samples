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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.JetConfig;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.flatMap;
import static com.hazelcast.jet.Processors.groupAndAccumulate;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseStream;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.summarizingLong;

/**
 * Measures the performance of a Jet word count job optimized for single-node
 * operation. The primary motivation of this example is an apples-to-apples
 * comparison with the {@link WordCountJdk} example.
 * <p>
 * The DAG used here is optimized for the assumption of single-node usage.
 * Compared to the one used in the {@link WordCount} example, the source vertex
 * immediately opens all the files and emits their lines ({@code doc-lines} is
 * merged into {@code source}), and the {@code combine} vertex is simply removed,
 * which also removes the distributed edge towards it. Finally, instead of
 * writing to an {@code IMap}, it writes to a simple {@code ConcurrentHashMap}.
 */
public class WordCountSingleNode {

    private JetInstance jet;

    public static void main(String[] args) throws Exception {
        new WordCountSingleNode().go();
    }

    private void go() throws Exception {
        List<Long> timings = new ArrayList<>();
        try {
            setup();
            // Warmup
            measure();
            measure();
            measure();
            for (int i = 0; i < 9; i++) {
                timings.add(measure());
                System.gc();
            }
        } finally {
            Jet.shutdownAll();
        }
        System.out.println(timings.stream().collect(summarizingLong(x -> x)));
    }

    private long measure() throws InterruptedException, ExecutionException {
        System.out.print("\nCounting words... ");
        final Map<String, Long> counts = new ConcurrentHashMap<>();
        final Job job = jet.newJob(buildDag(counts));
        long start = System.nanoTime();
        job.execute().get();
        final long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.print("done in " + took + " milliseconds.");
//        printResults(counts);
        return took;
    }

    private void setup() {
        JetConfig cfg = new JetConfig();
        System.out.println("Creating Jet instance");
        jet = Jet.newJetInstance(cfg);
    }

    private static void printResults(Map<String, Long> counts) {
        final int limit = 100;
        System.out.format(" Top %d entries are:%n", limit);
        System.out.println("/-------+---------\\");
        System.out.println("| Count | Word    |");
        System.out.println("|-------+---------|");
        counts.entrySet().stream()
              .sorted(comparingLong(Entry<String, Long>::getValue).reversed())
              .limit(limit)
              .forEach(e -> System.out.format("|%6d | %-8s|%n", e.getValue(), e.getKey()));
        System.out.println("\\-------+---------/");
    }

    @Nonnull
    private static DAG buildDag(Map<String, Long> counts) {
        final Pattern delimiter = Pattern.compile("\\W+");
        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", DocLinesP::new);
        Vertex tokenize = dag.newVertex("tokenize",
                flatMap((String line) -> traverseArray(delimiter.split(line.toLowerCase()))
                                            .filter(word -> !word.isEmpty()))
        );
        Vertex reduce = dag.newVertex("reduce",
                groupAndAccumulate(() -> 0L, (count, x) -> count + 1)
        );
        Vertex sink = dag.newVertex("sink", () -> new MapSinkP(counts));
        return dag.edge(between(source.localParallelism(1), tokenize))
                  .edge(between(tokenize, reduce).partitioned(wholeItem(), HASH_CODE))
                  .edge(between(reduce, sink));
    }

    private static Stream<String> docFilenames() {
        final ClassLoader cl = WordCountSingleNode.class.getClassLoader();
        final BufferedReader r = new BufferedReader(new InputStreamReader(cl.getResourceAsStream("books"), UTF_8));
        return r.lines().onClose(() -> close(r));
    }

    private static void close(Closeable c) {
        try {
            c.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<String> bookLines(String name) {
        try {
            return Files.lines(Paths.get(WordCountSingleNode.class.getResource("books/" + name).toURI()));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static class DocLinesP extends AbstractProcessor {
        private final Traverser<String> docLines =
                traverseStream(docFilenames().flatMap(WordCountSingleNode::bookLines));

        @Override
        public boolean complete() {
            return emitFromTraverser(docLines);
        }

        @Override
        public boolean isCooperative() {
            return false;
        }
    }

    private static class MapSinkP extends AbstractProcessor {
        private final Map<String, Long> counts;

        MapSinkP(Map<String, Long> counts) {
            this.counts = counts;
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            final Entry<String, Long> e = (Entry<String, Long>) item;
            counts.put(e.getKey(), e.getValue());
            return true;
        }
    }
}
