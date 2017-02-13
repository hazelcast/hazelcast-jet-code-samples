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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.EdgeConfig;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.KeyExtractors.wholeItem;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.flatMap;
import static com.hazelcast.jet.Processors.groupAndAccumulate;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.Processors.writeMap;
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
 * The DAG used here is the one used in the {@link WordCount} example, but
 * without the {@code combine} vertex, which also removes the distributed
 * edge towards it.
 */
public class WordCountSingleNode {

    private static final String DOCID_NAME = "docId_name";
    private static final String COUNTS = "counts";

    private JetInstance jet;

    public static void main(String[] args) throws Exception {
        new WordCountSingleNode().go();
    }

    private void go() throws Exception {
        List<Long> timings = new ArrayList<>();
        try {
            setup();
            final Job job = jet.newJob(buildDag());
            // Warmup
            measure(job);
            measure(job);
            measure(job);
            for (int i = 0; i < 9; i++) {
                timings.add(measure(job));
            }
        } finally {
            Jet.shutdownAll();
        }
        System.out.println(timings.stream().collect(summarizingLong(x -> x)));
    }

    private long measure(Job job) throws InterruptedException, ExecutionException {
        System.out.print("\nCounting words... ");
        long start = System.nanoTime();
        job.execute().get();
        final long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.print("done in " + took + " milliseconds.");
//        printResults();
        jet.getMap(COUNTS).clear();
        System.gc();
        return took;
    }

    private void setup() {
        JetConfig cfg = new JetConfig();
        System.out.println("Creating Jet instance");
        jet = Jet.newJetInstance(cfg);
        System.out.println("These books will be analyzed:");
        final IMap<Long, String> docId2Name = jet.getMap(DOCID_NAME);
        final long[] docId = {0};
        docFilenames().peek(System.out::println).forEach(line -> docId2Name.put(++docId[0], line));
    }

    private void printResults() {
        final int limit = 100;
        System.out.format(" Top %d entries are:%n", limit);
        final Map<String, Long> counts = jet.getMap(COUNTS);
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
    private static DAG buildDag() {
        final Pattern delimiter = Pattern.compile("\\W+");
        final Distributed.Supplier<Long> initialZero = () -> 0L;

        DAG dag = new DAG();
        // nil -> (docId, docName)
        Vertex source = dag.newVertex("source", readMap(DOCID_NAME));
        // (docId, docName) -> lines
        Vertex docLines = dag.newVertex("doc-lines", DocLinesP::new);
        // line -> words
        Vertex tokenize = dag.newVertex("tokenize",
                flatMap((String line) -> traverseArray(delimiter.split(line.toLowerCase()))
                                            .filter(word -> !word.isEmpty()))
        );
        // word -> (word, count)
        Vertex reduce = dag.newVertex("reduce",
                groupAndAccumulate(initialZero, (count, x) -> count + 1)
        );
        // (word, count) -> nil
        Vertex sink = dag.newVertex("sink", writeMap("counts"));

        return dag.edge(between(source.localParallelism(1), docLines))
                  .edge(between(docLines.localParallelism(1), tokenize))
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

    private static class DocLinesP extends AbstractProcessor {
        private final FlatMapper<Entry<Long, String>, String> flatMapper =
                flatMapper(e -> traverseStream(bookLines(e.getValue())));

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws Exception {
            return flatMapper.tryProcess((Entry<Long, String>) item);
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        private static Stream<String> bookLines(String name) {
            try {
                return Files.lines(Paths.get(WordCountSingleNode.class.getResource("books/" + name).toURI()));
            } catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
