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
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.processor.Processors.accumulateByKey;
import static com.hazelcast.jet.processor.Processors.combineByKey;
import static com.hazelcast.jet.processor.Processors.flatMap;
import static com.hazelcast.jet.processor.Processors.nonCooperative;
import static com.hazelcast.jet.processor.SourceProcessors.readMap;
import static com.hazelcast.jet.processor.SinkProcessors.writeMap;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.lang.Runtime.getRuntime;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparingLong;

/**
 * Analyzes a set of documents and finds the number of occurrences of each word
 * they contain. Implemented with the following DAG:
 * <pre>
 *                --------
 *               | source |
 *                --------
 *                    |
 *            (docId, docName)
 *                    V
 *               -----------
 *              | doc-lines |
 *               -----------
 *                    |
 *                 (line)
 *                    V
 *               ----------
 *              | tokenize |
 *               ----------
 *                    |
 *                 (word)
 *                    V
 *              ------------
 *             | accumulate |
 *              ------------
 *                    |
 *            (word, localCount)
 *                    V
 *                ---------
 *               | combine |
 *                ---------
 *                    |
 *           (word, totalCount)
 *                    V
 *                 ------
 *                | sink |
 *                 ------
 * </pre>
 * This is how the DAG works:
 * <ul><li>
 *     In the {@code sample-data} module there are some books in plain text
 *     format.
 * </li><li>
 *     Each book is assigned an ID and a Hazelcast distributed map is built that
 *     maps from document ID to document name. This is the DAG's source.
 * </li><li>
 *     {@code source} emits {@code (docId, docName)} pairs. On each cluster
 *     member this vertex observes only the map entries stored locally on that
 *     member. Therefore each member sees a unique subset of all the documents.
 * </li><li>
 *     {@code doc-lines} reads each document and emits its lines of text as
 *     {@code (docId, line)} pairs.
 * </li><li>
 *     Lines are sent over a <em>local</em> edge to the {@code tokenize} vertex.
 *     This means that the tuples stay within the member where they were created
 *     and are routed to locally-running {@code tokenize} processors. Since the
 *     edge isn't partitioned, the choice of processor is arbitrary but fair and
 *     balances the traffic to each processor.
 * </li><li>
 *     {@code tokenize} splits each line into words and emits them.
 * </li><li>
 *     Words are sent over a <em>partitioned local</em> edge which routes
 *     all the items with the same word to the same local {@code reduce}
 *     processor.
 * </li><li>
 *     {@code reduce} collates tuples by word and maintains the count of each
 *     seen word. After having received all the input from {@code tokenize}, it
 *     emits tuples of the form {@code (word, localCount)}.
 * </li><li>
 *     Tuples with local sums are sent to {@code combine} over a <em>distributed
 *     partitioned</em> edge. This means that for each word there will be a single
 *     unique instance of a {@code combine} processor in the whole cluster and
 *     tuples will be sent over the network if needed.
 * </li><li>
 *     {@code combine} combines the partial sums into totals and emits them.
 * </li><li>
 *     Finally, the {@code sink} vertex stores the result in the output Hazelcast
 *     map, named {@value #COUNTS}.
 * </li></ul>
 */
public class WordCount {

    private static final String DOCID_NAME = "docId_name";
    private static final String COUNTS = "counts";

    private JetInstance jet;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        new WordCount().go();
    }

    private void go() throws Exception {
        try {
            setup();
            System.out.print("\nCounting words... ");
            long start = System.nanoTime();
            jet.newJob(buildDag()).join();
            System.out.print("done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
            printResults();
        } finally {
            Jet.shutdownAll();
        }
    }

    private void setup() {
        JetConfig cfg = new JetConfig();
        cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                Math.max(1, getRuntime().availableProcessors() / 2)));

        System.out.println("Creating Jet instance 1");
        jet = Jet.newJetInstance(cfg);
        System.out.println("Creating Jet instance 2");
        Jet.newJetInstance(cfg);
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
        final DistributedSupplier<Long> initialZero = () -> 0L;

        DAG dag = new DAG();
        // nil -> (docId, docName)
        Vertex source = dag.newVertex("source", readMap(DOCID_NAME));
        // (docId, docName) -> lines
        Vertex docLines = dag.newVertex("doc-lines",
                nonCooperative(flatMap((Entry<?, String> e) -> traverseStream(docLines(e.getValue()))))
        );
        // line -> words
        Vertex tokenize = dag.newVertex("tokenize",
                flatMap((String line) -> traverseArray(delimiter.split(line.toLowerCase()))
                                            .filter(word -> !word.isEmpty()))
        );
        // word -> (word, count)
        Vertex accumulate = dag.newVertex("accumulate", accumulateByKey(wholeItem(), counting()));
        // (word, count) -> (word, count)
        Vertex combine = dag.newVertex("combine", combineByKey(counting()));
        // (word, count) -> nil
        Vertex sink = dag.newVertex("sink", writeMap("counts"));

        return dag.edge(between(source.localParallelism(1), docLines))
                  .edge(between(docLines.localParallelism(1), tokenize))
                  .edge(between(tokenize, accumulate)
                          .partitioned(wholeItem(), HASH_CODE))
                  .edge(between(accumulate, combine)
                          .distributed()
                          .partitioned(entryKey()))
                  .edge(between(combine, sink));
    }

    private static Stream<String> docFilenames() {
        final ClassLoader cl = WordCount.class.getClassLoader();
        final BufferedReader r = new BufferedReader(new InputStreamReader(cl.getResourceAsStream("books"), UTF_8));
        return r.lines().onClose(() -> close(r));
    }

    private static Stream<String> docLines(String name) {
        try {
            return Files.lines(Paths.get(WordCount.class.getResource("books/" + name).toURI()));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static void close(Closeable c) {
        try {
            c.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
