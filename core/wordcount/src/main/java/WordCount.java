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
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Suppliers.lazyIterate;
import static com.hazelcast.util.UuidUtil.newUnsecureUuidString;
import static java.util.Comparator.comparingLong;
import static java.util.stream.Collectors.toMap;

/**
 * A distributed word count job implemented with the following DAG:
 * <pre>
 *  --------                   -----------
 * | Source |-(rndKey, line)->| Generator |-(word, 1)--\
 *  --------                   -----------              |
 *  /--------------------------------------------------/
 * |    -------------                       -----------
 *  \->| Accumulator |-(word, localCount)->| Combiner  | -\
 *      -------------                       -----------    |
 *  /-----------------------------------------------------/
 * |                         ------
 *  \->(word, totalCount)-> | Sink |
 *                           ------
 * </pre>
 * <ul><li>
 *     In the {@code resources} directory there are some books in plain text format.
 * </li><li>
 *     Book content is inserted into a distributed Hazelcast map named {@code lines}.
 *     Each map entry is a line of text with a randomly chosen key. This achieves
 *     dispersion over the cluster.
 * </li><li>
 *     The Source vertex accesses the Hazelcast map and sends its contents as
 *     {@code (key, line)} pairs. On each cluster member the source sees only
 *     the entries stored on that member.
 * </li><li>
 *     Line tuples are sent over a <em>local</em> edge to the Generator vertex.
 *     This means that the tuples stay within the member where they were created
 *     and are routed to locally-running Generator instances.
 * </li><li>
 *     Generator splits each line into words and emits tuples of the form
 *     {@code (word, 1)}.
 * </li><li>
 *     Tuples are sent over a <em>partitioned local</em> edge which routes
 *     all the tuples with the same word to the same local instance of Accumulator.
 * </li><li>
 *     Accumulator collates tuples by word and maintains the count of each seen
 *     word. After having received all the input from the Generator, it emits
 *     tuples of the form {@code (word, localCount)}.
 * </li><li>
 *     Now the tuples are sent to the Combiner vertex over a <em>distributed
 *     partitioned</em> edge. This means that for each word there will be a single
 *     unique instance of Combiner in the whole cluster and tuples will be sent
 *     over the network if needed.
 * </li><li>
 *     Combiner sums up the partial results obtained by local Accumulators and
 *     outputs the total word counts.
 * </li><li>
 *     Finally, the Sink vertex stores the result in the output Hazelcast map,
 *     named {@code counts}.
 * </li></ul>
 */
public class WordCount {

    private static final ILogger LOGGER = Logger.getLogger(WordCount.class);
    private static final String[] BOOKS = new String[]{
            "dracula.txt", "pride_and_prejudice.txt", "ulysses.txt", "war_and_peace.txt", "a_tale_of_two_cities.txt"};

    public static void main(String[] args) throws Exception {
        JetInstance instance1 = Jet.newJetInstance();
        JetInstance instance2 = Jet.newJetInstance();

        IMap<String, String> lines = instance1.getMap("lines");

        LOGGER.info("Populating map...");

        for (String book : BOOKS) {
            populateMap(lines, book);
        }
        LOGGER.info("Populated map with " + lines.size() + " lines.");


        DAG dag = new DAG();
        Vertex input = new Vertex("input", Processors.mapReader("lines"));
        Vertex generator = new Vertex("generator", Generator::new);
        Vertex accumulator = new Vertex("accumulator", Combiner::new);
        Vertex combiner = new Vertex("combiner", Combiner::new);
        Vertex output = new Vertex("output", Processors.mapWriter("counts"));

        Distributed.Function<Entry<String, Long>, String> getWord = Entry<String, Long>::getKey;
        dag.vertex(input)
           .vertex(generator)
           .vertex(accumulator)
           .vertex(combiner)
           .vertex(output)
           .edge(between(input, generator))
           .edge(between(generator, accumulator).partitionedByKey(getWord))
           .edge(between(accumulator, combiner).distributed().partitionedByKey(getWord))
           .edge(between(combiner, output));

        LOGGER.info("Executing...");
        instance1.newJob(dag).execute().get();
        Set<Entry<String, Long>> counts = instance1.<String, Long>getMap("counts").entrySet();
        List<Entry<String, Long>> sorted = counts.stream()
                                                 .sorted(comparingLong(Entry::getValue))
                                                 .collect(Collectors.toList());
        System.out.println("Counts=" + sorted);
        Jet.shutdownAll();
    }

    private static class Generator extends AbstractProcessor {

        private static final Pattern PATTERN = Pattern.compile("\\w+");

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            String text = ((Entry<Integer, String>) item).getValue().toLowerCase();
            Matcher m = PATTERN.matcher(text);
            while (m.find()) {
                emit(new SimpleImmutableEntry<>(m.group(), 1L));
            }
            return true;
        }
    }

    private static class Combiner extends AbstractProcessor {
        private Map<String, Long> counts = new HashMap<>();
        private Supplier<Entry<String, Long>> cacheEntrySupplier = lazyIterate(() -> counts.entrySet().iterator());

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            Map.Entry<String, Long> entry = (Map.Entry<String, Long>) item;
            counts.compute(entry.getKey(), (k, v) -> v == null ? entry.getValue() : v + entry.getValue());
            return true;
        }

        @Override
        public boolean complete() {
            final boolean done = emitCooperatively(cacheEntrySupplier);
            if (done) {
                counts = null;
                cacheEntrySupplier = null;
            }
            return done;
        }
    }

    private static Stream<String> lineStream(String path) throws URISyntaxException, IOException {
        URL resource = WordCount.class.getResource(path);
        return Files.lines(Paths.get(resource.toURI()));
    }

    private static void populateMap(Map<String, String> map, String path) throws IOException, URISyntaxException {
        Map<String, String> lines = lineStream(path)
                .map(l -> new SimpleImmutableEntry<>(newUnsecureUuidString(), l))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        map.putAll(lines);
    }
}
