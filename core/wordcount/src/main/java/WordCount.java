/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;

/**
 * A distributed word count can be implemented with three vertices as follows:
 * -------------              ---------------                         ------------
 * | Generator |-(word, 1)--> | Accumulator | -(word, localCount)--> | Combiner  | --(word, globalCount) ->
 * -------------              ---------------                         ------------
 * <p>
 * first vertex will be split the words in each paragraph and emit tuples as (WORD, 1)
 * second vertex will combine the counts locally on each node
 * third vertex will combine the counts across all nodes.
 * <p>
 * The edge between generator and accumulator is local, but partitioned, so that all words with same hash go
 * to the same instance of the processor on the same node.
 * <p>
 * The edge between the accumulator and combiner vertex is both shuffled and partitioned, meaning all words
 * with same hash are processed by the same instance of the processor across all nodes.
 */
public class WordCount {

    private static final ILogger LOGGER = Logger.getLogger(WordCount.class);
    private static final String[] BOOKS = new String[]{"dracula.txt", "pride_and_prejudice.txt", "ulysses.txt",
            "war_and_peace.txt", "a_tale_of_two_cities.txt"};

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

        dag.addVertex(input)
           .addVertex(generator)
           .addVertex(accumulator)
           .addVertex(combiner)
           .addVertex(output)
           .addEdge(new Edge(input, generator))
           .addEdge(new Edge(generator, accumulator)
                   .partitionedByKey(item -> ((Entry) item).getKey()))
           .addEdge(new Edge(accumulator, combiner)
                   .distributed()
                   .partitionedByKey(item -> ((Entry) item).getKey()))
           .addEdge(new Edge(combiner, output));

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
        public boolean process(int ordinal, Object item) {
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
        private Iterator<Entry<String, Long>> iterator;

        @Override
        public boolean process(int ordinal, Object item) {
            Map.Entry<String, Long> entry = (Map.Entry<String, Long>) item;
            counts.compute(entry.getKey(), (k, v) -> v == null ? entry.getValue() : v + entry.getValue());
            return true;
        }

        @Override
        public boolean complete() {
            if (iterator == null) {
                iterator = counts.entrySet().iterator();
            }

            while (iterator.hasNext() && !getOutbox().isHighWater()) {
                emit(iterator.next());
            }
            return !iterator.hasNext();
        }
    }

    private static List<String> readText(String path) throws URISyntaxException, IOException {
        URL resource = WordCount.class.getResource(path);
        return Files.readAllLines(Paths.get(resource.toURI()));
    }

    private static void populateMap(Map<String, String> map, String path) throws IOException, URISyntaxException {
        Map<String, String> lines = readText(path)
                .stream()
                .map(l -> new SimpleImmutableEntry<>(UuidUtil.newUnsecureUuidString(), l))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        map.putAll(lines);
    }
}
