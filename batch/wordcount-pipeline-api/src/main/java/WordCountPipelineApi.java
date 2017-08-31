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
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

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

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.lang.Runtime.getRuntime;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparingLong;

/**
 */
public class WordCountPipelineApi {

    private static final String DOCID_NAME = "docId_name";
    private static final String COUNTS = "counts";

    private JetInstance jet;

    private static Pipeline buildPipeline() {
        Pattern delimiter = Pattern.compile("\\W+");
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Long, String>readMap(DOCID_NAME))
         .flatMap(e -> traverseStream(docLines(e.getValue()))).nonCooperative()
         .flatMap(line -> traverseArray(delimiter.split(line.toLowerCase())))
         .filter(word -> !word.isEmpty())
         .groupBy(wholeItem(), counting())
         .drainTo(Sinks.writeMap(COUNTS));
        return p;
    }

    public static void main(String[] args) throws Exception {
        new WordCountPipelineApi().go();
    }

    private void go() throws Exception {
        try {
            setup();
            System.out.print("\nCounting words... ");
            long start = System.nanoTime();
            buildPipeline().execute(jet).get();
            System.out.print("done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
            printResults();
            IMap<String, Long> counts = jet.getMap(COUNTS);
            if (counts.get("the") != 951_129) {
                throw new AssertionError("Wrong count of 'the'");
            }
            System.out.println("Count of 'the' is valid");
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

    private static Stream<String> docFilenames() {
        final ClassLoader cl = WordCountPipelineApi.class.getClassLoader();
        final BufferedReader r = new BufferedReader(new InputStreamReader(cl.getResourceAsStream("books"), UTF_8));
        return r.lines().onClose(() -> close(r));
    }

    private static Stream<String> docLines(String name) {
        try {
            return Files.lines(Paths.get(WordCountPipelineApi.class.getResource("books/" + name).toURI()));
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
}
