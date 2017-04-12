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

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;
import com.hazelcast.mapreduce.aggregation.impl.LongSumAggregation;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparingLong;

public class WordCountMR {

    private static long lineId;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.setProperty("hazelcast.logging.type", "log4j");
        final HazelcastInstance hz = Hazelcast.newHazelcastInstance();
        try {
            final JobTracker t = hz.getJobTracker("word-count");
            final IMap<Long, String> lines = hz.getMap("lines");
            System.out.println("Populating map...");
            docFilenames().limit(5).forEach(filename -> populateMap(lines, filename));
            System.out.println("Populated map with " + lines.size() + " lines");

            final LongSumAggregation<String, String> aggr = new LongSumAggregation<>();
            final Map<String, Long> counts =
                    t.newJob(KeyValueSource.fromMap(lines))
                     .mapper((Long x, String line, Context<String, Long> ctx) ->
                             Stream.of(line.toLowerCase().split("\\W+"))
                                   .filter(w -> !w.isEmpty())
                                   .forEach(w -> ctx.emit(w, 1L)))
                     .combiner(aggr.getCombinerFactory())
                     .reducer(aggr.getReducerFactory())
                     .submit()
                     .get();
            printResults(counts);
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    private static Stream<String> docFilenames() {
        final ClassLoader cl = WordCountMR.class.getClassLoader();
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

    private static Stream<String> lineStream(String bookName) {
        try {
            URL resource = WordCountMR.class.getResource("books/" + bookName);
            return Files.lines(Paths.get(resource.toURI()));
        } catch (URISyntaxException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void populateMap(Map<Long, String> map, String docName) {
        final Map<Long, String> lines = lineStream(docName)
                .map(l -> entry(lineId++, l))
                .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
        map.putAll(lines);
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
}


