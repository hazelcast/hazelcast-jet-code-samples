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

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparingLong;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingLong;

/**
 * Measures the performance of a basic JDK parallel stream that performs the
 * word count computation on the input from the {@code books} module.
 * <p>
 * It can be observed that the JDK implementation will be unable to parallelize
 * this computation and the main reason is that the spliterator returned from
 * {@link BufferedReader#lines()} has unknown size, which foils JDK's input
 * splitting strategy. If {@code docFilenames()} is replaced with {@code
 * docFilenames().collect(toList()).stream()}, the underlying spliterator will
 * be able to report its size and the JDK will then successfully parallelize
 * the job.
 */
public class WordCountJdk {

    public static void main(String[] args) throws Exception {
        // Warmup
        measure();
        measure();
        measure();
        List<Long> timings = new ArrayList<>();
        for (int i = 0; i < 9; i++) {
            timings.add(measure());
            System.gc();
        }
        System.out.println(timings.stream().collect(summarizingLong(x -> x)));
    }

    private static long measure() {
        final Pattern delimiter = Pattern.compile("\\W+");
        System.out.print("\nCounting words... ");
        long start = System.nanoTime();
        Map<String, Long> counts = docFilenames()
//                .collect(toList()).stream()
                .parallel()
                .flatMap(WordCountJdk::bookLines)
                .flatMap(line -> Arrays.stream(delimiter.split(line.toLowerCase())))
                .filter(w -> !w.isEmpty())
                .collect(groupingBy(identity(), counting()));
        final long took = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        System.out.print("done in " + took + " milliseconds.");
        printResults(counts);
        return took;
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

    private static Stream<String> docFilenames() {
        final ClassLoader cl = WordCountJdk.class.getClassLoader();
        final BufferedReader r = new BufferedReader(new InputStreamReader(cl.getResourceAsStream("books"), UTF_8));
        return r.lines().onClose(() -> close(r));
    }

    private static Stream<String> bookLines(String name) {
        try {
            return Files.lines(Paths.get(WordCountJdk.class.getResource("books/" + name).toURI()));
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
