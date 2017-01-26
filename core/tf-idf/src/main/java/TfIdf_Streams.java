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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;

public class TfIdf_Streams {

    private static final Pattern DELIMITER = Pattern.compile("\\W+");

    private Set<String> stopwords;
    private Map<String, List<Entry<Long, Double>>> invertedIndex;
    private Map<Long, String> docId2Name;

    public static void main(String[] args) {
        new TfIdf_Streams().go();
    }

    private void go() {
        docId2Name = buildDocumentInventory();
        final long start = System.nanoTime();
        buildInvertedIndex();
        System.out.println("Done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
        new SearchGui(docId2Name, invertedIndex, stopwords);
    }

    private void buildInvertedIndex() {
        final double logDocCount = Math.log(docId2Name.size());

        // Stream of (docId, word)
        final Stream<Entry<Long, String>> docWords = docId2Name
                .entrySet()
                .parallelStream()
                .flatMap(TfIdf_Streams::docLines)
                .flatMap(TfIdf_Streams::tokenize);

        System.out.println("Building TF");
        // TF: (docId, word) -> count
        final Map<Entry<Long, String>, Long> tf = docWords
                .collect(groupingBy(identity(), counting()));

        System.out.println("Building IDF");
        // IDF: word -> idf
        final Map<String, Double> idf = tf
                .keySet()
                .parallelStream()
                .collect(groupingBy(Entry::getValue,
                        collectingAndThen(counting(), count -> logDocCount - Math.log(count))));

        System.out.println("Building stopword set");
        stopwords = idf
                .entrySet()
                .parallelStream()
                .filter(e -> e.getValue() <= 0)
                .map(Entry::getKey)
                .collect(toSet());

        System.out.println("Building inverted index");
        // Inverted index: word -> list of (docId, TF-IDF_score)
        invertedIndex = tf
                .entrySet()
                .parallelStream()
                .filter(e -> idf.get(wordFromTfEntry(e)) > 0)
                .collect(groupingBy(
                        TfIdf_Streams::wordFromTfEntry,
                        collectingAndThen(
                                toList(),
                                entries -> entries.stream().map(e -> tfidfEntry(idf, e)).collect(toList())
                        )
                ));
    }

    private static Map<Long, String> buildDocumentInventory() {
        final ClassLoader cl = TfIdf_Streams.class.getClassLoader();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(cl.getResourceAsStream("books"), UTF_8))) {
            final long[] docId = {0};
            System.out.println("These books will be indexed:");
            return r.lines()
                    .peek(System.out::println)
                    .collect(toMap(x -> ++docId[0], identity()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<Entry<Long, String>> docLines(Entry<Long, String> idAndName) {
        try {
            return Files.lines(Paths.get(TfIdf_Streams.class.getResource("books/" + idAndName.getValue()).toURI()))
                        .map(line -> new SimpleImmutableEntry<>(idAndName.getKey(), line));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<Entry<Long, String>> tokenize(Entry<Long, String> docLine) {
        return Arrays.stream(DELIMITER.split(docLine.getValue()))
                     .map(word -> new SimpleImmutableEntry<>(docLine.getKey(), word));
    }

    private static String wordFromTfEntry(Entry<Entry<Long, String>, Long> tfEntry) {
        return tfEntry.getKey().getValue();
    }

    private static Entry<Long, Double> tfidfEntry(
            Map<String, Double> idf, Entry<Entry<Long, String>, Long> tfEntry
    ) {
        final String word = wordFromTfEntry(tfEntry);
        final Long tf = tfEntry.getValue();
        return new SimpleImmutableEntry<>(tfEntry.getKey().getKey(), tf * idf.get(word));
    }
}
