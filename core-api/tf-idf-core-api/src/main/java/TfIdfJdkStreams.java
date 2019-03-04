/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import support.SearchGui;
import support.TfIdfUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Implementation of TF-IDF without Jet, with just the JDK code.
 */
public class TfIdfJdkStreams {

    private Set<String> stopwords;
    private Map<String, List<Entry<Long, Double>>> invertedIndex;
    private Map<Long, String> docId2Name;

    public static void main(String[] args) {
        new TfIdfJdkStreams().go();
    }

    private void go() {
        stopwords = readStopwords();
        docId2Name = TfIdfUtil.buildDocumentInventory();
        final long start = System.nanoTime();
        buildInvertedIndex();
        System.out.println("Done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
        new SearchGui(docId2Name, invertedIndex, stopwords);
    }

    private void buildInvertedIndex() {
        final double logDocCount = Math.log(docId2Name.size());

        // stream of (docId, word)
        Stream<Entry<Long, String>> docWords = docId2Name
                .entrySet()
                .parallelStream()
                .flatMap(TfIdfUtil::docLines)
                .flatMap(this::tokenize);

        System.out.println("Building TF");
        // TF: (docId, word) -> count
        Map<Entry<Long, String>, Long> tfMap = docWords
                .parallel()
                .collect(groupingBy(identity(), counting()));

        System.out.println("Building inverted index");
        // Inverted index: word -> list of (docId, TF-IDF_score)
        invertedIndex = tfMap
                .entrySet()
                .parallelStream()
                .collect(groupingBy(
                        e -> e.getKey().getValue(),
                        collectingAndThen(
                                toList(),
                                entries -> {
                                    double idf = logDocCount - Math.log(entries.size());
                                    return entries.stream().map(e -> tfidfEntry(e, idf)).collect(toList());
                                }
                        )
                ));
    }

    private static Set<String> readStopwords() {
        try (BufferedReader r = TfIdfUtil.resourceReader("stopwords.txt")) {
            return r.lines().collect(toSet());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Stream<Entry<Long, String>> tokenize(Entry<Long, String> docLine) {
        return Arrays.stream(TfIdfUtil.DELIMITER.split(docLine.getValue()))
                     .filter(token -> !token.isEmpty())
                     .filter(token -> !stopwords.contains(token))
                     .map(word -> entry(docLine.getKey(), word));
    }

    // ((docId, word), count) -> (docId, tfIdf)
    private static Entry<Long, Double> tfidfEntry(Entry<Entry<Long, String>, Long> tfEntry, Double idf) {
        final Long tf = tfEntry.getValue();
        return entry(tfEntry.getKey().getKey(), tf * idf);
    }
}
