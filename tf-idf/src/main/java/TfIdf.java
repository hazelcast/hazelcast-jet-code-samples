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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.function.DistributedFunctions.constantKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * TODO
 */
public class TfIdf {

    private static final Pattern DELIMITER = Pattern.compile("\\W+");
    private static final String INVERTED_INDEX = "inverted-index";

    private JetInstance jet;

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        try {
            new TfIdf().go();
        } catch (Throwable t) {
            Jet.shutdownAll();
            throw t;
        }
    }

    private void go() {
        setup();
        buildInvertedIndex();
        System.out.println("size=" + jet.getMap(INVERTED_INDEX).size());
        new SearchGui(jet.getMap(INVERTED_INDEX), docLines("stopwords.txt").collect(toSet()));
    }

    private void setup() {
        JetConfig cfg = new JetConfig();
        System.out.println("Creating Jet instance 1");
        jet = Jet.newJetInstance(cfg);
    }

    private void buildInvertedIndex() {
        Job job = jet.newJob(createPipeline());
        long start = System.nanoTime();
        job.join();
        System.out.println("Indexing took " + NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
    }

    private static Pipeline createPipeline() {
        Path bookDirectory = getClasspathDirectory("books");
        Set<String> stopwords = docLines("stopwords.txt").collect(toSet());
        Pipeline p = Pipeline.create();

        BatchStage<Entry<String, String>> booksSource = p.drawFrom(
                Sources.<Entry<String, String>>filesBuilder(bookDirectory.toString())
                        .build(Util::entry));

        BatchStage<Double> logDocCount = booksSource
                .map(entryKey())  // extract file name
                .aggregate(AggregateOperations.toSet())  // set of unique file names
                .map(Set::size)  // extract size of the set
                .map(Math::log);  // calculate logarithm of it

        BatchStage<Entry<String, Map<String, Long>>> tf = booksSource
                .flatMap(entry ->
                        // split the line to words, convert to lower case, filter out stopwords
                        // and emit as entry(fileName, word)
                        traverseArray(DELIMITER.split(entry.getValue()))
                                .map(word -> {
                                    word = word.toLowerCase();
                                    return stopwords.contains(word) ? null : entry(entry.getKey(), word);
                                }))
                .addKey(entryValue()) // entry value is the word
                .aggregate(AggregateOperations.toMap(entryKey(), e -> 1L, Long::sum));

        tf.hashJoin(
                logDocCount,
                JoinClause.onKeys(constantKey(), constantKey()),
                (tfVal, logDocCountVal) -> toInvertedIndexEntry(
                        logDocCountVal, tfVal.getKey(), tfVal.getValue().entrySet()))
          .drainTo(Sinks.map(INVERTED_INDEX));

        return p;
    }

    private static Path getClasspathDirectory(String name) {
        try {
            return Paths.get(TfIdf.class.getResource(name).toURI());
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<String> docLines(String name) {
        try {
            return Files.lines(Paths.get(TfIdf.class.getResource(name).toURI()));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static Entry<String, Collection<Entry<String, Double>>> toInvertedIndexEntry(
            double logDocCount,
            String word,
            Collection<Entry<String, Long>> docIdTfs
    ) {
        return entry(word, docScores(logDocCount, docIdTfs));
    }

    private static List<Entry<String, Double>> docScores(double logDocCount, Collection<Entry<String, Long>> docIdTfs) {
        double logDf = Math.log(docIdTfs.size());
        return docIdTfs.stream()
                       .map(tfe -> tfidfEntry(logDocCount, logDf, tfe))
                       .collect(toList());
    }

    private static Entry<String, Double> tfidfEntry(double logDocCount, double logDf, Entry<String, Long> docIdTf) {
        String docId = docIdTf.getKey();
        double tf = docIdTf.getValue();
        double idf = logDocCount - logDf;
        return entry(docId, tf * idf);
    }
}
