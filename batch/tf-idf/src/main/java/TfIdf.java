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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.JoinClause;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.processor.Processors.flatMapP;
import static com.hazelcast.jet.core.processor.Processors.nonCooperativeP;
import static com.hazelcast.jet.function.DistributedFunctions.constantKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static java.lang.Runtime.getRuntime;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * TODO
 */
public class TfIdf {

    private static final Pattern DELIMITER = Pattern.compile("\\W+");
    private static final String DOC_ID_NAME = "docId_name";
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
        new SearchGui(jet.getMap(DOC_ID_NAME), jet.getMap(INVERTED_INDEX), docLines("stopwords.txt").collect(toSet()));
    }

    private void setup() {
        JetConfig cfg = new JetConfig();
        cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                Math.max(1, getRuntime().availableProcessors() / 2)));
        System.out.println("Creating Jet instance 1");
        jet = Jet.newJetInstance(cfg);
        System.out.println("Creating Jet instance 2");
        Jet.newJetInstance(cfg);
        System.out.println("These books will be indexed:");
        buildDocumentInventory();
    }

    private void buildInvertedIndex() {
        Job job = jet.newJob(createPipeline());
        long start = System.nanoTime();
        job.join();
        System.out.println("Indexing took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
    }

    private static Pipeline createPipeline() {
        Set<String> stopwords = docLines("stopwords.txt").collect(toSet());

        Pipeline p = Pipeline.create();

        BatchStage<Entry<Long, String>> docSource = p.drawFrom(Sources.map(DOC_ID_NAME));

        BatchStage<Double> logDocCount =
                docSource.aggregate(counting())
                         .map(Math::log)
                         .setName("map-log-count");

        BatchStage<Entry<String, Map<Long, Long>>> tf = docSource.
                <Entry<Long, String>>customTransform(
                        "read-books",
                        nonCooperativeP(flatMapP((Entry<Long, String> e) -> traverseStream(docLines("books/" + e.getValue())
                                .flatMap(DELIMITER::splitAsStream)
                                .map(String::toLowerCase)
                                .filter(word -> !stopwords.contains(word))
                                .map(word -> entry(e.getKey(), word)))
                        )))
                .setLocalParallelism(2)
                .groupingKey(entryValue()) // entry value is the word
                .aggregate(AggregateOperations.toMap(entryKey(), e -> 1L, Long::sum))
                .setName("docId&termFrequency-by-word-aggregate");

        tf.hashJoin(
                logDocCount,
                JoinClause.onKeys(constantKey(), constantKey()),
                (tf_val, logDocCount_val) -> toInvertedIndexEntry(logDocCount_val, tf_val.getKey(), tf_val.getValue().entrySet()))
          .drainTo(Sinks.map(INVERTED_INDEX));

        return p;
    }

    private void buildDocumentInventory() {
        ClassLoader cl = TfIdf.class.getClassLoader();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(cl.getResourceAsStream("books"), UTF_8))) {
            IMap<Long, String> docId2Name = jet.getMap(DOC_ID_NAME);
            long[] docId = {0};
            r.lines().peek(System.out::println).forEach(fName -> docId2Name.put(++docId[0], fName));
        } catch (IOException e) {
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

    private static Entry<String, Collection<Entry<Long, Double>>> toInvertedIndexEntry(
            double logDocCount,
            String word,
            Collection<Entry<Long, Long>> docIdTfs
    ) {
        return entry(word, docScores(logDocCount, docIdTfs));
    }

    private static List<Entry<Long, Double>> docScores(double logDocCount, Collection<Entry<Long, Long>> docIdTfs) {
        double logDf = Math.log(docIdTfs.size());
        return docIdTfs.stream()
                       .map(tfe -> tfidfEntry(logDocCount, logDf, tfe))
                       .collect(toList());
    }

    private static Entry<Long, Double> tfidfEntry(double logDocCount, double logDf, Entry<Long, Long> docIdTf) {
        Long docId = docIdTf.getKey();
        double tf = docIdTf.getValue();
        double idf = logDocCount - logDf;
        return entry(docId, tf * idf);
    }
}
