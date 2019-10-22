/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.pipeline.ServiceFactory;
import com.hazelcast.map.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.function.FunctionEx;
import support.SearchGui;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.core.Partitioner.HASH_CODE;
import static com.hazelcast.jet.core.processor.Processors.aggregateByKeyP;
import static com.hazelcast.jet.core.processor.Processors.flatMapUsingServiceP;
import static com.hazelcast.jet.core.processor.SourceProcessors.readMapP;
import static com.hazelcast.function.Functions.wholeItem;
import static java.lang.Runtime.getRuntime;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

/**
 * Builds, for a given set of text documents, an <em>inverted index</em> that
 * maps each word to the set of documents that contain it. Each document in
 * the set is assigned a TF-IDF score which tells how relevant the document
 * is to the search term. In short,
 * <ul><li>
 *     {@code TF(document, word)} is <em>term frequency</em>: the number of
 *     occurrences of a given word in a given document. {@code TF} is expected
 *     to correlate with the relevance of the word to the document.
 * </li><li>
 *     Let {@code DF(word)} be the <em>document frequency</em> of a word: the
 *     number of documents a given word occurs in.
 * </li><li>
 *     {@code IDF(word)} is the <em>inverse document frequency</em> of a word:
 *     {@code log(D/DF)} where {@code D} is the overall number of documents.
 *     IDF is expected to correlate with the salience of the word: a high value
 *     means it's highly specific to the documents it occurs in. For example,
 *     words like "in" and "the" have an IDF of zero because they occur
 *     everywhere.
 * </li><li>
 *     {@code TF-IDF(document, word)} is the product of {@code TF * IDF} for a
 *     given word in a given document.
 * </li><li>
 *     A word that occurs in all documents has an IDF score of zero, therefore
 *     its TF-IDF score is also zero for any document. Such words are called
 *     <em>stopwords</em> and can be eliminated both from the inverted index and
 *     from the search phrase as an optimization.
 * </li></ul>
 * When the user enters a search phrase, first the stopwords are crossed out,
 * then each remaining search term is looked up in the inverted index, resulting
 * in a set of documents for each search term. An intersection is taken of all
 * these sets, which gives us only the documents that contain all the search
 * terms. For each combination of document and search term there will be an
 * associated TF-IDF score. These scores are summed per document to retrieve
 * the total score of each document. The list of documents sorted by score
 * (descending) is presented to the user as the search result.
 * <p>
 * This is the DAG used to build the index:
 * <pre>
 *             ------------              -----------------
 *            | doc-source |            | stopword-source |
 *             ------------              -----------------
 *             /           \                     |
 *            /       (docId, docName)           |
 *           /                \                  |
 *          /                  V         (set-of-stopwords)
 *  (docId, docName)         -----------         |
 *         |                | doc-lines |        |
 *         |                 -----------         |
 *         |                     |               |
 *         |                (docId, line)        |
 *    -----------                |               |
 *   | doc-count |               V               |
 *    -----------            ----------          |
 *         |                | tokenize | <------/
 *         |                 ----------
 *         |                     |
 *      (count)            (docId, word)
 *         |                     |
 *         |                     V
 *         |                   ----
 *         |                  | tf |
 *         |                   ----
 *         |                     |
 *         |           ((docId, word), count)
 *         |                     |
 *         |      --------       |
 *          \--> | tf-idf | <---/
 *                --------
 *                   |
 *    (word, list(docId, tfidf-score))
 *                   |
 *                   V
 *                ------
 *               | sink |
 *                ------
 * </pre>
 * This is how the DAG works:
 * <ul><li>
 *     In the {@code sample-data} module there are some books in plain text
 *     format. We assign an ID to each book and prepare a Hazelcast distributed
 *     map that maps from document ID to document name. This is the DAG's
 *     source.
 * </li><li>
 *     {@code doc-source} emits {@code (docId, docName)} pairs. On each cluster
 *     member this vertex observes only the map entries stored locally on that
 *     member. Therefore each member sees a unique subset of all the documents.
 * </li><li>
 *     The {@code sample-data} module also contains the file {@code
 *     stopwords.txt} with one stopword per line. The {@code stopword-source}
 *     vertex reads it and builds a set of stopwords. It emits the set as a
 *     single item over a high-priority broadcast edge. This way all the
 *     processors of the {@code tokenize} vertex use the same instance of the
 *     set.
 * </li><li>
 *     {@code doc-source} also sends its output over a <em>distributed broadcast</em>
 *     edge to {@code doc-count}, which has a local parallelism of one. This
 *     means that there is one processor for {@code doc-count} on each member,
 *     and each processor observes all the items coming out of {@code
 *     doc-source}.
 * </li><li>
 *     {@code doc-count} is a simple vertex that counts the number of tuples
 *     it has received. Given the properties of its inbound edge, on each
 *     member its processor will emit the total document count.
 * </li><li>
 *     {@code doc-lines} reads each document and emits its lines of text as
 *     {@code (docId, line)} pairs. This is an example where a <em>
 *     non-cooperative</em> processor makes sense because it does file I/O. For
 *     the same reason, the vertex has a local parallelism of 1 because there
 *     is nothing to gain from doing file I/O in parallel.
 * </li><li>
 *     {@code tokenize} receives the stopword set and then starts tokenizing
 *     the lines received from {@code doc-lines}. It emits {@code (docId, word)}
 *     pairs. The mapping logic of this vertex could have been a part of {@code
 *     doc-lines}, but there is exploitable parallelism where {@code doc-lines}
 *     blocks on I/O operation while this vertex's processors keep churning the
 *     lines already read.
 * </li><li>
 *     {@code tokenize} sends its output over a <em>local partitioned</em> edge
 *     so all the pairs involving the same word and document go to the same
 *     {@code tf} processor instance. The edge can be local because TF is a
 *     value calculated within the context of a single document and the reading
 *     of any given document is already localized to a single member.
 * </li><li>
 *     {@code tf} sends its results to {@code tf-idf} over a <em>distributed
 *     partitioned</em> edge with {@code word} being the partitioning key. This
 *     achieves localization by word: every word is assigned its unique
 *     processor instance in the whole cluster so this processor will observe
 *     all TF entries related to the word.
 * </li><li>
 *     {@code doc-count} sends its count item over a local broadcast edge to
 *     {@code tf-idf} so all the parallel {@code tf-idf} instances get the
 *     document count.
 * </li><li>
 *     {@code tf-idf} builds the final product of this DAG: the inverted index
 *     of all words in all documents. The value in the index is a list of
 *     {@code (docId, tfidfScore)} pairs. {@code tf-idf} emits the entries for
 *     this index and the final {@code sink} vertex inserts them into the map.
 *     The map's name is "{@value #INVERTED_INDEX}".
 * </li></ul>
 * After using Jet to build the inverted index, this program opens a
 * minimalist GUI window which you can use to perform searches and review
 * the results.
 */
public class TfIdfCoreApi {

    private static final Pattern DELIMITER = Pattern.compile("\\W+");
    private static final String DOCID_NAME = "docId_name";
    private static final String INVERTED_INDEX = "inverted-index";

    private JetInstance jet;

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        try {
            new TfIdfCoreApi().go();
        } catch (Throwable t) {
            Jet.shutdownAll();
            throw t;
        }
    }

    private void go() {
        setup();
        buildInvertedIndex();
        new SearchGui(jet.getMap(DOCID_NAME), jet.getMap(INVERTED_INDEX), docLines("stopwords.txt").collect(toSet()));
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
        Job job = jet.newJob(createDag());
        long start = System.nanoTime();
        job.join();
        System.out.println("Indexing took " + NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
    }

    private static DAG createDag() {
        FunctionEx<Entry<Entry<?, String>, ?>, String> byWord = item -> item.getKey().getValue();

        DAG dag = new DAG();

        // nil -> Set<String> stopwords
        Vertex stopwordSource = dag.newVertex("stopword-source", StopwordsP::new);
        // nil -> (docId, docName)
        Vertex docSource = dag.newVertex("doc-source", readMapP(DOCID_NAME));
        // item -> count of items
        Vertex docCount = dag.newVertex("doc-count", Processors.aggregateP(counting()));
        // (docId, docName) -> many (docId, line)
        Vertex docLines = dag.newVertex("doc-lines",
                // we use flatMapUsingServiceP for the sake of being able to mark it as non-cooperative
                flatMapUsingServiceP(
                        ServiceFactory.withCreateFn(jet -> null).toNonCooperative(),
                        (Object ctx, Entry<Long, String> e) ->
                                traverseStream(docLines("books/" + e.getValue())
                                        .map(line -> entry(e.getKey(), line)))));
        // 0: stopword set, 1: (docId, line) -> many (docId, word)
        Vertex tokenize = dag.newVertex("tokenize", TokenizeP::new);
        // many (docId, word) -> ((docId, word), count)
        Vertex tf = dag.newVertex("tf", aggregateByKeyP(singletonList(wholeItem()), counting(), Util::entry));
        // 0: doc-count, 1: ((docId, word), count) -> (word, list of (docId, tf-idf-score))
        Vertex tfidf = dag.newVertex("tf-idf", TfIdfP::new);
        Vertex sink = dag.newVertex("sink", SinkProcessors.writeMapP(INVERTED_INDEX));

        stopwordSource.localParallelism(1);
        docSource.localParallelism(1);
        docCount.localParallelism(1);
        docLines.localParallelism(1);

        return dag
                .edge(between(stopwordSource, tokenize).broadcast().priority(-1))
                .edge(between(docSource, docCount).distributed().broadcast())
                .edge(from(docSource, 1).to(docLines))
                .edge(from(docLines).to(tokenize, 1))
                .edge(between(tokenize, tf).partitioned(wholeItem(), HASH_CODE))
                .edge(between(docCount, tfidf).broadcast().priority(-1))
                .edge(from(tf).to(tfidf, 1).distributed().partitioned(byWord, HASH_CODE))
                .edge(between(tfidf, sink));
    }

    private void buildDocumentInventory() {
        ClassLoader cl = TfIdfCoreApi.class.getClassLoader();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(cl.getResourceAsStream("books"), UTF_8))) {
            IMap<Long, String> docId2Name = jet.getMap(DOCID_NAME);
            long[] docId = {0};
            r.lines().peek(System.out::println).forEach(fname -> docId2Name.put(++docId[0], fname));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Stream<String> docLines(String name) {
        try {
            return Files.lines(Paths.get(TfIdfCoreApi.class.getResource(name).toURI()));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    private static class StopwordsP extends AbstractProcessor {
        @Override
        public boolean complete() {
            return tryEmit(docLines("stopwords.txt").collect(toSet()));
        }
    }

    private static class TokenizeP extends AbstractProcessor {
        private Set<String> stopwords;
        private final FlatMapper<Entry<Long, String>, Entry<Long, String>> flatMapper = flatMapper(e ->
                traverseStream(Arrays.stream(DELIMITER.split(e.getValue().toLowerCase()))
                                     .filter(word -> !stopwords.contains(word))
                                     .map(word -> entry(e.getKey(), word))));

        @Override
        @SuppressWarnings("unchecked")
        protected boolean tryProcess0(@Nonnull Object item) {
            stopwords = (Set<String>) item;
            return true;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean tryProcess1(@Nonnull Object item) {
            return flatMapper.tryProcess((Entry<Long, String>) item);
        }
    }

    private static class TfIdfP extends AbstractProcessor {
        private double logDocCount;

        private final Map<String, List<Entry<Long, Double>>> wordDocTf = new HashMap<>();
        private final Traverser<Entry<String, List<Entry<Long, Double>>>> invertedIndexTraverser =
                lazy(() -> traverseIterable(wordDocTf.entrySet()).map(this::toInvertedIndexEntry));

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            logDocCount = Math.log((Long) item);
            return true;
        }

        @Override
        @SuppressWarnings("unchecked")
        protected boolean tryProcess1(@Nonnull Object item) {
            Entry<Entry<Long, String>, Long> e = (Entry<Entry<Long, String>, Long>) item;
            long docId = e.getKey().getKey();
            String word = e.getKey().getValue();
            long tf = e.getValue();
            wordDocTf.computeIfAbsent(word, w -> new ArrayList<>())
                     .add(entry(docId, (double) tf));
            return true;
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(invertedIndexTraverser);
        }

        private Entry<String, List<Entry<Long, Double>>> toInvertedIndexEntry(
                Entry<String, List<Entry<Long, Double>>> wordDocTf
        ) {
            String word = wordDocTf.getKey();
            List<Entry<Long, Double>> docidTfs = wordDocTf.getValue();
            return entry(word, docScores(docidTfs));
        }

        private List<Entry<Long, Double>> docScores(List<Entry<Long, Double>> docidTfs) {
            double logDf = Math.log(docidTfs.size());
            return docidTfs.stream()
                           .map(tfe -> tfidfEntry(logDf, tfe))
                           .collect(toList());
        }

        private Entry<Long, Double> tfidfEntry(double logDf, Entry<Long, Double> docidTf) {
            Long docId = docidTf.getKey();
            double tf = docidTf.getValue();
            double idf = logDocCount - logDf;
            return entry(docId, tf * idf);
        }
    }
}
