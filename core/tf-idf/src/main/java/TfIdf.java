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
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;

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
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.DistributedFunctions.wholeItem;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.accumulate;
import static com.hazelcast.jet.Processors.flatMap;
import static com.hazelcast.jet.Processors.groupAndAccumulate;
import static com.hazelcast.jet.Processors.nonCooperative;
import static com.hazelcast.jet.Processors.readMap;
import static com.hazelcast.jet.Processors.writeMap;
import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.Util.entry;
import static java.lang.Runtime.getRuntime;
import static java.nio.charset.StandardCharsets.UTF_8;
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
 *     In the {@code books} module there are some books in plain text format.
 *     Each book is assigned an ID and a Hazelcast distributed map is prepared
 *     that maps from document ID to document name. This is the DAG's source.
 * </li><li>
 *     {@code doc-source} emits {@code (docId, docName)} pairs. On each cluster
 *     member this vertex observes only the map entries stored locally on that
 *     member. Therefore each member sees a unique subset of all the documents.
 * </li><li>
 *     The {@code books} module also contains the file {@code stopwords.txt}
 *     with one stopword per line. The {@code stopword-source} vertex reads
 *     it and builds a set of stopwords. It emits the set as a single item.
 *     This works well because it is a small set of a few hundred entries.
 * </li><li>
 *     Tuples are sent over a <em>distributed broadcast</em> edge to {@code
 *     doc-count}, which has a local parallelism of <em>one</em>. This means
 *     that there is one processor for {@code doc-count} on each member, and
 *     each processor observes all the items coming out of {@code doc-source}.
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
 *     The output of {@code tokenize} is sent over a <em>local partitioned</em>
 *     edge so all the pairs involving the same word and document go to the
 *     same {@code tf} processor instance. The edge can be local because TF is
 *     calculated within the context of a single document and the reading of
 *     any given document is already localized to a single member.
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
 *     this index and actual map insertion is done by the final {@code sink}
 *     vertex. The map's name is "{@value #INVERTED_INDEX}".
 * </li></ul>
 * When the inverted index is built, this program opens a minimalist GUI window
 * which can be used to perform searches and review the results.
 */
public class TfIdf {

    private static final Pattern DELIMITER = Pattern.compile("\\W+");
    private static final String DOCID_NAME = "docId_name";
    private static final String INVERTED_INDEX = "inverted-index";

    private JetInstance jet;

    public static void main(String[] args) throws Throwable {
        System.setProperty("hazelcast.logging.type", "log4j");
        try {
            new TfIdf().go();
        } catch (Throwable t) {
            Jet.shutdownAll();
            throw t;
        }
    }

    private void go() throws Throwable {
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

    private void buildInvertedIndex() throws Throwable {
        Job job = jet.newJob(createDag());
        long start = System.nanoTime();
        job.execute().get();
        System.out.println("Indexing took " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
    }

    private static DAG createDag() throws Throwable {
        final Distributed.Function<Entry<Entry<?, String>, ?>, String> byWord = item -> item.getKey().getValue();
        final Distributed.Supplier<Long> initialZero = () -> 0L;
        final Distributed.BiFunction<Long, Object, Long> counter = (count, x) -> count + 1;

        final DAG dag = new DAG();

        // nil -> Set<String> stopwords
        final Vertex stopwordSource = dag.newVertex("stopword-source", StopwordsP::new);
        // nil -> (docId, docName)
        final Vertex docSource = dag.newVertex("doc-source", readMap(DOCID_NAME));
        // item -> count of items
        final Vertex docCount = dag.newVertex("doc-count", accumulate(initialZero, counter));
        // (docId, docName) -> many (docId, line)
        final Vertex docLines = dag.newVertex("doc-lines", nonCooperative(flatMap((Entry<Long, String> e) ->
                traverseStream(docLines("books/" + e.getValue()).map(line -> entry(e.getKey(), line))))));
        // 0: stopword set, 1: (docId, line) -> many (docId, word)
        final Vertex tokenize = dag.newVertex("tokenize", TokenizeP::new);
        // many (docId, word) -> ((docId, word), count)
        final Vertex tf = dag.newVertex("tf", groupAndAccumulate(initialZero, counter));
        // 0: doc-count, 1: ((docId, word), count) -> (word, list of (docId, tf-idf-score))
        final Vertex tfidf = dag.newVertex("tf-idf", TfIdfP::new);
        final Vertex sink = dag.newVertex("sink", writeMap(INVERTED_INDEX));

        return dag
                .edge(between(stopwordSource.localParallelism(1), tokenize).broadcast().priority(-1))
                .edge(between(docSource.localParallelism(1), docCount.localParallelism(1)).distributed().broadcast())
                .edge(from(docSource, 1).to(docLines.localParallelism(1)))
                .edge(from(docLines).to(tokenize, 1))
                .edge(between(tokenize, tf).partitioned(wholeItem(), HASH_CODE))
                .edge(between(docCount, tfidf).broadcast().priority(-1))
                .edge(from(tf).to(tfidf, 1).distributed().partitioned(byWord, HASH_CODE))
                .edge(between(tfidf, sink));
    }

    private void buildDocumentInventory() {
        final ClassLoader cl = TfIdf.class.getClassLoader();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(cl.getResourceAsStream("books"), UTF_8))) {
            final IMap<Long, String> docId2Name = jet.getMap(DOCID_NAME);
            final long[] docId = {0};
            r.lines().peek(System.out::println).forEach(fname -> docId2Name.put(++docId[0], fname));
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

    private static class StopwordsP extends AbstractProcessor {
        @Override
        public boolean complete() {
            return tryEmit(docLines("stopwords.txt").collect(toSet()));
        }
    }

    private static class TokenizeP extends AbstractProcessor {
        private Set<String> stopwords;
        private final FlatMapper<Entry<Long, String>, Entry<Long, String>> flatMapper = flatMapper(e ->
                traverseStream(Arrays.stream(DELIMITER.split(e.getValue()))
                                     .filter(word -> !stopwords.contains(word))
                                     .map(word -> entry(e.getKey(), word))));

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            stopwords = (Set<String>) item;
            return true;
        }

        @Override
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
        protected boolean tryProcess0(@Nonnull Object item) throws Exception {
            logDocCount = Math.log((Long) item);
            return true;
        }

        @Override
        protected boolean tryProcess1(@Nonnull Object item) throws Exception {
            final Entry<Entry<Long, String>, Long> e = (Entry<Entry<Long, String>, Long>) item;
            final long docId = e.getKey().getKey();
            final String word = e.getKey().getValue();
            final long tf = e.getValue();
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
            final String word = wordDocTf.getKey();
            final List<Entry<Long, Double>> docidTfs = wordDocTf.getValue();
            return entry(word, docScores(docidTfs));
        }

        private List<Entry<Long, Double>> docScores(List<Entry<Long, Double>> docidTfs) {
            final double logDf = Math.log(docidTfs.size());
            return docidTfs.stream()
                           .map(tfe -> tfidfEntry(logDf, tfe))
                           .collect(toList());
        }

        private Entry<Long, Double> tfidfEntry(double logDf, Entry<Long, Double> docidTf) {
            final Long docId = docidTf.getKey();
            final double tf = docidTf.getValue();
            final double idf = logDocCount - logDf;
            return entry(docId, tf * idf);
        }
    }
}

