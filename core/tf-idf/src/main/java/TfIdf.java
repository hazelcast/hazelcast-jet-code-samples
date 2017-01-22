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
import com.hazelcast.jet.Partitioner;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.Processors.accumulate;
import static com.hazelcast.jet.Processors.flatMap;
import static com.hazelcast.jet.Processors.groupAndAccumulate;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.Processors.mapReader;
import static com.hazelcast.jet.Processors.mapWriter;
import static com.hazelcast.jet.Traversers.enumerate;
import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseIterable;
import static com.hazelcast.jet.Traversers.traverseStream;
import static java.lang.Runtime.getRuntime;

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
 *  --------                       -----------                    -----------
 * | source | -(docId, docName)-> | doc-lines | -(docId, line)-> | tokenizer |
 *  --------                       -----------                    -----------
 *      |                                                              /
 *      |                                                /--(docId, word)
 *      |                                                V
 *      |                                           ----------
 *      |                                          | tf-local | -------------\
 * (docId, word)                                    ----------                |
 *      |                                                        ((docId, word), count)
 *       \           -----------                                              |
 *         -------> | doc-count |                                             V
 *                   -----------           ----                             ----
 *                        |               | df | <-((docId, word), count)- | tf |
 *                     (count)             ----                             ----
 *                        v                /                                 /
 *                      -----     (word, docCount)                          /
 *            -------- | idf | <--------/                                  /
 *          /          -----                                             /
 *  (word, true)          |                                              /
 *        |         (word, idf)   ----------((docId, word), count)-----
 *        |               |     /
 *        V               V    V
 * ---------------       --------                                      ---------------------
 *| stopword-sink |     | tf-idf | -(word, list(docId, tfidf-score)-> | inverted-index-sink |
 * ---------------       --------                                      ---------------------
 * </pre>
 * This is how the DAG works:
 * <ul><li>
 *     In the {@code resources} directory there are some books in plain text
 *     format.
 * </li><li>
 *     Each book is assigned an ID and a Hazelcast distributed map is built that
 *     maps from document ID to document name. This is the DAG's source.
 * </li><li>
 *     The {@code source} vertex emits {@code (docId, docName)} pairs. On each
 *     cluster member this vertex observes only the map entries stored locally
 *     on that member. Therefore each member sees a unique subset of all the
 *     documents.
 * </li><li>
 *     {@code doc-count} is a simple vertex that counts the number of all documents.
 *     It receives {@code source}'s output over a <em>distributed broadcast</em>
 *     edge and has a <em>local parallelism</em> of one. This means that there is
 *     one processor for this vertex on each member, and each processor observes
 *     all the items coming out of the {@code source}.
 * </li><li>
 *     The {@code doc-lines} vertex reads each document and emits its lines of
 *     text as {@code (docId, line)} pairs. This is an example where a
 *     <em>non-cooperative</em> processor makes sense because it does file I/O.
 *     For the same reason, the vertex has a local parallelism of 1 because there
 *     is nothing to gain from doing file I/O in parallel.
 * </li><li>
 *     The {@code tokenizer} vertex tokenizes the lines and emits
 *     {@code (docId, word)} pairs. The mapping logic of this vertex could have
 *     been a part of {@code doc-lines}, but there is exploitable parallelism
 *     where {@code doc-lines} blocks on I/O operation while this vertex's
 *     processors keep churning the lines already read. The output is sent
 *     over a <em>local partitioned</em> edge so all the pairs involving the
 *     same word go to the same {@code tf-local} processor instance. The edge
 *     can be local because TF is calculated within the context of a single
 *     document.
 * </li><li>
 *     The {@code tf-local} vertex counts for each {@code (docId, word)} pair
 *     the number of times it occurs. The result is the TF score of each pair.
 * </li><li>
 *     {@code tf-local} sends its results to {@code tf} over a <em>distributed
 *     partitioned</em> edge with {@code word} being the partitionig key. This
 *     achieves the localization of each word, as required for the downstream
 *     logic. Every word is assigned its unique processor instance in the whole
 *     cluster: this processor observes all TF entries related to it.
 * </li><li>
 *     The output of {@code tf} can now be sent over a local partitioned edge
 *     to {@code df}, which will count for each word the number of distinct
 *     {@code docId}s present in the TF entries. The output of {@code tf} is
 *     also sent directly to {@code tf-idf}.
 * </li><li>
 *     {@code df} feeds into {@code idf} along with {@code doc-count}.
 *     {@code idf} applies the IDF function to {@code D} received from
 *     {@code d} and {@code DF} received from {@code df}.
 * </li><li>
 *     {@code idf} splits its output into two streams: words with
 *     {@code IDF > 0} are routed towards the {@code tfidf} vertex in
 *     {@code (word, idf)} pairs; words with zero IDF (the <em>stopwords</em>)
 *     are routed directly to the sink that stores them in the
 *     "{@value #STOPWORDS}" map.
 * </li><li>
 *     {@code tfidf} receives the output from {@code tf} and {@code idf}
 *     over partitioned local edges. It builds the final product of this DAG:
 *     the TF-IDF index of all words in all documents. The index is keyed by
 *     word and the value is a list of {@code docId, tfidfScore} pairs sorted
 *     descending by score. The {@code tfidf} vertex produces all the entries
 *     for this index and the actual map insertion is done by the final
 *     {@code sink} vertex. The map's name is "{@value #INVERTED_INDEX }".
 * </li><li>
 *     Note the special settings on the {@code tf -> tfidf} edge: there is
 *     a dependency chain {@code tf -> df -> idf -> tfidf} as well as the
 *     direct dependency {@code tf -> tfidf}. {@code tfidf} is implemented
 *     such that it first consumes all the {@code idf} output, then starts
 *     receiving {@code tf} and immediately producing the final results. These
 *     facts taken together mean that {@code tf} must output all its data
 *     before {@code tfidf} starts consuming it; therefore the whole output
 *     must be buffered on the way from {@code tf} to {@code tfidf}.
 * </li></ul>
 * When the inverted index is built, this program opens a minimalist GUI window
 * which can be used to perform searches and review the results.
 */
public class TfIdf {

    private static final String[] BOOK_NAMES = {
            "a_tale_of_two_cities",
            "anna_karenina",
            "awakening",
            "clarissa_harlowe",
            "crime_punishment",
            "divine_comedy",
            "dorian_gray",
            "dracula",
            "emma",
            "frankenstein",
            "great_expectations",
            "huckleberry_finn",
            "pride_and_prejudice",
            "shakespeare_complete_works",
            "siddharta",
            "swanns_way",
            "ulysses",
            "war_and_peace",
            "wuthering_heights",
    };
    private static final String DOCID_NAME = "docId_name";
    private static final String INVERTED_INDEX = "inverted-index";
    private static final String STOPWORDS = "stopwords";

    private JetInstance jet;

    public static void main(String[] args) throws Throwable {
        try {
            new TfIdf().go();
        } catch (Throwable t) {
            Jet.shutdownAll();
            throw t;
        }
    }

    private void go() throws Throwable {
        setup();
        createIndex();
        new SearchGui(jet.getMap(DOCID_NAME), jet.getMap(INVERTED_INDEX), jet.getMap(STOPWORDS));
    }

    private void setup() {
        JetConfig cfg = new JetConfig();
        cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                Math.max(1, getRuntime().availableProcessors() / 2)));
        System.out.println("Forming Jet cluster...");
        jet = Jet.newJetInstance(cfg);
        Jet.newJetInstance(cfg);
        final IMap<Long, String> docId2Name = jet.getMap(DOCID_NAME);
        for (int i = 0; i < BOOK_NAMES.length; i++) {
            docId2Name.set(i + 1L, BOOK_NAMES[i]);
        }
    }

    private void createIndex() throws Throwable {
        Job job = jet.newJob(createDag());
        System.out.println("Indexing documents...");
        long start = System.nanoTime();
        job.execute().get();
        System.out.println("Done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
    }

    private static DAG createDag() throws Throwable {
        final Partitioner tfTupleByWord = Partitioner.fromInt(
                item -> ((Entry<Entry<?, String>, ?>) item).getKey().getValue().hashCode());
        final Partitioner entryByKey = Partitioner.fromInt(item -> ((Entry) item).getKey().hashCode());
        final Distributed.Supplier<Long> initialZero = () -> 0L;
        final Distributed.BiFunction<Long, Object, Long> counter = (count, x) -> count + 1;

        final DAG dag = new DAG();

        // nil -> (docId, word)
        final Vertex source = dag.newVertex("source", mapReader(DOCID_NAME)).localParallelism(1);
        // item -> count of items
        final Vertex d = dag.newVertex("d", accumulate(initialZero, counter)).localParallelism(1);
        // (docId, docName) -> many (docId, line)
        final Vertex docLines = dag.newVertex("doc-lines", DocLinesP::new).localParallelism(1);
        // (docId, line) -> many (docId, word)
        final Vertex tokenizer = dag.newVertex("tokenize",
                flatMap((Entry<Long, String> e) -> docIdTokenTraverser(e.getKey(), e.getValue())
        ));
        // many (docId, word) -> ((docId, word), count)
        final Vertex tfLocal = dag.newVertex("tf-local", groupAndAccumulate(initialZero, counter));
        final Vertex tf = dag.newVertex("tf", map(Distributed.Function.identity()));
        // many ((docId, word), x) -> (word, docCount)
        final Vertex df = dag.newVertex("df", groupAndAccumulate(
                        (Entry<Entry<?, String>, ?> e) -> e.getKey().getValue(), initialZero, counter));
        // 0: single docCount, 1: (word, docCount) -> (word, idf)
        final Vertex idf = dag.newVertex("idf", IdfP::new);
        // 0: (word, idf), 1: ((docId, word), count) -> (word, list of (docId, tf-idf-score))
        final Vertex tfidf = dag.newVertex("tf-idf", TfIdfP::new);
        final Vertex invertedIndexSink = dag.newVertex("inverted-index-sink", mapWriter(INVERTED_INDEX));
        final Vertex stopwordSink = dag.newVertex("stopword-sink", mapWriter(STOPWORDS)).localParallelism(1);

        return dag
                .edge(between(source, docLines))
                .edge(from(source, 1).to(d).distributed().broadcast())
                .edge(between(docLines, tokenizer))
                .edge(between(tokenizer, tfLocal).partitionedByCustom(Partitioner.fromInt(Object::hashCode)))
                .edge(between(tfLocal, tf).distributed().partitionedByCustom(tfTupleByWord))
                .edge(between(tf, df).partitionedByCustom(tfTupleByWord))
                .edge(between(d, idf).broadcast())
                .edge(from(df).to(idf, 1).priority(1).partitionedByCustom(entryByKey))
                .edge(between(idf, tfidf).partitionedByCustom(entryByKey))
                .edge(from(idf, 1).to(stopwordSink))
                .edge(from(tf, 1).to(tfidf, 1)
                                 .priority(1).buffered()
                                 .partitionedByCustom(tfTupleByWord))
                .edge(between(tfidf, invertedIndexSink));
    }

    private static Traverser<Entry<Long, String>> docIdTokenTraverser(Long docId, String line) {
        final StringTokenizer t = new StringTokenizer(line.toLowerCase(), " \n\r\t,.:;!?-_\"'’<>()&%");
        return enumerate(t).map(token -> new SimpleImmutableEntry<>(docId, (String) token));
    }

    private static class DocLinesP extends AbstractProcessor {
        private FlatMapper<Entry<Long, String>, Entry<Long, String>> flatMapper;

        DocLinesP() {
            flatMapper = flatMapper(e ->
                    traverseStream(bookLines(e.getValue()))
                            .map(line -> new SimpleImmutableEntry<>(e.getKey(), line))
            );
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) {
            return flatMapper.tryProcess((Entry<Long, String>) item);
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        private static Stream<String> bookLines(String name) {
            try {
                return Files.lines(Paths.get(TfIdf.class.getResource(name + ".txt").toURI()));
            } catch (IOException | URISyntaxException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class IdfP extends AbstractProcessor {
        private double totalDocCount;

        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            totalDocCount = (Long) item;
            return true;
        }

        @Override
        protected boolean tryProcess1(@Nonnull Object item) {
            final Entry<String, Long> entry = (Entry<String, Long>) item;
            final String word = entry.getKey();
            final long docCount = entry.getValue();
            final double idf = Math.log(totalDocCount / docCount);
            if (idf > 0) {
                emit(0, new SimpleImmutableEntry<>(word, idf));
            } else {
                emit(1, new SimpleImmutableEntry<>(word, true));
            }
            return true;
        }
    }

    private static class TfIdfP extends AbstractProcessor {
        private Map<String, Double> wordIdf = new HashMap<>();
        private Map<String, List<Entry<Long, Double>>> wordDocScore = new HashMap<>();
        private Traverser<Entry<String, List<Entry<Long, Double>>>> docScoreTraverser =
                lazy(() -> traverseIterable(wordDocScore.entrySet()));


        @Override
        protected boolean tryProcess0(@Nonnull Object item) {
            final Entry<String, Double> e = (Entry<String, Double>) item;
            wordIdf.put(e.getKey(), e.getValue());
            return true;
        }

        @Override
        protected boolean tryProcess1(@Nonnull Object item) {
            final Entry<Entry<Long, String>, Long> e = (Entry<Entry<Long, String>, Long>) item;
            final long docId = e.getKey().getKey();
            final String word = e.getKey().getValue();
            final long tf = e.getValue();
            final Double idf = wordIdf.get(word);
            if (idf != null) {
                wordDocScore.computeIfAbsent(word, w -> new ArrayList<>())
                            .add(new SimpleImmutableEntry<>(docId, tf * idf));
            }
            return true;
        }

        @Override
        public boolean complete() {
            return emitCooperatively(docScoreTraverser);
        }
    }
}
