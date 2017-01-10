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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Partitioner;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.Distributed;
import com.hazelcast.jet.stream.IStreamMap;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.Processors.accumulate;
import static com.hazelcast.jet.Processors.flatMap;
import static com.hazelcast.jet.Processors.groupAndAccumulate;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.Processors.mapReader;
import static com.hazelcast.jet.Processors.mapWriter;
import static com.hazelcast.jet.Traversers.lazy;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.Comparator.comparingDouble;

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
 * </li></ul>
 * When the user enters a search phrase, each term is looked up in the
 * TF-IDF index, resulting in a set of documents for each entered term. The
 * result set is the intersection of all these sets: the documents that contain
 * all the entered terms. For each combination of document and search term there
 * will be an associated TF-IDF score. These scores are summed per document to
 * retrieve the total score of each document. The list of documents sorted by
 * score (descending) is presented to the user as the search result.
 * <p>
 * This is the DAG used to build the index:
 * <pre>
 *  --------                       -----------                    -----------
 * | source | -(docId, docName)-> | doc-lines | -(docId, line)-> | tokenizer | -(docId, word)-\
 *  --------                       -----------                    -----------                  |
 *      |                                                                                      |
 *      |                                   ----                             ----------        /
 *      |                                  | tf | <-((docId, word), count)- | tf-local | <----
 *      |                                   ----                             ----------
 *      |                                   / \
 * (docId, word)                  ((docId, word), count)
 *      |                                 /      \
 *       \           -----------       ----       |
 *         -------> | doc-count |     | df |      |
 *                   -----------       ----       |
 *                        |              |        |
 *                     (count)  (word, docCount)  |
 *                        v              |        |
 *                      -----           /         |
 *                     | idf | <-------           |
 *                      -----                    /
 *                        |         ------------
 *                   (word, idf)  /
 *                        |      /
 *                        v     v
 *                       --------                                      ------
 *                      | tf-idf | -(word, list(docId, tfidf-score)-> | sink |
 *                       --------                                      ------
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
 *     The {@code doc-lines} vertex reads each document and emits each line of
 *     text as a {@code (docId, line)} pair.
 * </li><li>
 *     The {@code tokenizer} vertex tokenizes the line and emits {@code (docId, word)}
 *     pairs. These pairs are sent over a <em>local partitioned</em> edge so all
 *     the pairs involving the same word go to the same {@code tf-local}
 *     processor instance. The edge can be local because TF is calculated within
 *     the context of a single document.
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
 *     {@code tfidf} receives the output from {@code tf} and {@code idf}
 *     over partitioned local edges. It builds the final product of this DAG:
 *     the TF-IDF index of all words in all documents. The index is keyed by
 *     word and the value is a list of {@code docId, tfidfScore} pairs sorted
 *     descending by score. The {@code tfidf} vertex produces all the entries
 *     for this index and the actual map insertion is done by the final
 *     {@code sink} vertex. The map's name is "{@value #WORD_DOCSCORES}".
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
 * When the TF-IDF index is built, this program opens a minimalist GUI window
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
    private static final String WORD_DOCSCORES = "word -> doc_score";
    private static final Pattern TOKENIZE_PATTERN = Pattern.compile("\\W+");

    private JetInstance jet;
    private IStreamMap<Long, String> docId2Name;
    private IStreamMap<String, List<Entry<Long, Double>>> tfidfIndex;

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
        new SearchGui(docId2Name, tfidfIndex);
    }

    private void setup() {
        jet = Jet.newJetInstance();
        Jet.newJetInstance();
        docId2Name = jet.getMap(DOCID_NAME);
        tfidfIndex = jet.getMap(WORD_DOCSCORES);
        for (int i = 0; i < BOOK_NAMES.length; i++) {
            docId2Name.set(i + 1L, BOOK_NAMES[i]);
        }
    }

    private void createIndex() throws Throwable {
        System.out.println("Indexing documents...");
        long start = System.nanoTime();
        jet.newJob(createDag()).execute().get();
        System.out.println("Done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
    }

    private static DAG createDag() throws Throwable {
        final Distributed.Function<Entry<Entry<?, String>, ?>, String> wordFromTfTuple = e -> e.getKey().getValue();
        final Partitioner tfTupleByWord = Partitioner.fromInt(
                item -> ((Entry<Entry<?, String>, ?>)item).getKey().getValue().hashCode());
        final Partitioner entryByKey = Partitioner.fromInt(item -> ((Entry) item).getKey().hashCode());
        final Distributed.BiFunction<Long, Object, Long> counter = (count, item) -> (count != null ? count : 0L) + 1;

        // nil -> (docId, word)
        final Vertex source = new Vertex("source", mapReader(DOCID_NAME));
        // item -> count of items
        final Vertex d = new Vertex("d", accumulate(counter));
        // (docId, docName) -> many (docId, line)
        final Vertex docLines = new Vertex("doc-lines", flatMap(
                (Distributed.Function<Entry<Long, String>, Stream<Entry<Long, String>>>)
                        e -> uncheckCall(() -> bookLines(e.getValue()))
                                .map(line -> new SimpleImmutableEntry<>(e.getKey(), line)))
        );
        // (docId, line) -> many (docId, word)
        final Vertex tokenizer = new Vertex("tokenize", flatMap(
                (Distributed.Function<Entry<Long, String>, Stream<Entry<Long, String>>>)
                e -> docIdTokenStream(e.getKey(), e.getValue())
        ));
        // many (docId, word) -> ((docId, word), count)
        final Vertex tfLocal = new Vertex("tf-local", groupAndAccumulate(counter));
        final Vertex tf = new Vertex("tf", map(Distributed.Function.identity()));
        // many ((docId, word), x) -> (word, docCount)
        final Vertex df = new Vertex("df", groupAndAccumulate(wordFromTfTuple, counter));
        // 0: single docCount, 1: (word, docCount) -> (word, idf)
        final Vertex idf = new Vertex("idf", IdfP::new);
        // 0: (word, idf), 1: ((docId, word), count) -> (word, list of (docId, tf-idf-score))
        final Vertex tfidf = new Vertex("tf-idf", TfIdfP::new);
        final Vertex sink = new Vertex("sink", mapWriter(WORD_DOCSCORES));

        return new DAG()
                .vertex(source.localParallelism(1))
                .vertex(d.localParallelism(1))
                .vertex(docLines)
                .vertex(tokenizer)
                .vertex(tfLocal)
                .vertex(tf)
                .vertex(df)
                .vertex(idf)
                .vertex(tfidf)
                .vertex(sink)

                .edge(between(source, docLines))
                .edge(from(source, 1).to(d).distributed().broadcast())
                .edge(between(docLines, tokenizer))
                .edge(between(tokenizer, tfLocal).partitionedByCustom(Partitioner.fromInt(Object::hashCode)))
                .edge(between(tfLocal, tf).distributed().partitionedByCustom(tfTupleByWord))
                .edge(between(tf, df).partitionedByCustom(tfTupleByWord))
                .edge(between(d, idf).priority(0).broadcast())
                .edge(from(df).to(idf, 1).priority(1).partitionedByCustom(entryByKey))
                .edge(between(idf, tfidf).priority(0).partitionedByCustom(entryByKey))
                .edge(from(tf, 1).to(tfidf, 1)
                                 .priority(1).buffered()
                                 .partitionedByCustom(tfTupleByWord))
                .edge(between(tfidf, sink));
    }

    private static Stream<String> bookLines(String name) throws IOException, URISyntaxException {
        return Files.lines(Paths.get(TfIdf.class.getResource(name + ".txt").toURI()));
    }

    private static Stream<Entry<Long, String>> docIdTokenStream(Long docId, String line) {
        return Arrays.stream(TOKENIZE_PATTERN.split(line.toLowerCase()))
                     .filter(token -> !token.isEmpty())
                     .map(token -> new SimpleImmutableEntry<>(docId, token));
    }

    // Useful diagnostic method
    private static <T> T print(T t) {
        System.out.println(t);
        return t;
    }


    private static class IdfP extends AbstractProcessor {
        private Double totalDocCount;

        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            if (ordinal == 0) {
                totalDocCount = (double) (Long) item;
                return true;
            }
            assert totalDocCount != null : "Failed to obtain the total doc count in time";
            final Entry<String, Long> entry = (Entry<String, Long>) item;
            final String word = entry.getKey();
            final long docCount = entry.getValue();
            emit(new SimpleImmutableEntry<>(word, Math.log(totalDocCount / docCount)));
            return true;
        }
    }

    private static class TfIdfP extends AbstractProcessor {
        private Map<String, Double> wordIdf = new HashMap<>();
        private Map<String, List<Entry<Long, Double>>> wordDocScore = new HashMap<>();
        private Traverser<Entry<String, List<Entry<Long, Double>>>> docScoreTraverser =
                lazy(() -> traverseStream(wordDocScore
                        .entrySet().stream()
                        .peek(e -> e.getValue().sort(comparingDouble(Entry<Long, Double>::getValue).reversed()))
                ));


        @Override
        protected boolean tryProcess(int ordinal, Object item) {
            if (ordinal == 0) {
                final Entry<String, Double> e = (Entry<String, Double>) item;
                wordIdf.put(e.getKey(), e.getValue());
                return true;
            }
            assert ordinal == 1;
            final Entry<Entry<Long, String>, Long> e = (Entry<Entry<Long, String>, Long>) item;
            final long docId = e.getKey().getKey();
            final String word = e.getKey().getValue();
            final long tf = e.getValue();
            final double idf = wordIdf.get(word);
            wordDocScore.computeIfAbsent(word, w -> new ArrayList<>()).add(
                    new SimpleImmutableEntry<>(docId, tf * idf));
            return true;
        }

        @Override
        public boolean complete() {
            return emitCooperatively(docScoreTraverser);
        }
    }
}
