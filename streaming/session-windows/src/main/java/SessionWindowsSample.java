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

import com.hazelcast.jet.AggregateOperation;
import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.PunctuationPolicies;
import com.hazelcast.jet.Session;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.samples.sessionwindows.ProductEvent;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.hazelcast.jet.AggregateOperations.allOf;
import static com.hazelcast.jet.AggregateOperations.summingToLong;
import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.processor.DiagnosticProcessors.writeLogger;
import static com.hazelcast.jet.processor.Processors.aggregateToSessionWindow;
import static com.hazelcast.jet.processor.Processors.insertPunctuation;
import static com.hazelcast.jet.samples.sessionwindows.ProductEventType.PURCHASE;
import static com.hazelcast.jet.samples.sessionwindows.ProductEventType.VIEW_LISTING;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A sample demonstrating accumulation into session windows. It processes
 * following events:
 * <ul>
 *     <li>view of a product listing</li>
 *     <li>product purchase</li>
 * </ul>
 * Events are aggregated into sessions, which are based on userId. We calculate
 * the number of viewed product listings for the set of purchased product.
 * <p>
 * The DAG is as follows:
 * <pre>
 *          +--------------+
 *          |    Source    |
 *          +------+-------+
 *                 |
 *                 | ProductEvent
 *                 |
 *       +--------------------+
 *       | Insert punctuation |
 *       +--------------------+
 *                 |
 *                 | ProductEvent & punctuations
 *                 |  distributed + partitioned edge
 *     +-----------+-----------+
 *     | Aggregate to sessions |
 *     +-----------+-----------+
 *                 |
 *                 | Sessions
 *                 |
 *           +-----+-----+
 *           |   Sink    |
 *           +-----------+
 * </pre>
 */
public class SessionWindowsSample {

    private static final long JOB_DURATION = 60_000;
    private static final int SESSION_TIMEOUT = 5000;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            jet.newJob(buildDag()).execute();
            Thread.sleep(SECONDS.toMillis(JOB_DURATION));
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag() {
        DAG dag = new DAG();
        // we'll calculate two aggregations over the same input data:
        // 1. number of viewed product listings
        // 2. set of purchased products
        // Output of the aggregation will be List{Integer, Set<String>}
        AggregateOperation<ProductEvent, List<Object>, List<Object>> aggrOp = allOf(
                summingToLong(e -> e.getProductEventType() == VIEW_LISTING ? 1 : 0),
                toSet(e -> e.getProductEventType() == PURCHASE ? e.getProductId() : null)
        );

        // if you want to see events emitted from source, replace "GenerateEventsP::new"
        // with "Processors.peekOutput(GenerateEventsP::new)"
        Vertex source = dag.newVertex("source", GenerateEventsP::new)
                           .localParallelism(1);
        Vertex insertPunc = dag.newVertex("insertPunc", insertPunctuation(ProductEvent::getTimestamp,
                () -> PunctuationPolicies.withFixedLag(100).throttleByMinStep(100)));
        Vertex aggregateSessions = dag.newVertex("aggregateSessions",
                aggregateToSessionWindow(SESSION_TIMEOUT, ProductEvent::getTimestamp, ProductEvent::getUserId, aggrOp));
        Vertex sink = dag.newVertex("sink", writeLogger(SessionWindowsSample::sessionToString))
                .localParallelism(1);

        dag.edge(between(source, insertPunc).oneToMany())
           // This edge needs to be partitioned+distributed. It is not possible to calculate session
           // windows in two-stage fashion.
           .edge(between(insertPunc, aggregateSessions)
                   .partitioned(ProductEvent::getUserId)
                   .distributed())
           .edge(between(aggregateSessions, sink));

        return dag;
    }

    /**
     * Returns an aggregator, aggregating objects to a {@code Set}.
     * @param mapper a function extracting value inserted to the Set from input
     *               items. If it maps to {@code null}, that item is ignored.
     */
    private static <T, U> AggregateOperation<T, ?, Set<U>> toSet(
            @Nonnull DistributedFunction<? super T, ? extends U> mapper) {
        return AggregateOperation.of(
                HashSet<U>::new,
                (set, item) -> {
                    U mapped = mapper.apply(item);
                    if (mapped != null) {
                        set.add(mapped);
                    }
                },
                Set::addAll,
                null,
                identity()
        );
    }

    /**
     * Formatter for output Session
     */
    private static String sessionToString(Session<String, List<Long>> s) {
        return String.format("Session{userId=%s, start=%s, duration=%2ds, value={viewed=%2d, purchases=%s}",
                s.getKey(), // userId
                Instant.ofEpochMilli(s.getStart()).atZone(ZoneId.systemDefault()).toLocalTime(), // session start
                Duration.ofMillis(s.getEnd() - s.getStart()).getSeconds(), // session duration
                s.getResult().get(0),  // number of wiewed listings
                s.getResult().get(1)); // set of purchased products
    }
}
