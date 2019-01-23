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
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.pipeline.WindowDefinition;
import model.ProductEvent;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Set;

import static com.hazelcast.jet.aggregate.AggregateOperations.allOf;
import static com.hazelcast.jet.aggregate.AggregateOperations.mapping;
import static com.hazelcast.jet.aggregate.AggregateOperations.summingLong;
import static com.hazelcast.jet.aggregate.AggregateOperations.toSet;
import static model.ProductEventType.PURCHASE;
import static model.ProductEventType.VIEW_LISTING;

/**
 * Demonstrates the use of {@link WindowDefinition#session session windows}
 * to track the behavior of the users of an online shop. Two kinds of events
 * are processed:
 * <ol><li>
 *     opening of a product listing page;
 * </li><li>
 *     purchase of a product.
 * </li></ol>
 * A user is identified by a {@code userId} and the time span of one user
 * session is inferred from the spread between adjacent events by the same
 * user. Any period without further events from the same user longer than
 * the session timeout ends the session window and causes its results to be
 * emitted. The aggregated results of a session consist of two items: the
 * total number of product listing views and the set of purchased items.
 */
public class SessionWindow {

    private static final long JOB_DURATION_MS = 60_000;
    private static final int SESSION_TIMEOUT = 5_000;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();
        Jet.newJetInstance();
        try {
            jet.newJob(buildPipeline());
            Thread.sleep(JOB_DURATION_MS);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Pipeline buildPipeline() {
        // we'll calculate two aggregations over the same input data:
        // 1. number of viewed product listings
        // 2. set of purchased product IDs
        // Output of the aggregation will be List{Integer, Set<String>}
        AggregateOperation1<ProductEvent, ?, Tuple2<Long, Set<String>>> aggrOp = allOf(
                summingLong(e -> e.getProductEventType() == VIEW_LISTING ? 1 : 0),
                mapping(e -> e.getProductEventType() == PURCHASE ? e.getProductId() : null, toSet())
        );

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<ProductEvent>streamFromProcessor("generator",
                ProcessorMetaSupplier.of(GenerateEventsP::new, 1)))
         .withTimestamps(ProductEvent::getTimestamp, 0)
         .groupingKey(ProductEvent::getUserId)
         .window(WindowDefinition.session(SESSION_TIMEOUT))
         .aggregate(aggrOp, SessionWindow::sessionToString)
         .drainTo(Sinks.logger());
        return p;
    }

    @Nonnull
    private static String sessionToString(long start, long end, String key, Tuple2<Long, Set<String>> value) {
        return String.format("Session{userId=%s, start=%s, duration=%2ds, value={viewed=%2d, purchases=%s}",
                key, // userId
                Instant.ofEpochMilli(start).atZone(ZoneId.systemDefault()).toLocalTime(), // session start
                Duration.ofMillis(end - start).getSeconds(), // session duration
                value.f0(),  // number of viewed listings
                value.f1()); // set of purchased products
    }
}
