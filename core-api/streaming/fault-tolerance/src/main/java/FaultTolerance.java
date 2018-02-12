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

import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.config.ProcessingGuarantee;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.SlidingWindowPolicy;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.function.DistributedFunction;
import com.hazelcast.jet.function.DistributedToLongFunction;
import com.hazelcast.jet.stream.IStreamMap;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_CURRENT;
import static com.hazelcast.jet.Util.mapPutEvents;
import static com.hazelcast.jet.aggregate.AggregateOperations.averagingDouble;
import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.SlidingWindowPolicy.slidingWinPolicy;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.limitingLag;
import static com.hazelcast.jet.core.processor.DiagnosticProcessors.writeLoggerP;
import static com.hazelcast.jet.core.processor.Processors.aggregateToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static java.util.Collections.singletonList;

/**
 * A simple application which uses Jet with the event journal reader for
 * {@code IMap} to perform rolling average calculations and
 * illustrates the differences in processing guarantees.
 * <p>
 * A price updater thread keeps updating the current price of each stock
 * and writes the new value, along with a timestamp into a distributed
 * {@code IMap}.
 * <p>
 * A price analyzer DAG consumes events from this map, and performs a
 * rolling average calculation per ticker using the given window sizes. The
 * output is written to stdout via the logger sink.
 * <p>
 * Initially, there are two nodes in the cluster. After a given delay one
 * of the nodes will be shut down and the job automatically restarted.
 * The output after restart should be observed to see what happens when
 * different kinds of {@link ProcessingGuarantee}s are given.
 */
public class FaultTolerance {

    private static final String PRICES_MAP_NAME = "prices";
    private static final String[] TICKER_LIST = {"AAPL", "AMZN", "EBAY", "GOOG", "MSFT", "TSLA"};

    private static final long LAG_SECONDS = 10;
    private static final long WINDOW_SIZE_SECONDS = 10;
    private static final long SHUTDOWN_DELAY_SECONDS = LAG_SECONDS * 2;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        JobConfig config = new JobConfig();


        ////////////////////////////////////////////////////////////////////////////////
        // Configure this option to observe the output with different processing
        // guarantees
        //
        // Here we will lose data after shutting down the second node. You will see
        // a gap in the sequence number.
        config.setProcessingGuarantee(ProcessingGuarantee.NONE);
        //
        // Here we will not lose data after shutting down the second node but you might
        // see duplicates
        // config.setProcessingGuarantee(ProcessingGuarantee.AT_LEAST_ONCE);
        //
        // Here we will not lose data after shutting down the second node and you will
        // not see duplicates.
        // config.setProcessingGuarantee(ProcessingGuarantee.EXACTLY_ONCE);
        //
        ////////////////////////////////////////////////////////////////////////////////

        //default is 10 seconds, we are using 5
        config.setSnapshotIntervalMillis(5000);

        // create two instances
        JetInstance instance1 = createNode();
        JetInstance instance2 = createNode();

        // create a client and submit the price analyzer DAG
        JetInstance client = Jet.newJetClient();
        Job job = client.newJob(buildDAG(), config);

        Thread.sleep(1000);

        System.out.println("******************************************");
        System.out.println("Starting price updater. You should start seeing the output after "
                + LAG_SECONDS + " seconds");
        System.out.println("After " + SHUTDOWN_DELAY_SECONDS + " seconds, one of the nodes will be shut down.");
        System.out.println("******************************************");
        // start price updater thread to start generating events
        new Thread(() -> updatePrices(instance1)).start();

        Thread.sleep(SHUTDOWN_DELAY_SECONDS * 1000);
        instance2.shutdown();

        System.out.println("Member shut down, the job will now restart and you can inspect the output again.");

    }

    private static JetInstance createNode() {
        JetConfig config = new JetConfig();

        // configure event journal and backups for the prices map
        EventJournalConfig journalConfig = new EventJournalConfig()
                .setMapName(PRICES_MAP_NAME)
                .setCapacity(100_000)
                .setEnabled(true);

        MapConfig mapConfig = new MapConfig(PRICES_MAP_NAME)
                .setBackupCount(1);
        config.getHazelcastConfig().addMapConfig(mapConfig);
        config.getHazelcastConfig().addEventJournalConfig(journalConfig);
        return Jet.newJetInstance(config);
    }

    private static DAG buildDAG() {
        SlidingWindowPolicy winPolicy = slidingWinPolicy(WINDOW_SIZE_SECONDS, 1);

        DAG dag = new DAG();
        Vertex streamMap = dag.newVertex("stream-map",
                SourceProcessors.<PriceUpdateEvent, String, Tuple2<Integer, Long>>streamMapP(
                        "prices",
                        mapPutEvents(),
                        e -> new PriceUpdateEvent(e.getKey(), e.getNewValue().f0(), e.getNewValue().f1()),
                        START_FROM_CURRENT,
                        wmGenParams(
                                PriceUpdateEvent::timestamp,
                                limitingLag(LAG_SECONDS),
                                emitByFrame(winPolicy),
                                1000L
                        )
                )
        ).localParallelism(1);

        Vertex insertWm = dag.newVertex("insert-wm",
                insertWatermarksP(wmGenParams(
                        PriceUpdateEvent::timestamp, limitingLag(LAG_SECONDS), emitByFrame(winPolicy), 30000L)));

        Vertex slidingWindow = dag.newVertex("sliding-window",
                aggregateToSlidingWindowP(
                        singletonList((DistributedFunction<PriceUpdateEvent, String>) PriceUpdateEvent::ticker),
                        singletonList((DistributedToLongFunction<PriceUpdateEvent>) PriceUpdateEvent::timestamp),
                        TimestampKind.EVENT,
                        winPolicy,
                        averagingDouble(PriceUpdateEvent::price),
                        TimestampedEntry::new)
        );

        Vertex fileSink = dag.newVertex("logger",
                writeLoggerP(FaultTolerance::formatOutput)).localParallelism(1);

        dag.edge(between(streamMap, slidingWindow).distributed().partitioned(PriceUpdateEvent::ticker))
           .edge(between(slidingWindow, fileSink));
        return dag;
    }

    private static void updatePrices(JetInstance jet) {
        IStreamMap<String, Tuple2<Integer, Long>> prices = jet.getMap(PRICES_MAP_NAME);
        int price = 100;
        long timestamp = 0;
        while (true) {
            for (String ticker : TICKER_LIST) {
                prices.put(ticker, tuple2(price, timestamp));
            }
            price++;
            timestamp++;
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    private static String formatOutput(TimestampedEntry entry) {
        return String.format("%d,%s,%s", entry.getTimestamp(), entry.getKey(), entry.getValue());
    }
}
