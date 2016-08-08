/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package taxiride;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.sink.MapSink;
import com.hazelcast.jet.dag.source.FileSource;
import com.hazelcast.jet.job.Job;
import com.hazelcast.jet.processor.ProcessorDescriptor;
import com.hazelcast.jet.strategy.ProcessingStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.util.concurrent.Future;

/**
 * A taxi ride processing application, which will calculate the average speeds for taxi rides in NYC, based on a
 * stream of taxi ride start and finish events.
 * <p>
 * The DAG consists of three vertices:
 * <p>
 * -------------                    ----------                         ----------------------
 * | Generator |-(taxiRideEvent)--> | Filter | - (taxiRideEvent) -->   | Average Calculator | --(rideId, averageSpeed) ->
 * -------------                    ----------                         ----------------------
 * <p>
 * First vertex will parse the events from the input file and emit TaxiRideEvent instances
 * Second vertex will filter events by location, and emit only the events that are within NYC boundaries
 * Third vertex will match ride start and ride end events to calculate the average speed of the ride.
 * <p>
 * The edge between Filter and Average Calculator vertices will be partitioned by rideId, to ensure that all rides
 * with same ID go to the same instance of the processor for the vertex.
 */
public class TaxiRide {

    private static final ILogger LOGGER = Logger.getLogger(TaxiRide.class);

    public static void main(String[] args) throws Exception {
        HazelcastInstance instance = Hazelcast.newHazelcastInstance();
        IMap<Long, Float> averageSpeeds = instance.getMap("average-speeds");

        DAG dag = new DAG("ride-processor");

        int numTasks = Runtime.getRuntime().availableProcessors();

        Vertex parser = new Vertex("generator", ProcessorDescriptor.builder(TaxiRideGenerator.class)
                .withTaskCount(numTasks)
                .build());
        parser.addSource(new FileSource(getFilePath()));
        dag.addVertex(parser);

        Vertex filter = new Vertex("filter", ProcessorDescriptor.builder(TaxiRideFilter.class)
                .withTaskCount(numTasks)
                .build());
        dag.addVertex(filter);

        Vertex calculator = new Vertex("calculator", ProcessorDescriptor.builder(TaxiRideAverageCalculator.class)
                .withTaskCount(numTasks)
                .build());
        dag.addVertex(calculator);
        calculator.addSink(new MapSink(averageSpeeds));


        Edge parserToFilter = new Edge.EdgeBuilder("generator-filter", parser, filter).build();
        dag.addEdge(parserToFilter);

        // add this edge with partitioning, to ensure that the rides with same ID always end up in the same
        // processor instance.
        Edge filterToAverage = new Edge.EdgeBuilder("filter-average", filter, calculator)
                .processingStrategy(ProcessingStrategy.PARTITIONING)
                .partitioningStrategy(new PartitioningStrategy<TaxiRideEvent>() {
                    @Override
                    public Object getPartitionKey(TaxiRideEvent ride) {
                        return ride.rideId;
                    }
                })
                .build();
        dag.addEdge(filterToAverage);

        Job job = JetEngine.getJob(instance, "taxi-ride", dag);

        try {
            Future executionFuture = job.execute();
            executionFuture.get();
            LOGGER.info("Average Speeds=" + averageSpeeds.entrySet());
        } finally {
            job.destroy();
            Hazelcast.shutdownAll();
        }
    }

    private static String getFilePath() {
        return TaxiRide.class.getClassLoader().getResource("taxiride/smallStream").getFile();
    }


}
