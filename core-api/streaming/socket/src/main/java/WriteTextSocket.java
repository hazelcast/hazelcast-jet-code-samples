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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.SinkProcessors;
import com.hazelcast.jet.processor.SourceProcessors;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.IStreamMap;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
import static com.hazelcast.jet.Edge.between;

/**
 * A DAG which reads from Hazelcast IMap,
 * extracts the value and add a new-line,
 * and writes to a socket
 * <p>
 * The Netty server increments a counter for each message read
 */
public class WriteTextSocket {

    private static final String HOST = "localhost";
    private static final int PORT = 5252;
    private static final int COUNT = 100_000;
    private static final AtomicInteger COUNTER = new AtomicInteger();
    private static final String MAP_NAME = "map";

    public static void main(String[] args) throws Exception {
        NettyServer nettyServer = new NettyServer(PORT, noopConsumer(), msg -> COUNTER.incrementAndGet());
        nettyServer.start();

        JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();

        try {
            System.out.println("Filling Map");
            IStreamMap<Integer, Integer> map = instance.getMap(MAP_NAME);
            IntStream.range(0, COUNT).parallel().forEach(i -> map.put(i, i));

            DAG dag = new DAG();
            Vertex source = dag.newVertex("source", SourceProcessors.readMap(MAP_NAME));
            Vertex mapper = dag.newVertex("map", Processors.map((Map.Entry entry) -> entry.getValue() + "\n"));
            Vertex sink = dag.newVertex("sink", SinkProcessors.writeSocket(HOST, PORT));

            dag.edge(between(source, mapper));
            dag.edge(between(mapper, sink));

            System.out.println("Starting job");
            instance.newJob(dag).join();


        } finally {
            nettyServer.stop();
            Jet.shutdownAll();
        }

        System.out.println("Count: " + COUNTER.get());

    }
}
