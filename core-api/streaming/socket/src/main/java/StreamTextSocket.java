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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;

import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.function.DistributedFunctions.noopConsumer;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * A DAG which reads from a socket and writes the lines to a Hazelcast IList
 * <p>
 * The Netty server writes to the connected sockets by decrementing an AtomicInteger till 0.
 */
public class StreamTextSocket {

    private static final String HOST = "localhost";
    private static final int PORT = 5252;
    private static final AtomicInteger COUNTER = new AtomicInteger(100_000);
    private static final String LIST_NAME = "list";

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        NettyServer nettyServer = new NettyServer(PORT, channel -> {
            for (int i; (i = COUNTER.getAndDecrement()) > 0; ) {
                channel.writeAndFlush(i + "\n");
            }
            channel.close();
        }, noopConsumer());
        nettyServer.start();

        JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();

        try {
            DAG dag = new DAG();

            Vertex source = dag.newVertex("source", SourceProcessors.streamSocketP(HOST, PORT, UTF_8));
            Vertex sink = dag.newVertex("sink", SinkProcessors.writeListP(LIST_NAME));

            dag.edge(between(source, sink));

            System.out.println("Starting Job");
            instance.newJob(dag).join();

            System.out.println("Count: " + instance.getList(LIST_NAME).size());
        } finally {
            nettyServer.stop();
            Jet.shutdownAll();
        }

    }
}
