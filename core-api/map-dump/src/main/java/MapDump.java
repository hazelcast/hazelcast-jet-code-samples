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

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.IMapJet;
import refman.WriteFilePSupplier;

import java.io.File;

import static com.hazelcast.jet.core.Edge.between;
import static java.util.stream.IntStream.range;

/**
 * A DAG which does a distributed dump of the contents of a Hazelcast IMap
 * into several files. This example illustrates how a simple distributed
 * sink can be implemented.
 * <p>
 * Each {@code WriteFileP} instance writes to a separate file, identified
 * by the name of the node and the local index of the processor. The data
 * in the map that is read will be distributed across several writer
 * instances, resulting in one output file per {@code WriteFileP} instance.
 */
public class MapDump {

    private static final int COUNT = 10_000;
    private static final String OUTPUT_FOLDER = "map-dump";

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        Jet.newJetInstance();
        JetInstance jet = Jet.newJetInstance();
        try {

            IMapJet<Object, Object> map = jet.getMap("map");
            range(0, COUNT).parallel().forEach(i -> map.put("key-" + i, i));

            DAG dag = new DAG();

            Vertex source = dag.newVertex("map-source", SourceProcessors.readMapP(map.getName()));
            Vertex sink = dag.newVertex("file-sink", new WriteFilePSupplier(OUTPUT_FOLDER));
            dag.edge(between(source, sink));

            jet.newJob(dag).join();
            System.out.println("\nHazelcast IMap dumped to folder " + new File(OUTPUT_FOLDER).getAbsolutePath());
        } finally {
            Jet.shutdownAll();
        }
    }
}
