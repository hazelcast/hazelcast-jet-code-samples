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

package map;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.stream.IStreamMap;

import java.util.Map.Entry;

import static com.hazelcast.jet.core.Edge.between;

/**
 * A DAG which reads from a Hazelcast IMap,
 * converts the key/value to string,
 * and writes to another Hazelcast IMap
 */
public class ReadWriteMap {

    private static final int ITEM_COUNT = 10;
    private static final String SOURCE_MAP_NAME = "sourceMap";
    private static final String SINK_MAP_NAME = "sinkMap";

    public static void main(String[] args) throws Exception {
        JetInstance instance = Jet.newJetInstance();

        try {
            IStreamMap<Integer, Integer> sourceMap = instance.getMap(SOURCE_MAP_NAME);
            for (int i = 0; i < ITEM_COUNT; i++) {
                sourceMap.put(i, i);
            }

            DAG dag = new DAG();

            Vertex source = dag.newVertex("source", SourceProcessors.readMapP(SOURCE_MAP_NAME));
            Vertex transform = dag.newVertex("transform", Processors.mapP((Entry<Integer, Integer> e)
                    -> Util.entry(e.getKey().toString(), e.getValue().toString())));
            Vertex sink = dag.newVertex("sink", SinkProcessors.writeMapP(SINK_MAP_NAME));

            dag.edge(between(source, transform));
            dag.edge(between(transform, sink));

            instance.newJob(dag).join();

            IStreamMap<String, String> sinkMap = instance.getMap(SINK_MAP_NAME);
            System.out.println("Sink map size: " + sinkMap.size());
            System.out.println("Sink map entries: ");
            for (int i = 0; i < ITEM_COUNT; i++) {
                System.out.println(sinkMap.get(Integer.toString(i)));
            }
        } finally {
            Jet.shutdownAll();
        }

    }
}
