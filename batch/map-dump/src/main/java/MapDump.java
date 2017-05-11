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
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.stream.IStreamMap;

import javax.annotation.Nonnull;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.readMap;
import static java.util.stream.Collectors.toList;
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

            IStreamMap<Object, Object> map = jet.getMap("map");
            range(0, COUNT).parallel().forEach(i -> map.put("key-" + i, i));

            DAG dag = new DAG();

            Vertex source = dag.newVertex("map-source", readMap(map.getName()));
            Vertex sink = dag.newVertex("file-sink", new WriteFilePSupplier(OUTPUT_FOLDER));
            dag.edge(between(source, sink));

            jet.newJob(dag).execute().get();
            System.out.println("\nHazelcast IMap dumped to folder " + new File(OUTPUT_FOLDER).getAbsolutePath());
        } finally {
            Jet.shutdownAll();
        }
    }

    static class WriteFilePSupplier implements ProcessorSupplier {

        private final String path;

        private transient List<WriteFileP> writers;

        WriteFilePSupplier(String path) {
            this.path = path;
        }

        @Override
        public void init(@Nonnull Context context) {
            new File(path).mkdirs();
        }

        @Nonnull @Override
        public List<WriteFileP> get(int count) {
            return (writers = range(0, count)
                    .mapToObj(e -> new WriteFileP(path))
                    .collect(toList()));
        }

        @Override
        public void complete(Throwable error) {
            writers.forEach(p -> {
                try {
                    p.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
    }

    static class WriteFileP extends AbstractProcessor implements Closeable {

        private final String path;

        private BufferedWriter writer;

        WriteFileP(String path) {
            this.path = path;
        }

        @Override
        protected void init(@Nonnull Context context) throws Exception {
            Path path = Paths.get(this.path, context.jetInstance().getName() + '-' + context.globalProcessorIndex());
            writer = Files.newBufferedWriter(path);
        }

        @Override
        protected boolean tryProcess(int ordinal, @Nonnull Object item) throws IOException {
            writer.append(item.toString());
            writer.newLine();
            return true;
        }

        @Override
        public boolean isCooperative() {
            return false;
        }

        @Override
        public void close() throws IOException {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
