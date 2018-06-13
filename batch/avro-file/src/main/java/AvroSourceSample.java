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

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Util;
import com.hazelcast.jet.avro.AvroSources;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import model.User;
import org.apache.avro.reflect.ReflectDatumReader;

import java.nio.file.Paths;

/**
 * Demonstrates reading Apache Avro files from a directory and populating IMap
 * Run {@link AvroSinkSample} first to create necessary Apache Avro files directory.
 */
public class AvroSourceSample {

    private JetInstance jet;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        p.drawFrom(AvroSources.<User, User>filesBuilder(AvroSinkSample.DIRECTORY_NAME, ReflectDatumReader::new)
                //Both Jet members share the same local file system
                .sharedFileSystem(true)
                .build())
         .map(user -> Util.entry(user.getUsername(), user))
         .drainTo(Sinks.map(AvroSinkSample.MAP_NAME));
        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        new AvroSourceSample().go();
    }

    private void go() {
        try {
            setup();
            jet.newJob(buildPipeline()).join();

            IMapJet<String, User> map = jet.getMap(AvroSinkSample.MAP_NAME);
            System.out.println("Map Size: " + map.size());
            map.forEach((key, value) -> System.out.println(key + " - " + value));
        } finally {
            Jet.shutdownAll();
        }
    }

    private void setup() {
        if (!Paths.get(AvroSinkSample.DIRECTORY_NAME).toFile().exists()) {
            System.out.println("Avro files directory does not exist, please run " +
                    AvroSinkSample.class.getSimpleName() + " first to create it.");
            System.exit(0);
        }
        jet = Jet.newJetInstance();
        Jet.newJetInstance();
    }

}
