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

/**
 * Demonstrates reading avro files from a directory and populating IMap
 * Run {@link SinkSample} first to create necessary avro files directory
 */
public class SourceSample {

    private JetInstance jet;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        new SourceSample().go();
    }

    private void go() {
        try {
            setup();
            jet.newJob(buildPipeline()).join();

            IMapJet<String, User> map = jet.getMap(SinkSample.MAP_NAME);
            System.out.println("Map Size: " + map.size());
            map.forEach((key, value) -> System.out.println(key + " - " + value));
        } finally {
            Jet.shutdownAll();
        }
    }

    private void setup() {
        jet = Jet.newJetInstance();
        Jet.newJetInstance();
    }

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        p.drawFrom(AvroSources.files(SinkSample.DIRECTORY_NAME, User.class, true))
         .map(user -> Util.entry(user.getUsername(), user))
         .drainTo(Sinks.map(SinkSample.MAP_NAME));
        return p;
    }

}
