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

import com.hazelcast.core.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.pipeline.BatchSource;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedStream;

import java.util.Arrays;

import static com.hazelcast.jet.pipeline.Sources.batchFromProcessor;

/**
 * Demonstrates how to use custom processor as a source
 */
public class CustomSource {

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        try {
            JetInstance instance = Jet.newJetInstance();
            Jet.newJetInstance();

            BatchSource<String> source = batchFromProcessor(
                    "hello-world", ProcessorMetaSupplier.of(HelloWorldP::new)
            );
            IList<String> sink = DistributedStream
                    .fromSource(instance, source, false)
                    .flatMap(line -> Arrays.stream(line.split(" ")))
                    .collect(DistributedCollectors.toIList("sink"));

            sink.forEach(System.out::println);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static class HelloWorldP extends AbstractProcessor {

        @Override
        public boolean complete() {
            return tryEmit("Hello World!");
        }
    }
}
