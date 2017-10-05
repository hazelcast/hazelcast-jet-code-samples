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

import com.hazelcast.core.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedStream;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.ProcessorMetaSupplier.of;

/**
 * Demonstrates how to use custom processor as a source
 */
public class CustomSource {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        try {
            JetInstance instance = Jet.newJetInstance();
            Jet.newJetInstance();

            ProcessorMetaSupplier metaSupplier = of(new DummySupplier());
            IList<String> sink = DistributedStream
                    .<String>fromSource(instance, metaSupplier)
                    .flatMap(line -> Arrays.stream(line.split(" ")))
                    .collect(DistributedCollectors.toIList("sink"));

            sink.forEach(System.out::println);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static class DummySupplier implements ProcessorSupplier {
        @Nonnull
        @Override
        public Collection<? extends Processor> get(int count) {
            return IntStream.range(0, count).mapToObj(i -> {
                if (i == 0) {
                    return new DummySource();
                }
                return Processors.noopP().get();
            }).collect(Collectors.toList());
        }
    }

    private static class DummySource extends AbstractProcessor {

        @Override
        public boolean complete() {
            return tryEmit("Hello World!");
        }
    }
}
