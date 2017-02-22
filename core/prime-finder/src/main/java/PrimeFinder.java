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
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Processors;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.stream.IStreamList;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeList;
import static com.hazelcast.jet.Traversers.traverseStream;
import static com.hazelcast.jet.stream.DistributedCollectors.toList;
import static java.lang.Runtime.getRuntime;
import static java.util.stream.IntStream.range;

/**
 * A DAG which finds the prime numbers up to a certain number and writes the output to
 * a {@link IStreamList}. A distributed number generator is used to distribute the numbers across
 * the processors. This examples is mostly aimed at illustrating how a custom partitioning
 * at the source can be achieved using the {@link ProcessorMetaSupplier} API.
 *
 * Each processor will emit a subset of the number range, by only emitting the numbers with
 * a specific remainder when divided by the total number of processors across all the nodes.
 *
 * The {@code filter-primes} vertex is a simple filtering processor, which checks the incoming
 * number for primeness, and emits if the number is prime. The results are then written into a
 * Hazelcast list.
 *
 */
public class PrimeFinder {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        try {
            JetConfig cfg = new JetConfig();
            cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                    Math.max(1, getRuntime().availableProcessors() / 2)));

            Jet.newJetInstance(cfg);
            JetInstance jet = Jet.newJetInstance(cfg);

            DAG dag = new DAG();

            final int limit = 15_485_864;
            Vertex generator = dag.newVertex("number-generator", new NumberGeneratorMetaSupplier(limit));
            Vertex primeChecker = dag.newVertex("filter-primes", Processors.filter(PrimeFinder::isPrime));
            Vertex writer = dag.newVertex("writer", writeList("primes"));

            dag.edge(between(generator, primeChecker));
            dag.edge(between(primeChecker, writer));

            jet.newJob(dag).execute().get();

            IStreamList<Object> primes = jet.getList("primes");
            List sortedPrimes = primes.stream().sorted().limit(1000).collect(toList());
            System.out.println("Found " + primes.size() + " primes.");
            System.out.println("Some of the primes found are: " + sortedPrimes);

        } finally {
            Jet.shutdownAll();
        }
    }

    private static boolean isPrime(int n) {
        if (n <= 1) {
            return false;
        }

        for (int i = 2; i <= Math.sqrt(n); i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }

    static class NumberGeneratorMetaSupplier implements ProcessorMetaSupplier {

        private final int limit;

        private transient int totalParallelism;
        private transient int localParallelism;

        NumberGeneratorMetaSupplier(int limit) {
            this.limit = limit;
        }

        @Override
        public void init(@Nonnull Context context) {
            totalParallelism = context.totalParallelism();
            localParallelism = context.localParallelism();
        }


        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            Map<Address, ProcessorSupplier> map = new HashMap<>();
            for (int i = 0; i < addresses.size(); i++) {
                Address address = addresses.get(i);
                int start = i * localParallelism;
                int end = (i + 1) * localParallelism;
                int mod = totalParallelism;
                map.put(address, count -> range(start, end)
                        .mapToObj(index -> new NumberGenerator(range(0, limit).filter(f -> f % mod == index)))
                        .collect(toList())
                );
            }
            return map::get;
        }
    }

    static class NumberGenerator extends AbstractProcessor {

        private final Traverser<Integer> traverser;

        NumberGenerator(IntStream stream) {
            traverser = traverseStream(stream.boxed());
        }

        @Override
        public boolean complete() {
            return emitCooperatively(traverser);
        }
    }
}
