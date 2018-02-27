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

import com.hazelcast.jet.IListJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SinkProcessors;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedStream;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.jet.core.Edge.between;
import static java.lang.Runtime.getRuntime;

/**
 * A DAG which finds the prime numbers up to a certain number and writes
 * the output to a {@link IListJet}. A distributed number generator is
 * used to distribute the numbers across the processors. This examples is
 * mostly aimed at illustrating how a custom partitioning at the source can
 * be achieved using the {@link ProcessorMetaSupplier} API.
 * <p>
 * Each processor will emit a subset of the number range, by only emitting
 * the numbers with a specific remainder when divided by the total number of
 * processors across all the nodes.
 * <p>
 * The {@code filter-primes} vertex is a simple filtering processor, which
 * checks the incoming number for primeness, and emits if the number is prime.
 * The results are then written into a Hazelcast list.
 *
 */
public class PrimeFinder {

    public static void main(String[] args) {
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
            Vertex primeChecker = dag.newVertex("filter-primes", Processors.filterP(PrimeFinder::isPrime));
            Vertex writer = dag.newVertex("writer", SinkProcessors.writeListP("primes"));

            dag.edge(between(generator, primeChecker));
            dag.edge(between(primeChecker, writer));

            jet.newJob(dag).join();

            IListJet<Integer> primes = jet.getList("primes");
            System.out.println("Found " + primes.size() + " primes.");
            List sortedPrimes = DistributedStream.fromList(primes).filter(i -> i < 1000).collect(DistributedCollectors.toList());
            System.out.println("Some of the primes found are: " + sortedPrimes);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static boolean isPrime(int n) {
        if (n <= 1) {
            return false;
        }

        int endValue = (int) Math.sqrt(n);
        for (int i = 2; i <= endValue; i++) {
            if (n % i == 0) {
                return false;
            }
        }
        return true;
    }

    static class NumberGeneratorMetaSupplier implements ProcessorMetaSupplier {

        private final int limit;
        private int totalParallelism;

        NumberGeneratorMetaSupplier(int limit) {
            this.limit = limit;
        }

        @Override
        public void init(@Nonnull Context context) {
            totalParallelism = context.totalParallelism();
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresses) {
            return address -> count -> IntStream.range(0, count)
                                                .mapToObj(i -> new NumberGenerator(limit, totalParallelism))
                                                .collect(Collectors.toList());
        }
    }

    static class NumberGenerator extends AbstractProcessor {

        private final int limit;
        private final int totalParallelism;
        private Traverser<Integer> traverser;

        NumberGenerator(int limit, int totalParallelism) {
            this.limit = limit;
            this.totalParallelism = totalParallelism;
        }

        @Override
        protected void init(@Nonnull Context context) {
            // Create an ad-hoc traverser that starts with globalProcessorIndex and then
            // increments by totalParallelism, up to the limit.
            traverser = new Traverser<Integer>() {
                int nextValue = context.globalProcessorIndex();
                @Override
                public Integer next() {
                    int curValue = nextValue;
                    nextValue += totalParallelism;
                    return curValue < limit ? curValue : null;
                }
            };
        }

        @Override
        public boolean complete() {
            return emitFromTraverser(traverser);
        }
    }
}
