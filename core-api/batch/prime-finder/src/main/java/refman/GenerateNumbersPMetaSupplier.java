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

package refman;

import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.ProcessorSupplier;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

class GenerateNumbersPMetaSupplier implements ProcessorMetaSupplier {

    private final int upperBound;

    private transient int totalParallelism;
    private transient int localParallelism;

    GenerateNumbersPMetaSupplier(int upperBound) {
        this.upperBound = upperBound;
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
            // We'll calculate the global index of each processor in the cluster:
            int globalIndexBase = localParallelism * i;
            // Capture the value of the transient field for the lambdas below:
            int divisor = totalParallelism;
            // processorCount will be equal to localParallelism:
            ProcessorSupplier supplier = processorCount ->
                    range(globalIndexBase, globalIndexBase + processorCount)
                            .mapToObj(globalIndex ->
                                new GenerateNumbersP(upperBound, divisor, globalIndex)
                            ).collect(toList());
            map.put(addresses.get(i), supplier);
        }
        return map::get;
    }

}
