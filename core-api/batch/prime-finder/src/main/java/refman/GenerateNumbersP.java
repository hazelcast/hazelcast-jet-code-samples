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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Traversers;

import java.util.stream.IntStream;

class GenerateNumbersP extends AbstractProcessor {

    private final Traverser<Integer> traverser;

    GenerateNumbersP(int upperBound) {
        traverser = Traversers.traverseStream(IntStream.range(0, upperBound).boxed());
    }

    GenerateNumbersP(int upperBound, int processorCount, int processorIndex) {
        traverser = Traversers.traverseStream(
                IntStream.range(0, upperBound)
                         .filter(n -> n % processorCount == processorIndex)
                         .boxed());
    }

    @Override
    public boolean complete() {
        return emitFromTraverser(traverser);
    }
}
