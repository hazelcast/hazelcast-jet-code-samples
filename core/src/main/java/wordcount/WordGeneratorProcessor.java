/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package wordcount;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.JetPair;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.processor.ContainerProcessor;

import java.util.regex.Pattern;

/**
 * Processor which parses incoming lines for individual words and emits a Pair with (word, 1) for each
 * encountered word.
 */
public class WordGeneratorProcessor implements ContainerProcessor<Pair<Integer, String>, Pair<String, Integer>> {

    static final Pattern PATTERN = Pattern.compile("\\W+");

    @Override
    public boolean process(ProducerInputStream<Pair<Integer, String>> inputStream,
                           ConsumerOutputStream<Pair<String, Integer>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Pair<Integer, String> pair : inputStream) {

            // split each line into lowercase words
            String[] split = PATTERN.split(pair.getValue().toLowerCase());

            for (String word : split) {
                // emit each word with count of 1
                outputStream.consume(new JetPair<>(word, 1));
            }
        }
        return true;
    }

}
