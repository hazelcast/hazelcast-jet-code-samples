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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.pipeline.ContextFactory;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.tensorflow.SavedModelBundle;
import org.tensorflow.Tensor;
import org.tensorflow.Tensors;

import java.util.Map;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;

/**
 * Execute the TensorFlow model using the in-process method.
 * <p>
 * See README.md in the project root for more information.
 */
public class InProcessClassification {

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");

        if (args.length != 1) {
            System.out.println("Usage: InProcessClassification <data path>");
            System.exit(1);
        }

        WordIndex wordIndex = new WordIndex(args[0]);
        JetInstance instance = Jet.newJetInstance();
        try {
            IMap<Long, String> reviewsMap = instance.getMap("reviewsMap");
            SampleReviews.populateReviewsMap(reviewsMap);

            // Create the factory that will load the model on each member, shared
            // by all parallel processors on that member. The path has to be available
            // on all members.
            ContextFactory<SavedModelBundle> modelContext = ContextFactory
                    .withCreateFn(jet -> SavedModelBundle.load(args[0] + "/model/1", "serve"))
                    .shareLocally()
                    .withDestroyFn(SavedModelBundle::close);

            Pipeline p = Pipeline.create();
            p.drawFrom(Sources.map(reviewsMap))
             .map(Map.Entry::getValue)
             .mapUsingContext(modelContext, (model, review) -> executeClassification(wordIndex, model, review))
             // TensorFlow executes modes in parallel, we'll use 2 local threads to maximize throughput.
             .setLocalParallelism(2)
             .drainTo(Sinks.logger());

            instance.newJob(p).join();
        } finally {
            instance.shutdown();
        }
    }

    private static Tuple2<String, Float> executeClassification(WordIndex wordIndex,
                                                               SavedModelBundle model,
                                                               String review) {
        try (Tensor<Float> input = Tensors.create(wordIndex.createTensorInput(review));
             Tensor<?> output = model.session().runner().feed("embedding_input:0", input)
                                     .fetch("dense_1/Sigmoid:0").run().get(0)) {
            float[][] result = new float[1][1];
            output.copyTo(result);
            return tuple2(review, result[0][0]);
        }
    }
}
