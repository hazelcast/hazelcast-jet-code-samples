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

import org.tensorflow.SavedModelBundle;
import org.tensorflow.Tensor;
import org.tensorflow.Tensors;

import java.util.List;

import static java.util.Arrays.asList;

public class LocalClassification {

    public static void main(String[] args) {
        System.out.println("loading word index...");
        WordIndex wordIndex = new WordIndex(args);

        List<String> reviews = asList(
                "the movie was good",
                "the movie was bad",
                "excellent movie best piece ever",
                "absolute disaster, worst movie ever",
                "had both good and bad parts, excellent casting, but camera was poor");

        System.out.println("loading model...");
        String simpleMlp = "c:/work/hazelcast/python-project/output";
        try (SavedModelBundle model = SavedModelBundle.load(simpleMlp, "serve")) {
            System.out.println("model loaded");
            for (String review : reviews) {
                try (Tensor<Float> input = Tensors.create(wordIndex.createTensorInput(review));
                     Tensor<?> output = model.session().runner().feed("embedding_input:0", input)
                                             .fetch("dense_1/Sigmoid:0").run().get(0)) {
                    float[][] result = new float[1][1];
                    output.copyTo(result);
                    System.out.println(review + " = " + result[0][0]);
                }
            }
        }
    }
}
