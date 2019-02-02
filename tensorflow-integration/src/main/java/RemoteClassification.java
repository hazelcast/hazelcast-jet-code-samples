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

import com.google.protobuf.Int64Value;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.tensorflow.framework.TensorProto;
import org.tensorflow.framework.TensorShapeProto;
import tensorflow.serving.Model;
import tensorflow.serving.Predict;
import tensorflow.serving.PredictionServiceGrpc;
import tensorflow.serving.PredictionServiceGrpc.PredictionServiceBlockingStub;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;

public class RemoteClassification {
    public static void main(String[] args) {
        System.out.println("loading word index...");
        WordIndex wordIndex = new WordIndex(args);

        ManagedChannel channel = ManagedChannelBuilder.forAddress("192.168.139.3", 8500)
                                                      .usePlaintext().build();
        PredictionServiceBlockingStub blockingStub = PredictionServiceGrpc.newBlockingStub(channel);
        List<String> reviews = asList(
                "the movie was good",
                "the movie was bad",
                "excellent movie best piece ever",
                "absolute disaster, worst movie ever",
                "had both good and bad parts, excellent casting, but camera was poor");
        for (String review : reviews) {
            System.out.println("sending request for review: " + review);
            float[][] featuresTensorData = wordIndex.createTensorInput(review);
            TensorProto.Builder featuresTensorBuilder = TensorProto.newBuilder();

            for (float[] featuresTensorDatum : featuresTensorData) {
                for (float v : featuresTensorDatum) {
                    featuresTensorBuilder.addFloatVal(v);
                }
            }

            TensorShapeProto.Dim featuresDim1 = TensorShapeProto.Dim.newBuilder().setSize(featuresTensorData.length).build();
            TensorShapeProto.Dim featuresDim2 = TensorShapeProto.Dim.newBuilder().setSize(featuresTensorData[0].length).build();
            TensorShapeProto featuresShape = TensorShapeProto.newBuilder().addDim(featuresDim1).addDim(featuresDim2).build();
            featuresTensorBuilder.setDtype(org.tensorflow.framework.DataType.DT_FLOAT).setTensorShape(featuresShape);
            TensorProto featuresTensorProto = featuresTensorBuilder.build();

            // Generate gRPC request
            Int64Value version = Int64Value.newBuilder().setValue(1).build();
            Model.ModelSpec modelSpec = Model.ModelSpec.newBuilder().setName("reviewSentiment").setVersion(version).build();
            Predict.PredictRequest request = Predict.PredictRequest.newBuilder().setModelSpec(modelSpec).putInputs("input_review", featuresTensorProto).build();

            // Request gRPC server
            Predict.PredictResponse response;
            try {
                response = blockingStub.withDeadlineAfter(10, TimeUnit.SECONDS).predict(request);
                TensorProto output = response.getOutputsOrThrow("dense_1/Sigmoid:0");
                System.out.println("Response: " + output.getFloatVal(0));
            } catch (StatusRuntimeException e) {
                System.out.println("RPC failed: " + e.getStatus());
                return;
            }
        }
    }
}
