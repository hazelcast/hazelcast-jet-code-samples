# Sample running TensorFlow models from Hazelcast Jet

TensorFlow is a popular library to train and use machine learning
models. We integrate it with Jet to classify stream of events with the
result of a TF model execution.

TensorFlow provides two ways of running models:
- In-process: load the model into memory and execute it
- In ModelServer: a request-response based service that executes the
model and can also handle multiple versions of the models etc. The calls
can be made using gRPC or through a REST API.

Each way has pros and cons and discussion of them is beyond the scope of
this sample.

As a sample, we took the Large Movie Reviews Dataset as provided by the
TensorFlow Keras Datasets. We trained a model as described in a
[tutorial](https://www.tensorflow.org/tutorials/keras/basic_text_classification)
and save it. The result of the training is in the
[`data/model`](data/model) subdirectory.

You can run either in-process classification or classification using the
ModelServer:

```
$ mvn exec:java -Dexec.mainClass=InProcessClassification
$ mvn exec:java -Dexec.mainClass=ModelServerClassification
```

If you want to re-train the model yourself, you need to have python
installed. Since TensorFlow 1.12 [doesn't support the current python
version](https://github.com/tensorflow/tensorflow/issues/17022), you
should use python 3.6 and install tensorflow using:

```
$ pip install tensorflow
```

Then, run the provided script to download the source dataset, train and
save the model:

```
$ cd {hazelcast-jet-code-samples}/tensorflow-integration
$ bin/imdb_review_train.py
```

After this, `data/model` subdirectory should be newly written with the
following contents:

```
data/model/1/saved_model.pb
data/model/1/variables/variables.data-00000-of-00001
data/model/1/variables/variables.index
data/imdb_word_index.json
```
