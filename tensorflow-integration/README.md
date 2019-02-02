# Sample running TensorFlow models from Hazelcast Jet

TensorFlow is a popular library to train and use machine learning
models. We integrate it with Jet to classify stream of events with the
result of TF model classification.

TensorFlow provides two ways of running models:
- In-process: load the model into memory and execute it
- ModelServer: a request-response based service that executes the model
and can also handle multiple versions of models etc. The calls can be
made using gRPC or through a REST API.

As a sample, we took the IMDB Movie Reviews Dataset as provided by the
TensorFlow Keras Datasets. We train the model as described in a
[tutorial](https://www.tensorflow.org/tutorials/keras/basic_text_classification)
and save it.
