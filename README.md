# Hazelcast Jet Code Samples

A repository of code samples for Hazelcast Jet. The samples show you how
to integrate Hazelcast Jet with other systems, how to connect to various
data sources (both from a Hazelcast IMDG and 3rd-party systems) and how
to use the Pipeline API to solve a range of use cases. There is also a
folder with samples using the Core API.

## Basic Examples

### [Word Count](wordcount/src/main/java)

Analyzes a set of documents and finds the number of occurrences
of each word they contain.

### [Enrichment](enrichment/src/main/java)

This sample shows how to enrich a stream of items with additional
information using three different approaches.

### [Hazelcast Connectors](hazelcast-connectors/src/main/java)

Demonstrates the usage of Hazelcast `IMap`, `ICache` and `IList` as data
sources and sinks.

### [Map & Cache Event Journal Source](event-journal/src/main/java)

Shows how to enable and use the event journal to process an unbounded
stream of changes on an `IMap` or `ICache`.

### [Kafka](kafka/src/main/java)

Shows how to connect to a Kafka topic as a Jet data source. Also shows
how to use an Avro schema registry with this source.

### [Job-Management](job-management/src/main/java)

Demonstrates Jet's job management capabilities with job submission,
job tracking, scaling up and updating a job.

### [File IO](file-io/src/main/java)

Shows how to use files on the local filesystem as the data source in
a Jet job. Jet can use files as a batch data source (reading what's
currently in the files) and as an unbounded stream source (emitting
new data as it gets appended to the files).

The sample also shows how to deal with Avro-serialized files.

### [Hadoop](hadoop/src/main/java)

Shows how to work with Hadoop using text input and Avro.

### [JDBC Connector](jdbc/src/main/java)

Demonstrates the usage of a database as a Jet data source/sink.

### [JMS Connector](jms/src/main/java)

Demonstrates the usage of JMS queue and topic as a Jet data source.

### [Session Window Aggregation](session-windows/src/main/java)

Demonstrates the session window vertex to track the behavior of the
users of an online shop application.

### [Socket Source and Sink](sockets/src/main/java)

Samples showing the usage of a TCP/IP socket as a source and a sink.

### [Sliding Windows](sliding-windows/src/main/java)

A demonstration of Jet's sliding window aggregations on an infinite
stream using stock trades as source data.

## Integrations

### [Spring Integration](integration/spring)

A sample project which shows how to integrate Hazelcast Jet with Spring.

### [Pivotal Cloud Foundry](integration/pcf)

A sample Spring Boot application which
shows how to use Hazelcast Jet in Pivotal Cloud Foundry environment.

### [Docker Compose](integration/docker-compose)

Shows how to deploy Hazelcast Jet into a docker environment

### [Kubernetes](integration/kubernetes)

Shows how to deploy Hazelcast Jet inside Kubernetes environment. 

## Enterprise

### [Enterprise SSL Configuration](enterprise)

Shows how to configure Hazelcast Jet Enterprise with SSL

## Advanced Examples

### [Co-Group Transform](co-group/src/main/java)

Uses the co-group transform to perform a full outer join of three
streams on a common join key (a many-to-many relationship). Shows
both batch and streaming approaches.

### [Custom Source](source-builder/src/main/java)

Shows how to use the Source Builder to create a custom Jet data source.
The sample creates an unbounded stream source that repeatedly issues an
HTTP request and emits the data from the response. The sample includes
a simple HTTP server that serves system monitor data, a Jet pipeline
that performs windowed aggregation on the data, and a GUI window that
visualizes the results.

### [Custom Sink](sink-builder/src/main/java)

Shows how to use the Sink Builder to create a custom Jet data sink.
The sink in the sample connects to a Hazelcast `ITopic`.

### [Fault Tolerance](fault-tolerance/src/main/java)

A sample that illustrates the various fault tolerance features of Jet
and the effects of different processing guarantees that a Jet job can be
configured with.

### [Inverted Index with TF-IDF Scoring](tf-idf/src/main/java)

The sample builds an _inverted index_ on a corpus of about a 100 MB of
book material and then presents you with a GUI dialog where you can
enter your search terms. The GUI poignantly demonstrates the speed of
the search by instantly responding to every keystroke and displaying a
result list.

### [Core-API Samples](core-api/README.md)

Various samples using the DAG API. See the related README for more details.

## License

Hazelcast is available under the Apache 2 License. Please see the
[Licensing section](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#licensing)
for more information.

## Copyright

Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
