# Hazelcast Jet Code Samples

A repository of code samples for Hazelcast Jet. It includes several
samples for various features and use cases, as well as some samples
for various integrations.

The following samples are included:

## Basic Examples

### [Word Count](wordcount/src/main/java)

Analyzes a set of documents and finds the number of occurrences
of each word they contain.

### [Enrichment](enrichment/src/main/java)

This sample shows how to enrich a stream of items with additional
information using three different approaches.

### [Hazelcast Connectors](hazelcast-connectors/src/main/java)

Demonstrates the usage of Hazelcast `IMap`, `ICache` and `IList` as
data sources and sinks.

### [Map & Cache Event Journal Source](event-journal/src/main/java)

An application which shows how to enable and use the event journal to process
changes in `IMap` and `ICache` structures.

### [Kafka](kafka/src/main/java)

Demonstrates the usage of a Kafka topic as a Jet data source and additionally
shows how an Avro schema registry can be used with this source.

### [Job-Management](job-management/src/main/java)

Demonstrates Jet's job management capabilities with job submission,
job tracking and scaling-up.
	    
### [File IO](file-io/src/main/java)

This sample shows how to work with both batch and stream oriented file sources 
in Jet, as well as dealing with Avro files.

### [Hadoop](hadoop/src/main/java)

A sample which shows how to work with Hadoop using text input and Avro

### [JDBC Connector](jdbc/src/main/java)

Demonstrates the usage of a database as a Jet data source/sink.
    
### [JMS Connector](jms/src/main/java)

Demonstrates the usage of JMS queue and topic as a Jet data source.
    
### [Session Window Aggregation](session-windows/src/main/java)

Demonstrates the session window vertex to track the behavior of the
users of an online shop application.

### [Socket Source and Sink](sockets/src/main/java)

Samples showing the usage of a TCP/IP socket as a source
and a sink.

### [Sliding Windows](sliding-windows/src/main/java)

A demonstration of Jet's sliding window aggregations on an infinite stream
using stock trades as source data.

## Integrations

### [Spring Integration](integration/spring)

A sample project which shows how to integrate Hazelcast Jet with Spring.
 			
### [Pivotal Cloud Foundry](integration/pcf)

A sample Spring Boot application which 
shows how to use Hazelcast Jet in Pivotal Cloud Foundry environment. 

### [Docker Compose](integration/docker-compose)

Shows how to deploy Jet into a docker environment

## Advanced Examples

### [Co-Group Transform](co-group/src/main/java)
   
Uses the co-group transform to perform a full outer join of three
streams on a common join key (a many-to-many relationship). Shows
both batch and streaming approaches.

### [Custom Sink](custom-sink/src/main/java)

Demonstrates implementation of a Hazelcast `ITopic` sink with a sample
which does text filtering on the books with the	Pipeline API.

### [Fault Tolerance](fault-tolerance/src/main/java)

A sample that illustrates the various fault tolerance features of Jet and the 
effects of different processing guarantees that a Jet job can be configured with.

### [Inverted Index with TF-IDF Scoring](tf-idf/src/main/java)

The sample builds an _inverted index_ on a corpus of about a 100 MB of book
material and then presents you with a GUI dialog where you can enter
your search terms. The GUI poignantly demonstrates the speed of the
search by instantly responding to every keystroke and displaying a
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
