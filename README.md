# Hazelcast Jet Code Samples

A repository of code samples for Hazelcast Jet. It covers the high-level
APIs (pipelines and java.util.stream) as well as the lower-level Core API.
This is the basic layout of the directories:

1. `batch`: batch jobs with high-level APIs
2. `streaming`: streaming jobs with high-level APIs
4. `pcf`: a sample application on the Pivotal Cloud Foundry infrastructure
3. `core-api/batch`: batch jobs with the Core API
3. `core-api/streaming`: streaming jobs with the Core API
4. `sample-data`: a module containing the sample data shared by
   several samples
5. `refman`: code snippets used in the Reference Manual

## Batch Jobs

- **[Access Log Analyzer](batch/access-log-analyzer/src/main/java/AccessLogAnalyzer.java)**

  Analyzes access log files from an HTTP server. Demonstrates the usage
  of file sources and sinks.

- **[Co-Group Transform](batch/co-group/src/main/java/CoGroup.java)**

  Uses the co-group transform to perform a full outer join of three
  streams on a common join key (a many-to-many relationship).
	
- **[Hazelcast Connectors](batch/hazelcast-connectors/src/main/java)**

	Demonstrates the usage of Hazelcast `IMap`, `ICache` and `IList` as
	data sources and sinks.
	
- **[java.util.stream API Samples](batch/java.util.stream/src/main/java)**

	Several basic samples using the java.util.stream API.
	
- **[Word Count](batch/wordcount/src/main/java/WordCount.java)**

	Analyzes a set of documents and finds the number of occurrences of
	each word they contain.

- **[Word Count with HDFS](batch/wordcount-hadoop/src/main/java/HadoopWordCount.java)**

   A Word Count job with the Hadoop File System as both source and sink.
   
## Streaming Jobs

- **[Data Enrichment Using Hash-Join](streaming/enrichment/src/main/java/Enrichment.java)**

	Uses the hash-join transform to enrich an infinite stream of trade
	events. Attaches to each event the associated product and broker
	objects.

- **[Map Journal Source](streaming/map-journal-source/src/main/java/MapJournalSource.java)**

	Consumes and filters events generated from a Hazelcast `IMap`'s
	Event Journal.
	
- **[Kafka Source](streaming/kafka-source/src/main/java/KafkaSource.java)**

	Demonstrates the usage of a Kafka topic as a Jet data source. Pours
	the data from a Kafka topic to a Hazelcast `IMap`.

- **[Socket Connector](streaming/socket-connector/src/main/java)**

 	Two code samples showing the usage of a TCP/IP socket as a source
 	and a sink.

## Spring Integration

The directory [spring](spring) contains a sample project which shows how to integrate 
Hazelcast Jet with Spring.
 			
## Pivotal Cloud Foundry

The directory [pcf](pcf) contains a sample Spring Boot application which shows how to use Hazelcast Jet in Pivotal Cloud Foundry environment. 

## Core API

Refer to the [core-api](core-api) readme for details on its contents.

#### License

Hazelcast is available under the Apache 2 License. Please see the 
[Licensing section](http://docs.hazelcast.org/docs/latest-dev/manual/html-single/index.html#licensing) 
for more information.

#### Copyright

Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
