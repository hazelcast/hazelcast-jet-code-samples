# Hazelcast Jet Code Samples

A repository of code samples for Hazelcast Jet. It covers the high-level
APIs (pipelines and java.util.stream) as well as the lower-level Core API.
This is the basic layout of the directories:

1. `batch`: samples of batch jobs
2. `streaming`: samples of streaming jobs
3. `pcf`: a sample application on the Pivotal Cloud Foundry infrastructure
4. `core-api`: samples using the DAG API
5. `sample-data`: a module containing the sample data shared by
   several samples
6. `refman`: code snippets used in the Reference Manual

## Batch Jobs

- **[Access Log Analyzer](batch/access-log-analyzer/src/main/java/AccessLogAnalyzer.java)**

  Analyzes access log files from an HTTP server. Demonstrates the usage
  of file sources and sinks.
  
- **[Avro File Connector](batch/avro-file/src/main/java/)**

  Demonstrates the usage of Apache Avro file as data source and sink. 

- **[Batch Enrichment](batch/batch-enrichment/src/main/java/)**

    This sample shows, how to enrich batch of items with additional
    information by matching them by key

- **[Co-Group Transform](batch/co-group/src/main/java/BatchCoGroup.java)**

  Uses the co-group transform to perform a full outer join of three
  streams on a common join key (a many-to-many relationship).

- **[Custom Sink Builder](batch/custom-sink-builder/src/main/java)**

	Demonstrates implementation of a Hazelcast `ITopic` sink with a sample
	which does text filtering on the books with the	Pipeline API.

- **[Hazelcast Connectors](batch/hazelcast-connectors/src/main/java)**

	Demonstrates the usage of Hazelcast `IMap`, `ICache` and `IList` as
	data sources and sinks.
	
- **[Hdfs Avro File](batch/hdfs-avro-fil/src/main/java)**

	Demonstrates the usage of Apache Avro file as data source and sink in 
	Hadoop File System.

- **[java.util.stream API Samples](batch/java.util.stream/src/main/java)**

	Several basic samples using the java.util.stream API.

- **[Inverted Index with TF-IDF Scoring](batch/tf-idf/src/main/java/TfIdf.java)**

    The sample builds an _inverted index_ on a corpus of about a 100 MB of book
    material and then presents you with a GUI dialog where you can enter
    your search terms. The GUI poignantly demonstrates the speed of the
    search by instantly responding to every keystroke and displaying a
    result list.

- **[Word Count](batch/wordcount/src/main/java/WordCount.java)**

	Analyzes a set of documents and finds the number of occurrences of
	each word they contain.

- **[Word Count with HDFS](batch/wordcount-hadoop/src/main/java/HadoopWordCount.java)**

   A Word Count job with the Hadoop File System as both source and sink.
   
## Streaming Jobs

- **[Access Stream Analyzer](streaming/access-stream-analyzer/src/main/java/AccessLogStreamAnalyzer.java)**

    Shows how to use the File Watcher streaming source. It continuously
     monitors HTTP access log files for new content and applies a sliding
     window aggregation that tracks the frequency of visits to each page.
     
- **[Map & Cache Event Journal Source](streaming/event-journal-source/src/main/java/)**

	Consumes and filters events generated from Hazelcast `IMap`'s and `ICache`'s
	Event Journal.

- **[Fault Tolerance](streaming/fault-tolerance/src/main/java/FaultTolerance.java)**

    Illustrates the effects of different processing guarantees that
    a Jet job can be configured with a simple application which uses Jet with the event journal reader for
    to perform rolling average calculations and illustrates the differences in processing guarantees.
    
- **[JMS Connector](streaming/jms-connector/src/main/java/)**

    Demonstrates the usage of JMS queue and topic as a Jet data source.

- **[Job-Management](streaming/job-management/src/main/java/)**

    Demonstrates Jet's job management capabilities with job submission, job tracking and scaling-up.
    
- **[Avro Serialization for Kafka](streaming/kafka-avro-serialization/src/main/java/KafkaSource.java)**

    Demonstrates the usage of Apache Avro serialization and schema registry
    for Kafka.

- **[Kafka Source](streaming/kafka-source/src/main/java/KafkaSource.java)**

	Demonstrates the usage of a Kafka topic as a Jet data source. Pours
	the data from a Kafka topic to a Hazelcast `IMap`.

- **[Session Window Aggregation](streaming/session-windows/src/main/java/SessionWindow.java)**

  Demonstrates the session window vertex to track the behavior of the
  users of an online shop application.

- **[Socket Connector](streaming/socket-connector/src/main/java)**

 	Two code samples showing the usage of a TCP/IP socket as a source
 	and a sink.

- **[Stock Exchange Simulation](streaming/stock-exchange/src/main/java)**

    A simple demonstration of Jet's sliding window aggregation on an infinite stream

- **[Data Enrichment Using Hash-Join](streaming/streaming-enrichment/src/main/java/StreamingEnrichment.java)**

	Uses the hash-join transform to enrich an infinite stream of trade
	events. Attaches to each event the associated product and broker
	objects.

- **[Finding Top-N Stocks](streaming/top-n-stocks/src/main/java/TopNStocks.java)**

    Demonstrates cascaded sliding windows where the second one's source
    is the output of the first one. The first one calculates the
    frequency 	of trading each stock and the second one finds the
    hottest-trading stocks.

- **[Windowed Co-Group](streaming/windowed-cogroup/src/main/java)**

    Uses the co-group transform to perform a full outer join of three
    streams on a common join key (a many-to-many relationship)

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
