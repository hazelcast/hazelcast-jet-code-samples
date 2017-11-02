# Hazelcast Jet Code Samples

Code samples for Hazelcast Jet, using both jet-core and java.util.stream APIs.


## Batch Code Samples

- **Access Log Analyzer ([access-log-analyzer](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/access-log-analyzer))**

  Analyzes access log files from an HTTP server. Demonstrates the usage of reading from files and writing results to another file.

- **Co-Grouping ([cogroup-operator](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/cogroup-operator))**

  A sample demonstrating the implementation of CoGroup processor. The processor
  does a full outer join on two sources by a given key demonstrating a
  many-to-many relationship.
  	
- **Data Enrichment ([enrichment](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/enrichment))** 

	This sample shows how to enrich batch or stream of items with additional
   information by matching them by key. 
	
- **Hazelcast Connectors ([hazelcast-connectors](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/hazelcast-connectors))**

	An example that shows how to read from and write to Hazelcast IList, IMap and ICache data structures.
	
- **java.util.stream API ([java.util.stream](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/java.util.stream))**   

	An example that shows how to sort and filter the data residing in Hazelcast IMap using java.util.stream API. 
	
- **Dumping data from Hazelcast IMap ([map-dump](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/map-dump))**   

	 This example does a distributed dump of the contents of a Hazelcast IMap
   into several files and illustrates how a simple distributed
   sink can be implemented.
	
- **Migrating from Hazelcat MapReduce ([mapreduce-migration](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/mapreduce-migration))**  

	This project shows how a word-count implementation in Hazelcast MapReduce can be implemented in Hazelcast Jet.
	
- **Prime Number Finder ([prime-finder](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/prime-finder))** 

	This example finds the prime numbers up to a certain number and writes
   the output to a Hazelcast IList. A distributed number generator is
   used to distribute the numbers across the processors. This example is
   mostly aimed at illustrating how a custom partitioning at the source can
   be achieved using the ProcessorMetaSupplier API.
	
- **TF-IDF Calculation ([tf-idf](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/tf-idf))** 

	This example builds, for a given set of text documents, an <em>inverted index</em> that
   maps each word to the set of documents that contain it. Each document in
   the set is assigned a TF-IDF score which tells how relevant the document
   is to the search term.
	
- **Word-Count ([wordcount-core-api](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/wordcount-core-api))**

	Analyzes a set of documents and finds the number of occurrences of each word they contain.
	
- **Word-Count with Hadoop ([wordcount-hadoop](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/wordcount-hadoop))**

	Analyzes a set of documents from HDFS and finds the number of occurrences of each word they contain and outputs that number to HDFS.

	
- **Word-Count with java.util.stream API ([wordcount-j.u.s](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/batch/wordcount-j.u.s))**

	Analyzes a set of documents and finds the number of occurrences of each word they contain using the java.util.stream API.

	
	

## Streaming Code Samples


- **Access Stream Analyzer ([access-stream-analyzer](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/streaming/access-stream-analyzer))**

	 Analyzes access log files from an HTTP server. Demonstrates reading files line by
 line in streaming fashion - by running indefinitely and watching for changes as they appear. It uses sliding window aggregation to output frequency of visits to each page continuously.

	
- **Kafka Consumer Example ([kafka](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/streaming/kafka))**

	A sample which does a distributed read from two Kafka topics and writes to a Hazelcast IMap.

- **Session Window Aggregation ([session-windows](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/streaming/session-windows))**

  A sample demonstrating the use of session windows to track the behavior of the users of an online shop application.
  
- **Socket Producer and Consumer ([socket](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/streaming/socket))**

 This project contains examples showing how to consume from and produce to sockets.
	
- **Stock Exchange Simulation ([stock-exchange](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/streaming/stock-exchange))**

	A simple demonstration of Jet's continuous operators on an infinite stream. Initially a Hazelcast IMap is populated with some stock ticker names; the job reads the map and feeds the data to the vertex that simulates an event stream coming from a stock market. The job then computes the number of trades per ticker within a sliding window of a given duration and dumps the results to a set of files.
	
- **Finding Top-N Stocks ([top-n-stocks](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.4-maintenance/streaming/top-n-stocks))**

	This sample shows how to nest accumulations. It first calculates the linear trend for each stock, then finds the top 5 stocks with the highest price growth and top 5 stocks with the highest price drop.
	

#### License

Hazelcast is available under the Apache 2 License. Please see the [Licensing section](http://docs.hazelcast.org/docs/latest/manual/html-single/index.html#licensing) for more information.

#### Copyright

Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.

Visit [www.hazelcast.com](http://www.hazelcast.com/) for more info.
