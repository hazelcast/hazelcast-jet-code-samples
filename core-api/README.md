# Hazelcast Jet Core-API Code Samples

Code samples for Hazelcast Jet, using jet-core APIs.


## Batch Code Samples
  	
- **Data Enrichment ([enrichment-core-api](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/batch/enrichment-core-api))** 

	This sample shows, how to enrich batch or stream of items with additional
   information by matching them by key. 
		
- **Dumping data from Hazelcast Map ([map-dump](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/batch/map-dump))**   

	 This example does a distributed dump of the contents of a Hazelcast IMap
   into several files and illustrates how a simple distributed
   sink can be implemented.
	
- **Migrating from Hazelcat Map-Reduce ([mapreduce-migration](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/batch/mapreduce-migration))**  

	This project shows how a word-count implementation in Hazelcast Map-Reduce
	can be implemented in Hazelcast Jet.
	
- **Prime Number Finder ([prime-finder](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/batch/prime-finder))** 

	This example finds the prime numbers up to a certain number and writes
   the output to a Hazelcast List. A distributed number generator is
   used to distribute the numbers across the processors. This examples is
   mostly aimed at illustrating how a custom partitioning at the source can
   be achieved using the ProcessorMetaSupplier API.
	
- **TF-IDF Calculation ([tf-idf](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/batch/tf-idf))** 

    This example builds, for a given set of text documents, an 
    <em>inverted index</em> that maps each word to the set of documents that
     contain it. Each document in the set is assigned a TF-IDF score which
     tells how relevant the document is to the search term.
	
- **Word-Count ([wordcount-core-api](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/batch/wordcount-core-api))**

	Analyzes a set of documents and finds the number of occurrences of each word
	they contain.
	
- **Word-Count with Hadoop ([wordcount-hadoop](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/batch/wordcount-hadoop))**

	Analyzes a set of documents from HDFS and finds the number of occurrences of
	each word they contain and outputs that to HDFS.
	
	

## Streaming Code Samples


- **Access Stream Analyzer ([access-stream-analyzer](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/streaming/access-stream-analyzer))**

     Analyzes access log files from a HTTP server. Demonstrates reading files
line by  line in streaming fashion - by running indefinitely and watching for
changes as they appear. It uses sliding window aggregation to output frequency
of visits to each page continuously.

	
- **Event Journal Example ([event-journal](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/streaming/event-journal))**

	A sample which consumes and filters events generated from a Hazelcast Map.
	
- **Fault-Tolerance Example ([fault-tolerance](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/streaming/fault-tolerance))**

  A sample which uses Jet with the event journal reader for Hazelcast Map to
  perform rolling average calculations and illustrates the differences in
  processing guarantees.
		
- **Kafka Consumer Example ([kafka](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/streaming/kafka))**

	A sample which does a distributed read from two Kafka topics and writes to a
	Hazelcast Map.

- **Session Window Aggregation ([session-windows](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/streaming/session-windows))**

  A sample demonstrating the use of session windows to track the behavior of the
  users of an online shop application
  
- **Socket Producer and Consumer ([socket](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/streaming/socket))**

 This project contains examples showing how to consume from and produce to
  sockets.
	
- **Stock Exchange Simulation ([stock-exchange](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/streaming/stock-exchange))**

	A simple demonstration of Jet's continuous operators on an infinite stream.
	Initially a Hazelcast map is populated with some stock ticker names; the job
	reads the map and feeds the data to the vertex that simulates an event
	stream coming from a stock market. The job then computes the number of
	trades per ticker within a sliding window of a given duration and dumps the
	results to a set of files
	
- **Finding Top-N Stocks ([top-n-stocks](https://github.com/hazelcast/hazelcast-jet-code-samples/tree/0.5-maintenance/core-api/streaming/top-n-stocks))**

	This sample shows how to nest accumulations. It first calculates linear
	trend for each stock, then finds top 5 stocks with highest price growth and
	top 5 stocks with highest price drop.
	
