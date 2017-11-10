# Hazelcast Jet Core-API Code Samples

Code samples for Hazelcast Jet using the Core API.


## Batch Jobs
  			
- **[Custom File Sink](batch/map-dump/src/main/java/MapDump.java)**   

    Shows how to implement a custom distributed sink that stores the
    data in files.
	
- **[Migrating from Hazelcat Map-Reduce](batch/mapreduce-migration/src/main/java)**  

    Contains parallel samples with Hazelcast's Map-Reduce API and
    Hazelcast Jet's Core API that help you migrate your Map-Reduce code
    to Hazelcast Jet.
	
- **[Prime Number Finder](batch/prime-finder/src/main/java/PrimeFinder.java)** 

	Shows how to implement a custom distributed source, including custom
	partitioning at the source using the `ProcessorMetaSupplier` API.
	The sample implements a distributed generator of integers which is 
	used as the source for a filtering vertex that selects the prime
	numbers from it.
	
- **[Inverted Index with TF-IDF Scoring](batch/tf-idf/src/main/java/TfIdf.java)** 

    Demonstrates the power of the Core API by building a hand-optimized
    DAG that cannot be reproduced with the higher-level APIs. The sample
    builds an _inverted index_ on a corpus of about a 100 MB of book
    material and then presents you with a GUI dialog where you can enter 
    your search terms. The GUI poignantly demonstrates the speed of the
    search by instantly responding to every keystroke and displaying a
    result list.
	
- **[Word Count](batch/wordcount-core-api/src/main/java/WordCountCoreApi.java)**

    The classical Word Count task implemented in the Core API.


## Streaming Jobs


- **[Access Stream Analyzer](streaming/access-stream-analyzer/src/main/java/AccessStreamAnalyzer.java)**

    Shows how to use the File Watcher streaming source. It continuously
    monitors HTTP access log files for new content and applies a sliding
    window aggregation that tracks the frequency of visits to each page.
	
- **[Fault Tolerance](streaming/fault-tolerance/src/main/java/FaultTolerance.java)**

  Illustrates the effects of different processing guarantees that 
  a Jet job can be configured with. Uses a Hazelcast `IMap`'s Event Journal to perform rolling average calculations.
		
- **[Session Window Aggregation](streaming/session-windows/src/main/java/SessionWindowsSample.java)**

  Demonstrates the session window vertex to track the behavior of the
  users of an online shop application.
  	
- **[Stock Exchange Simulation](streaming/stock-exchange/src/main/java)**

    Two samples that demonstrate sliding window aggregation in a
    single-stage and in a two-stage setup.
	
- **[Finding Top-N Stocks](streaming/top-n-stocks/src/main/java/TopNStocks.java)**

    Demonstrates cascaded sliding windows where the second one's source
    is the output of the first one. The first one calculates the
    frequency 	of trading each stock and the second one finds the
    hottest-trading stocks.

