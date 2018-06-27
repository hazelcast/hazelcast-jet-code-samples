# Hazelcast Jet Core-API Code Samples

Code samples for Hazelcast Jet using the Core API.


## Batch Jobs

- **[Enrichment](enrichment-core-api/src/main/java/)**

    This sample shows, how to enrich items with additional
    information by matching them by key

- **[Stock Exchange Simulation](stock-exchange/src/main/java)**

    Two samples that demonstrate sliding window aggregation in a
    single-stage and in a two-stage setup.

- **[Custom File Sink](map-dump/src/main/java/MapDump.java)**   

    Shows how to implement a custom distributed sink that stores the
    data in files.
	
- **[Prime Number Finder](prime-finder/src/main/java/PrimeFinder.java)** 

	Shows how to implement a custom distributed source, including custom
	partitioning at the source using the `ProcessorMetaSupplier` API.
	The sample implements a distributed generator of integers which is 
	used as the source for a filtering vertex that selects the prime
	numbers from it.
	
- **[Inverted Index with TF-IDF Scoring](tf-idf/src/main/java/TfIdf.java)** 

    Demonstrates the power of the Core API by building a hand-optimized
    DAG that cannot be reproduced with the higher-level APIs. The sample
    builds an _inverted index_ on a corpus of about a 100 MB of book
    material and then presents you with a GUI dialog where you can enter 
    your search terms. The GUI poignantly demonstrates the speed of the
    search by instantly responding to every keystroke and displaying a
    result list.
	
- **[Word Count](wordcount-core-api/src/main/java/WordCountCoreApi.java)**

    The classical Word Count task implemented in the Core API.


