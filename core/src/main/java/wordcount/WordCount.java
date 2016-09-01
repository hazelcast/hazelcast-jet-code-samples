/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package wordcount;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.sink.MapSink;
import com.hazelcast.jet.dag.source.MapSource;
import com.hazelcast.jet.job.Job;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

/**
 * A distributed word count can be implemented with three vertices as follows:
 * -------------              ---------------                         ------------
 * | Generator |-(word, 1)--> | Accumulator | -(word, localCount)--> | Combiner  | --(word, globalCount) ->
 * -------------              ---------------                         ------------
 * <p>
 * first vertex will be split the words in each paragraph and emit tuples as (WORD, 1)
 * second vertex will combine the counts locally on each node
 * third vertex will combine the counts across all nodes.
 * <p>
 * The edge between generator and accumulator is local, but partitioned, so that all words with same hash go
 * to the same instance of the processor on the same node.
 * <p>
 * The edge between the accumulator and combiner vertex is both shuffled and partitioned, meaning all words
 * with same hash are processed by the same instance of the processor across all nodes.
 */
public class WordCount {

    private static final ILogger LOGGER = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        IMap<Integer, String> source = instance1.getMap("source");
        IMap<String, Integer> sink = instance1.getMap("sink");

        source.put(0, "It was the best of times, "
                + "it was the worst of times, "
                + "it was the age of wisdom, "
                + "it was the age of foolishness, "
                + "it was the epoch of belief, "
                + "it was the epoch of incredulity, "
                + "it was the season of Light, "
                + "it was the season of Darkness, "
                + "it was the spring of hope, "
                + "it was the winter of despair, "
                + "we had everything before us, "
                + "we had nothing before us, "
                + "we were all going direct to Heaven, "
                + "we were all going direct the other way-- "
                + "in short, the period was so far like the present period, that some of "
                + "its noisiest authorities insisted on its being received, for good or for "
                + "evil, in the superlative degree of comparison only.");

        source.put(1, "There were a king with a large jaw and a queen with a plain face, on the "
                + "throne of England; there were a king with a large jaw and a queen with "
                + "a fair face, on the throne of France. In both countries it was clearer "
                + "than crystal to the lords of the State preserves of loaves and fishes, "
                + "that things in general were settled for ever.");

        source.put(2, "It was the year of Our Lord one thousand seven hundred and seventy-five. "
                + "Spiritual revelations were conceded to England at that favoured period, "
                + "as at this. Mrs. Southcott had recently attained her five-and-twentieth "
                + "blessed birthday, of whom a prophetic private in the Life Guards had "
                + "heralded the sublime appearance by announcing that arrangements were "
                + "made for the swallowing up of London and Westminster. Even the Cock-lane "
                + "ghost had been laid only a round dozen of years, after rapping out its "
                + "messages, as the spirits of this very year last past (supernaturally "
                + "deficient in originality) rapped out theirs. Mere messages in the "
                + "earthly order of events had lately come to the English Crown and People, "
                + "from a congress of British subjects in America: which, strange "
                + "to relate, have proved more important to the human race than any "
                + "communications yet received through any of the chickens of the Cock-lane "
                + "brood.");


        DAG dag = new DAG();

        int parallelism = Runtime.getRuntime().availableProcessors();

        Vertex generator = new Vertex("word-generator", WordGeneratorProcessor.class)
                .parallelism(parallelism);

        generator.addSource(new MapSource(source));

        Vertex accumulator = new Vertex("word-accumulator", WordCombinerProcessor.class)
                .parallelism(parallelism);

        Vertex combiner = new Vertex("word-combiner", WordCombinerProcessor.class)
                .parallelism(parallelism);

        combiner.addSink(new MapSink(sink));

        dag.addVertex(generator);
        dag.addVertex(accumulator);
        dag.addVertex(combiner);

        // use partitioning to ensure the same words are consumed by the same processor instance
        dag.addEdge(new Edge("generator-accumulator", generator, accumulator)
                .partitioned());

        dag.addEdge(new Edge("accumulator-combiner", accumulator, combiner)
                .partitioned()
                .distributed()
        );

        LOGGER.info("Submitting DAG");
        Job job = JetEngine.getJob(instance1, "word-count", dag);
        try {
            LOGGER.info("Executing application");
            job.execute().get();
            LOGGER.info("Counts=" + sink.entrySet().toString());
        } finally {
            job.destroy();
            Hazelcast.shutdownAll();
        }
    }
}
