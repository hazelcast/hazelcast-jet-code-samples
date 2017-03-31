/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.connector.hadoop.ReadHdfsP;
import com.hazelcast.jet.connector.hadoop.WriteHdfsP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import javax.annotation.Nonnull;
import java.util.Map;
import java.util.regex.Pattern;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Projections.entryKey;
import static com.hazelcast.jet.Projections.wholeItem;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.flatMap;
import static com.hazelcast.jet.Processors.groupAndAccumulate;
import static com.hazelcast.jet.Traversers.traverseArray;
import static com.hazelcast.jet.connector.hadoop.ReadHdfsP.readHdfs;
import static java.lang.Runtime.getRuntime;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * Word count example adapted to read from and write to HDFS instead of Jet in memory maps.
 * <p>
 * For more details about the word count DAG itself, please see the JavaDoc for the
 * class {@code WordCount} in the core module.
 * <p>
 * {@link ReadHdfsP} is a processor that can be used for reading from HDFS
 * given a {@code JobConf} with input paths and input formats. The files in the input
 * folder will be split among Jet processors, using {@code InputSplit}s.
 * <p>
 * {@link WriteHdfsP} writes the output to the given output path, with each processor
 * writing to a single file within the path. The files are identified by the member ID
 * and the local ID of the processor writing. Unlike in MapReduce, the output files are not
 * sorted by key.
 * <p>
 * In this example, files are read from and written to using {@code TextInputFormat} and
 * {@code TextOutputFormat} respectively, but the example can be adjusted to be used with
 * any input/output format.
 */
public class HadoopWordCount {

    private static final String OUTPUT_PATH = "hadoop-word-count";

    public static void main(String[] args) throws Exception {

        Path inputPath = new Path(HadoopWordCount.class.getClassLoader().getResource("books").getPath());
        Path outputPath = new Path(OUTPUT_PATH);

        // set up the Hadoop job config, the input and output paths and formats
        JobConf jobConfig = new JobConf();
        jobConfig.setInputFormat(TextInputFormat.class);
        jobConfig.setOutputFormat(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(jobConfig, outputPath);
        TextInputFormat.addInputPath(jobConfig, inputPath);

        // Delete the output directory, if already exists
        FileSystem.get(new Configuration()).delete(outputPath, true);

        JetConfig cfg = new JetConfig();
        cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                Math.max(1, getRuntime().availableProcessors() / 2)));

        JetInstance jetInstance = Jet.newJetInstance(cfg);
        Jet.newJetInstance(cfg);

        try {
            System.out.print("\nCounting words from " + inputPath);
            long start = nanoTime();
            jetInstance.newJob(buildDag(jobConfig)).execute().get();
            System.out.print("Done in " + NANOSECONDS.toMillis(nanoTime() - start) + " milliseconds.");
            System.out.println("Output written to " + outputPath);
        } finally {
            Jet.shutdownAll();
        }
    }

    @Nonnull
    private static DAG buildDag(JobConf jobConf) {
        final Pattern delimiter = Pattern.compile("\\W+");
        final Distributed.Supplier<Long> initialZero = () -> 0L;

        DAG dag = new DAG();

        // read line number and line, and ignore the line number
        Vertex source = dag.newVertex("source", readHdfs(jobConf, (k, v) -> v.toString()));

        // line -> words
        Vertex tokenize = dag.newVertex("tokenize",
                flatMap((String line) -> traverseArray(delimiter.split(line.toLowerCase()))
                        .filter(word -> !word.isEmpty()))
        );

        // word -> (word, count)
        Vertex reduce = dag.newVertex("reduce",
                groupAndAccumulate(initialZero, (count, x) -> count + 1)
        );

        // (word, count) -> (word, count)
        Vertex combine = dag.newVertex("combine",
                groupAndAccumulate(Map.Entry::getKey, initialZero,
                        (Long count, Map.Entry<String, Long> wordAndCount) -> count + wordAndCount.getValue())
        );

        Vertex sink = dag.newVertex("sink", WriteHdfsP.writeHdfs(jobConf));

        return dag.edge(between(source, tokenize))
                  .edge(between(tokenize, reduce)
                          .partitioned(wholeItem(), HASH_CODE))
                  .edge(between(reduce, combine)
                          .distributed()
                          .partitioned(entryKey()))
                  .edge(between(combine, sink));
    }
}
