/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.processor.HdfsProcessors;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static java.lang.Runtime.getRuntime;
import static java.lang.System.nanoTime;
import static java.util.Comparator.comparingLong;

/**
 * Demonstrates how to use hdfs processor as a source
 */
public class WordCountFromHDFS {

    private static final String OUTPUT_PATH = "hadoop-word-count-jus";

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        Path inputPath = new Path(WordCountFromHDFS.class.getClassLoader().getResource("books").getPath());
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
            final Pattern delimiter = Pattern.compile("\\W+");
            IMap<String, Long> counts = DistributedStream
                    .<String>fromSource(jetInstance, HdfsProcessors.readHdfsP(jobConfig, (k, v) -> v.toString()))
                    .flatMap(line -> Arrays.stream(delimiter.split(line.toLowerCase())))
                    .filter(word -> !word.isEmpty())
                    .collect(DistributedCollectors.toIMap("counts", w -> w, w -> 1L, (left, right) -> left + right));
            System.out.print("done in " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) + " milliseconds.");
            printResults(counts);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static void printResults(Map<String, Long> counts) {
        final int limit = 100;
        System.out.format(" Top %d entries are:%n", limit);
        System.out.println("/-------+---------\\");
        System.out.println("| Count | Word    |");
        System.out.println("|-------+---------|");
        counts.entrySet().stream()
              .sorted(comparingLong(Map.Entry<String, Long>::getValue).reversed())
              .limit(limit)
              .forEach(e -> System.out.format("|%6d | %-8s|%n", e.getValue(), e.getKey()));
        System.out.println("\\-------+---------/");
    }
}
