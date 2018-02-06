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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.accumulator.LongAccumulator;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.aggregate.AggregateOperations;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.TimestampKind;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.core.WindowDefinition;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.nio.IOUtil;

import java.io.BufferedWriter;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.WatermarkEmissionPolicy.emitByFrame;
import static com.hazelcast.jet.core.WatermarkGenerationParams.wmGenParams;
import static com.hazelcast.jet.core.WatermarkPolicies.withFixedLag;
import static com.hazelcast.jet.core.processor.Processors.accumulateByFrameP;
import static com.hazelcast.jet.core.processor.Processors.combineToSlidingWindowP;
import static com.hazelcast.jet.core.processor.Processors.filterP;
import static com.hazelcast.jet.core.processor.Processors.insertWatermarksP;
import static com.hazelcast.jet.core.processor.Processors.mapP;
import static com.hazelcast.jet.core.processor.SourceProcessors.streamFilesP;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Analyzes access log files from a HTTP server. Demonstrates the usage of
 * {@link SourceProcessors#streamFilesP(String, Charset, String)} to read
 * files line by line in streaming fashion - by running indefinitely and
 * watching for changes as they appear.
 * <p>
 * It uses sliding window aggregation to output frequency of visits to each
 * page continuously.
 * <p>
 * This analyzer could be run on a Jet cluster deployed on the same
 * machines as those forming the web server cluster. This way each instance
 * will process local files locally and subsequently merge the results
 * globally.
 * <p>
 * This sample does not work well on Windows. On Windows, even though new
 * lines are flushed, WatchService is not notified of changes, until the
 * file is closed.
 */
public class AccessStreamAnalyzer {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        Path tempDir = Files.createTempDirectory(AccessStreamAnalyzer.class.getSimpleName());

        WindowDefinition wDef = WindowDefinition.slidingWindowDef(10_000, 1_000);
        AggregateOperation1<Object, LongAccumulator, Long> aggrOper = AggregateOperations.counting();

        DAG dag = new DAG();
        // use localParallelism=1 as to have just one thread watching the directory and reading the files
        Vertex streamFiles = dag.newVertex("streamFiles", streamFilesP(tempDir.toString(), UTF_8, "*"))
                .localParallelism(1);
        Vertex parseLine = dag.newVertex("parseLine", mapP(LogLine::parse));
        Vertex removeUnsuccessful = dag.newVertex("removeUnsuccessful", filterP(
                (LogLine line) -> line.getResponseCode() >= 200 && line.getResponseCode() < 400));
        Vertex insertWatermarks = dag.newVertex("insertWatermarks", insertWatermarksP(wmGenParams(
                LogLine::getTimestamp, withFixedLag(100), emitByFrame(wDef), 30000L
        )));
        Vertex slidingWindowStage1 = dag.newVertex("slidingWindowStage1",
                accumulateByFrameP(
                        LogLine::getEndpoint,
                        LogLine::getTimestamp, TimestampKind.EVENT,
                        wDef,
                        aggrOper));
        Vertex slidingWindowStage2 = dag.newVertex("slidingWindowStage2", combineToSlidingWindowP(wDef, aggrOper));
        // output to logger (to console) - good just for the demo. Part of the output will be on each node.
        Vertex writeLoggerP = dag.newVertex("writeLoggerP", DiagnosticProcessors.writeLoggerP()).localParallelism(1);

        dag.edge(between(streamFiles, parseLine).isolated())
           .edge(between(parseLine, removeUnsuccessful).isolated())
           .edge(between(removeUnsuccessful, insertWatermarks).isolated())
           .edge(between(insertWatermarks, slidingWindowStage1)
                   .partitioned(identity()))
           .edge(between(slidingWindowStage1, slidingWindowStage2)
                   .partitioned(entryKey())
                   .distributed())
           .edge(between(slidingWindowStage2, writeLoggerP));

        JetInstance instance = Jet.newJetInstance();
        try {
            instance.newJob(dag);
            // job is running in its own threads. Let's generate some random traffic in this thread.
            startGenerator(tempDir);
            // wait for all writes to be picked up
            LockSupport.parkNanos(SECONDS.toNanos(1));
        } finally {
            Jet.shutdownAll();
            IOUtil.delete(tempDir.toFile());
        }
    }

    private static void startGenerator(Path tempDir) throws Exception {
        Random random = new Random();
        try (BufferedWriter w = Files.newBufferedWriter(tempDir.resolve("access_log"), StandardOpenOption.CREATE)) {
            for (int i = 0; i < 60_000; i++) {
                int articleNum = Math.min(10, Math.max(0, (int) (random.nextGaussian() * 2 + 5)));
                w.append("129.21.37.3 - - [")
                 .append(LogLine.DATE_TIME_FORMATTER.format(ZonedDateTime.now()))
                 .append("] \"GET /article")
                 .append(String.valueOf(articleNum))
                 .append(" HTTP/1.0\" 200 12345");

                w.newLine();
                w.flush();
                LockSupport.parkNanos(MILLISECONDS.toNanos(1));
            }
        }
    }

    /**
     * Immutable data transfer object mapping the log line.
     */
    private static class LogLine implements Serializable {

        public static final DateTimeFormatter DATE_TIME_FORMATTER =
                DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);

        // Example Apache log line:
        //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
        private static final String LOG_ENTRY_PATTERN =
                // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
                "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
        private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

        private final String ipAddress;
        private final String clientIdentd;
        private final String userID;
        private final long timestamp;
        private final String method;
        private final String endpoint;
        private final String protocol;
        private final int responseCode;
        private final long contentSize;

        LogLine(String ipAddress, String clientIdentd, String userID, long timestamp, String method, String endpoint,
                String protocol, int responseCode, long contentSize) {
            this.ipAddress = ipAddress;
            this.clientIdentd = clientIdentd;
            this.userID = userID;
            this.timestamp = timestamp;
            this.method = method;
            this.endpoint = endpoint;
            this.protocol = protocol;
            this.responseCode = responseCode;
            this.contentSize = contentSize;
        }

        public static LogLine parse(String line) {
            Matcher m = PATTERN.matcher(line);
            if (!m.find()) {
                throw new IllegalArgumentException("Cannot parse log line: " + line);
            }
            long time = ZonedDateTime.parse(m.group(4), DATE_TIME_FORMATTER).toInstant().toEpochMilli();
            return new LogLine(m.group(1), m.group(2), m.group(3), time, m.group(5), m.group(6), m.group(7),
                    Integer.parseInt(m.group(8)), Long.parseLong(m.group(9)));
        }

        public String getIpAddress() {
            return ipAddress;
        }

        public String getClientIdentd() {
            return clientIdentd;
        }

        public String getUserID() {
            return userID;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public String getMethod() {
            return method;
        }

        public String getEndpoint() {
            return endpoint;
        }

        public String getProtocol() {
            return protocol;
        }

        public int getResponseCode() {
            return responseCode;
        }

        public long getContentSize() {
            return contentSize;
        }
    }
}
