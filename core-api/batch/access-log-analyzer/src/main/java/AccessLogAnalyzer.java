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
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.processor.Processors;
import com.hazelcast.jet.processor.SinkProcessors;
import com.hazelcast.jet.processor.SourceProcessors;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.aggregate.AggregateOperations.counting;
import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.function.DistributedFunctions.entryKey;
import static com.hazelcast.jet.function.DistributedFunctions.wholeItem;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Analyzes access log files from a HTTP server. Demonstrates the usage of
 * {@link SourceProcessors#readFiles(String, Charset, String)} to read files line by
 * line and writing results to another file using
 * {@link SinkProcessors#writeFile(String)}.
 * <p>
 * Also demonstrates custom {@link Traverser} implementation in {@link
 * #explodeSubPaths(LogLine)}.
 * <p>
 * The reduce+combine pair is the same as in WordCount sample: allows local
 * counting first then combines partial counts globally.
 * <p>
 * This analyzer could be run on a Jet cluster deployed on the same machines
 * as those forming the web server cluster. This way each instance will process
 * local files locally and subsequently merge the results globally. Note that
 * one output file will be on each member of the cluster, containing part of
 * the keys. For real-life scenario, different sink should be used.
 * <p>
 * Example data are in {@code {module.dir}/data/access_log.processed}.
 */
public class AccessLogAnalyzer {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        if (args.length != 2) {
            System.err.println("Usage:");
            System.err.println("  " + AccessLogAnalyzer.class.getSimpleName() + " <sourceDir> <targetDir>");
            System.exit(1);
        }
        final String sourceDir = args[0];
        final String targetDir = args[1];

        DAG dag = new DAG();
        Vertex readFiles = dag.newVertex("readFiles", SourceProcessors.readFiles(sourceDir, UTF_8, "*"));
        Vertex parseLine = dag.newVertex("parseLine", Processors.map(LogLine::parse));
        Vertex filterUnsuccessful = dag.newVertex("filterUnsuccessful",
                Processors.filter((LogLine log) -> log.getResponseCode() >= 200 && log.getResponseCode() < 400));
        Vertex explodeSubPaths = dag.newVertex("explodeSubPaths",
                Processors.flatMap(AccessLogAnalyzer::explodeSubPaths));
        Vertex accumulate = dag.newVertex("accumulate", Processors.accumulateByKey(wholeItem(), counting()));
        Vertex combine = dag.newVertex("combine", Processors.combineByKey(counting()));
        // we use localParallelism=1 to have one file per Jet node
        Vertex writeFile = dag.newVertex("writeFile", SinkProcessors.writeFile(targetDir)).localParallelism(1);

        dag.edge(between(readFiles, parseLine).isolated())
           .edge(between(parseLine, filterUnsuccessful).isolated())
           .edge(between(filterUnsuccessful, explodeSubPaths).isolated())
           .edge(between(explodeSubPaths, accumulate)
                   .partitioned(identity()))
           .edge(between(accumulate, combine)
                   .partitioned(entryKey())
                   .distributed())
           .edge(between(combine, writeFile));

        JetInstance instance = Jet.newJetInstance();
        try {
            instance.newJob(dag).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Explodes a string e.g. {@code "/path/to/file"} to following sequence:
     * <pre>
     *  {
     *     "/path/",
     *     "/path/to/",
     *     "/path/to/file"
     *  }
     * </pre>
     */
    private static Traverser<String> explodeSubPaths(LogLine logLine) {
        // remove the query string after "?"
        int qmarkPos = logLine.getEndpoint().indexOf('?');
        String endpoint = qmarkPos < 0 ? logLine.getEndpoint() : logLine.getEndpoint().substring(0, qmarkPos);

        return new Traverser<String>() {
            int pos;

            @Override
            public String next() {
                if (pos < 0) {
                    return null; // we're done, return null terminator
                }
                int pos1 = endpoint.indexOf('/', pos + 1);
                try {
                    return pos1 < 0 ? endpoint : endpoint.substring(0, pos1 + 1);
                } finally {
                    pos = pos1;
                }
            }
        };
    }

    /**
     * Immutable data transfer object mapping the log line.
     */
    private static class LogLine implements Serializable {
        // Example Apache log line:
        //   127.0.0.1 - - [21/Jul/2014:9:55:27 -0800] "GET /home.html HTTP/1.1" 200 2048
        private static final String LOG_ENTRY_PATTERN =
                // 1:IP  2:client 3:user 4:date time                   5:method 6:req 7:proto   8:respcode 9:size
                "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)";
        private static final Pattern PATTERN = Pattern.compile(LOG_ENTRY_PATTERN);

        private static final DateTimeFormatter DATE_TIME_FORMATTER =
                DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z", Locale.US);

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
