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
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.sample.Trade;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.WindowDefinition;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.Processors.writeFile;
import static com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP.readMap;
import static com.hazelcast.jet.sample.GenerateTradesP.MAX_LAG;
import static com.hazelcast.jet.sample.GenerateTradesP.generateTrades;
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLagAndRetention;
import static com.hazelcast.jet.windowing.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.windowing.WindowOperations.counting;
import static com.hazelcast.jet.windowing.WindowingProcessors.groupByFrame;
import static com.hazelcast.jet.windowing.WindowingProcessors.insertPunctuation;
import static com.hazelcast.jet.windowing.WindowingProcessors.slidingWindow;

/**
 * A simple demonstration of Jet's continuous operators on an infinite
 * stream. Initially a Hazelcast map is populated with some stock
 * ticker names; the job reads the map and feeds the data to the vertex
 * that simulates an event stream coming from a stock market. Jet
 * computes the number of trades per ticker within a sliding window
 * of a given duration and dumps the results to a set of files.
 * <pre>
 *             ---------------
 *            | ticker-source |
 *             ---------------
 *                    |
 *         (ticker, initialPrice)
 *                    V
 *            -----------------
 *           | generate-trades |
 *            -----------------
 *                    |
 *    (timestamp, ticker, quantity, price)
 *                    V
 *          --------------------
 *         | insert-punctuation |
 *          --------------------
 *                    |
 *                    V
 *            ----------------
 *           | group-by-frame |
 *            ----------------
 *                    |
 *          (ticker, time, count)
 *                    V
 *            ----------------
 *           | sliding-window |
 *            ----------------
 *                    |
 *          (ticker, time, count)
 *                    V
 *            ---------------
 *           | format-output |
 *            ---------------
 *                    |
 *          "time ticker count"
 *                    V
 *                 ------
 *                | sink |
 *                 ------
 * </pre>
 */
public class StockExchange {

    private static final String TICKER_MAP_NAME = "tickers";
    private static final String OUTPUT_DIR_NAME = "stock-exchange";
    private static final int SLIDING_WINDOW_LENGTH_MILLIS = 1000;
    private static final int SLIDE_STEP_MILLIS = 10;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();
        try {
            loadTickers(jet);
            jet.newJob(buildDag()).execute();
            Thread.sleep(10_000);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag() {
        DAG dag = new DAG();
        WindowDefinition windowDef = slidingWindowDef(SLIDING_WINDOW_LENGTH_MILLIS, SLIDE_STEP_MILLIS);
        int tradesPerSecPerMember = 1_000_000;
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

        Vertex tickerSource = dag.newVertex("ticker-source", readMap(TICKER_MAP_NAME));
        Vertex generateTrades = dag.newVertex("generate-trades", generateTrades(tradesPerSecPerMember));
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertPunctuation(Trade::getTime, () -> cappingEventSeqLagAndRetention(MAX_LAG, 100)
                        .throttleByFrame(windowDef)));
        Vertex groupByFrame = dag.newVertex("group-by-frame",
                groupByFrame(Trade::getTicker, Trade::getTime, windowDef, counting()));
        Vertex slidingWin = dag.newVertex("sliding-window", slidingWindow(windowDef, counting()));
        Vertex formatOutput = dag.newVertex("format-output",
                map((Frame<String, Long> f) -> String.format("%s %5s %4d",
                        timeFormat.format(Instant.ofEpochMilli(f.getSeq()).atZone(ZoneId.systemDefault())),
                        f.getKey(), f.getValue()
                )));
        Vertex sink = dag.newVertex("sink", writeFile(Paths.get(OUTPUT_DIR_NAME).toString()));

        tickerSource.localParallelism(1);
        generateTrades.localParallelism(1);
        dag
                .edge(between(tickerSource, generateTrades).oneToMany())
                .edge(between(generateTrades, insertPunctuation).oneToMany())
                .edge(between(insertPunctuation, groupByFrame).partitioned(Trade::getTicker, HASH_CODE))
                .edge(between(groupByFrame, slidingWin).partitioned(Frame<Object, Object>::getKey)
                                                 .distributed())
                .edge(between(slidingWin, formatOutput).oneToMany())
                .edge(between(formatOutput, sink).oneToMany());
        return dag;
    }

    private static void loadTickers(JetInstance jet) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                StockExchange.class.getResourceAsStream("/nasdaqlisted.txt")))
        ) {
            Map<String, Integer> tickers = jet.getMap(TICKER_MAP_NAME);
            reader.lines()
                  .skip(1)
                  .limit(100)
                  .map(l -> l.split("\\|")[0])
                  .forEach(t -> tickers.put(t, 0));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
