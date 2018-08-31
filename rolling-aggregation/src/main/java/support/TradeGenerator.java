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

package support;

import com.hazelcast.core.IMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class TradeGenerator {

    private static final int MAX_LAG = 1000;
    private static final long NASDAQLISTED_ROWCOUNT = 3170;

    public static void generate(int numTickers, IMap<Long, Trade> map, int tradesPerSec) {
        List<String> tickers = loadTickers(numTickers);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long startTime = System.currentTimeMillis();
        long numTrades = 0;

        Map<Long, Trade> tmpMap = new HashMap<>();
        while (true) {
            long now = System.currentTimeMillis();
            long expectedTrades = (now - startTime) * tradesPerSec / 1000;
            for (int i = 0; i < 10_000 && numTrades < expectedTrades; numTrades++, i++) {
                String ticker = tickers.get(rnd.nextInt(tickers.size()));
                long tradeTime = now - rnd.nextLong(MAX_LAG);
                Trade trade = new Trade(tradeTime, ticker, 1, rnd.nextInt(5000));
                tmpMap.put(numTrades, trade);
            }
            map.putAll(tmpMap);
            tmpMap.clear();
            LockSupport.parkNanos(MILLISECONDS.toNanos(30));
        }
    }

    private static List<String> loadTickers(long numTickers) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TradeGenerator.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))
        ) {
            final List<String> result = new ArrayList<>();
            final long strideLength = NASDAQLISTED_ROWCOUNT / numTickers;
            int rowCount = 0;
            for (String line; (line = reader.readLine()) != null; ) {
                if (++rowCount % strideLength != 0) {
                    continue;
                }
                result.add(line.substring(0, line.indexOf('|')));
                if (result.size() == numTickers) {
                    break;
                }
            }
            return result;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
