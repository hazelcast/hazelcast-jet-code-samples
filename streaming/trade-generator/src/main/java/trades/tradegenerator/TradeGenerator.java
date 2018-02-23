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

package trades.tradegenerator;

import com.hazelcast.core.IMap;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.locks.LockSupport;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;

public class TradeGenerator {

    private static final int MAX_LAG = 1000;
    private static final int QUANTITY = 100;

    public static void generate(int numTickers, IMap<Long, Trade> map, int tradesPerSec, long timeoutSeconds) {
        List<String> tickers = loadTickers(numTickers);
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long startTime = System.currentTimeMillis();
        long endTime = startTime + SECONDS.toMillis(timeoutSeconds);
        long now;
        long numTrades = 0;

        Map<Long, Trade> tmpMap = new HashMap<>();
        while ((now = System.currentTimeMillis()) < endTime) {
            long expectedTrades = (now - startTime) * tradesPerSec / 1000;
            for (int i = 0; i < 10_000 && numTrades < expectedTrades; numTrades++, i++) {
                String ticker = tickers.get(rnd.nextInt(tickers.size()));
                long tradeTime = now - rnd.nextLong(MAX_LAG);
                Trade trade = new Trade(tradeTime, ticker, QUANTITY, rnd.nextInt(5000));
                tmpMap.put(numTrades, trade);
            }
            map.putAll(tmpMap);
            tmpMap.clear();
            LockSupport.parkNanos(1_000_000); // 1ms
        }
    }

    private static List<String> loadTickers(long numTickers) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                TradeGenerator.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))
        ) {
            return reader.lines()
                  .skip(1)
                  .limit(numTickers)
                  .map(l -> l.split("\\|")[0])
                  .collect(toList());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
