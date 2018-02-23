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

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.AbstractProcessor;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.function.DistributedSupplier;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * Generates simulated stock market traffic. Items represent
 * trading events.
 */
@Deprecated
public final class GenerateTradesP extends AbstractProcessor {

    public static final String TICKER_MAP_NAME = "tickers";

    // A simple but dirty hack to determine the throughput of the sample
    // Jet job. This works only in a demo setting, where all Jet instances
    // are created on the same JVM that creates and submits the job.
    // For production code,
    //
    //          DO NOT USE MUTABLE STATIC STATE IN PROCESSORS.
    //
    public static final AtomicLong TOTAL_EVENT_COUNT = new AtomicLong();

    public static final int MAX_LAG = 1000;
    private static final int MAX_TRADES_PER_SEC = 4_000_000;
    private static final int QUANTITY = 100;

    private final List<String> tickers = new ArrayList<>();

    private final long periodNanos;
    private long nextSchedule;

    private GenerateTradesP(long periodNanos) {
        setCooperative(false);
        this.periodNanos = periodNanos;
    }

    public static DistributedSupplier<Processor> generateTradesP(double tradesPerSec) {
        checkTrue(tradesPerSec >= 1, "tradesPerSec must be at least 1");
        checkTrue(tradesPerSec <= MAX_TRADES_PER_SEC, "tradesPerSec can be at most " + MAX_TRADES_PER_SEC);
        return () -> new GenerateTradesP((long) (SECONDS.toNanos(1) / tradesPerSec));
    }

    @Override
    protected void init(@Nonnull Context context) {
        nextSchedule = System.nanoTime() + periodNanos;
    }

    @Override
    protected boolean tryProcess(int ordinal, @Nonnull Object item) {
        Map.Entry<String, Integer> initial = (Entry) item;
        tickers.add(initial.getKey());
        return true;
    }

    @Override
    public boolean complete() {
        if (tickers.isEmpty()) {
            return true;
        }
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        long now = nanoTime();
        for (; now < nextSchedule; now = nanoTime()) {
            if (now < nextSchedule - MILLISECONDS.toNanos(1)) {
                parkNanos(MICROSECONDS.toNanos(100));
            }
            if (now < nextSchedule - MICROSECONDS.toNanos(50)) {
                parkNanos(1);
            }
        }
        long timestamp = currentTimeMillis();
        long count = 0;
        for (; nextSchedule <= now; nextSchedule += periodNanos, count++) {
            String ticker = tickers.get(rnd.nextInt(tickers.size()));
            if (!tryEmit(new Trade(
                    timestamp - rnd.nextLong(MAX_LAG),
                    ticker, QUANTITY, rnd.nextInt(5000)))) {
                break;
            }
        }
        TOTAL_EVENT_COUNT.addAndGet(count);
        return false;
    }

    public static void loadTickers(JetInstance jet, long numTickers) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                GenerateTradesP.class.getResourceAsStream("/nasdaqlisted.txt"), UTF_8))
        ) {
            Map<String, Integer> tickers = jet.getMap(TICKER_MAP_NAME);
            reader.lines()
                  .skip(1)
                  .limit(numTickers)
                  .map(l -> l.split("\\|")[0])
                  .forEach(t -> tickers.put(t, 0));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
