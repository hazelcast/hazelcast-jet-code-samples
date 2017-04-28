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

package com.hazelcast.jet.sample;

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Distributed;
import com.hazelcast.jet.Processor;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

import static com.hazelcast.jet.impl.util.Util.memoize;
import static com.hazelcast.util.Preconditions.checkTrue;
import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

/**
 * Generates simulated stock market traffic. Items represent
 * trading events.
 */
public final class GenerateTradesP extends AbstractProcessor {

    public static final int MAX_LAG = 1000;

    private static final int MAX_TRADES_PER_SEC = 4_000_000;
    private static final int QUANTITY = 100;

    private final Map<String, Integer> tickerToPrice = new HashMap<>();
    private final Supplier<String[]> tickerStore =
            memoize(() -> tickerToPrice.keySet().stream().toArray(String[]::new));

    private final long periodNanos;
    private long nextSchedule;

    private GenerateTradesP(long periodNanos) {
        setCooperative(false);
        this.periodNanos = periodNanos;
    }

    public static Distributed.Supplier<Processor> generateTrades(double tradesPerSec) {
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
        tickerToPrice.put(initial.getKey(), initial.getValue());
        return true;
    }

    @Override
    public boolean complete() {
        String[] tickers = tickerStore.get();
        if (tickers.length == 0) {
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
        for (; nextSchedule <= now; nextSchedule += periodNanos) {
            String ticker = tickers[rnd.nextInt(tickers.length)];
            emit(new Trade(
                    timestamp - rnd.nextLong(MAX_LAG),
                    ticker, QUANTITY, tickerToPrice.get(ticker)));
        }
        return false;
    }
}
