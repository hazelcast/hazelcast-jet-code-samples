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

package trades;

import com.hazelcast.core.IList;

import java.io.Serializable;

/**
 * DTO for a trade.
 */
public final class Trade implements Serializable {
    private final long time;
    private final String ticker;
    private final int quantity;
    private final int price; // in cents

    public Trade(long time, String ticker, int quantity, int price) {
        this.time = time;
        this.ticker = ticker;
        this.quantity = quantity;
        this.price = price;
    }

    public long getTime() {
        return time;
    }

    public String getTicker() {
        return ticker;
    }

    public int getQuantity() {
        return quantity;
    }

    public int getPrice() {
        return price;
    }

    public static void populateTrades(IList<Trade> trades) {
        trades.add(new Trade(1, "AAAP", 1, 1));
        trades.add(new Trade(1, "BABY", 1, 1));
        trades.add(new Trade(1, "CA", 1, 1));
        trades.add(new Trade(2, "AAAP", 1, 1));
        trades.add(new Trade(2, "BABY", 1, 1));
        trades.add(new Trade(2, "CA", 1, 1));
        trades.add(new Trade(3, "AAAP", 1, 1));
        trades.add(new Trade(3, "BABY", 1, 1));
        trades.add(new Trade(3, "CA", 1, 1));
    }

    @Override
    public String toString() {
        return "Trade{time=" + time + ", ticker='" + ticker + '\'' + ", quantity=" + quantity + ", price=" + price + '}';
    }
}
