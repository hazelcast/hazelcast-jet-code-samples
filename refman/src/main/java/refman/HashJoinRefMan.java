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

package refman;

import com.hazelcast.jet.ComputeStage;
import com.hazelcast.jet.HashJoinBuilder;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sources;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import refman.datamodel.hashjoin.Broker;
import refman.datamodel.hashjoin.Market;
import refman.datamodel.hashjoin.Product;
import refman.datamodel.hashjoin.Trade;

import java.util.Map.Entry;

import static com.hazelcast.jet.JoinClause.joinMapEntries;

//CHECKSTYLE:OFF
public class HashJoinRefMan {
    static void hashJoinDirect() {
        Pipeline p = Pipeline.create();

        // The primary stream: trades
        ComputeStage<Trade> trades = p.drawFrom(Sources.<Trade>readList("trades"));

        // The enriching streams: products and brokers
        ComputeStage<Entry<Integer, Product>> prodEntries =
                p.drawFrom(Sources.<Integer, Product>readMap("products"));
        ComputeStage<Entry<Integer, Broker>> brokEntries =
                p.drawFrom(Sources.<Integer, Broker>readMap("brokers"));

        // Join the trade stream with the product and broker streams
        ComputeStage<Tuple3<Trade, Product, Broker>> joined = trades.hashJoin(
                prodEntries, joinMapEntries(Trade::productId),
                brokEntries, joinMapEntries(Trade::brokerId)
        );
    }

    static void hashJoinBuild() {
        Pipeline p = Pipeline.create();

        // The primary stream: trades
        ComputeStage<Trade> trades = p.drawFrom(Sources.<Trade>readList("trades"));

        // The enriching streams: products and brokers
        ComputeStage<Entry<Integer, Product>> prodEntries =
                p.drawFrom(Sources.<Integer, Product>readMap("products"));
        ComputeStage<Entry<Integer, Broker>> brokEntries =
                p.drawFrom(Sources.<Integer, Broker>readMap("brokers"));
        ComputeStage<Entry<Integer, Market>> marketEntries =
                p.drawFrom(Sources.<Integer, Market>readMap("markets"));

        HashJoinBuilder<Trade> b = trades.hashJoinBuilder();
        Tag<Product> prodTag = b.add(prodEntries, joinMapEntries(Trade::productId));
        Tag<Broker> brokTag = b.add(brokEntries, joinMapEntries(Trade::brokerId));
        Tag<Market> marketTag = b.add(marketEntries, joinMapEntries(Trade::marketId));
        ComputeStage<Tuple2<Trade, ItemsByTag>> joined = b.build();

        ComputeStage<String> mapped = joined.map(
                (Tuple2<Trade, ItemsByTag> t) -> {
                    Trade trade = t.f0();
                    ItemsByTag ibt = t.f1();
                    Product product = ibt.get(prodTag);
                    Broker broker = ibt.get(brokTag);
                    Market market = ibt.get(marketTag);
                    return trade + ": " + product + ", " + broker + ", " + market;
                });
    }
}
