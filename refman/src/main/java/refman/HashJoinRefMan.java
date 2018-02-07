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

package refman;

import com.hazelcast.jet.pipeline.BatchStage;
import com.hazelcast.jet.pipeline.HashJoinBuilder;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.datamodel.Tuple3;
import refman.datamodel.hashjoin.Broker;
import refman.datamodel.hashjoin.Market;
import refman.datamodel.hashjoin.Product;
import refman.datamodel.hashjoin.Trade;

import java.util.Map.Entry;

import static com.hazelcast.jet.datamodel.Tuple2.tuple2;
import static com.hazelcast.jet.datamodel.Tuple3.tuple3;
import static com.hazelcast.jet.pipeline.JoinClause.joinMapEntries;

//CHECKSTYLE:OFF
public class HashJoinRefMan {
    static void hashJoinDirect() {
        Pipeline p = Pipeline.create();

        // The primary stream: trades
        BatchStage<Trade> trades = p.drawFrom(Sources.<Trade>list("trades"));

        // The enriching streams: products and brokers
        BatchStage<Entry<Integer, Product>> prodEntries =
                p.drawFrom(Sources.<Integer, Product>map("products"));
        BatchStage<Entry<Integer, Broker>> brokEntries =
                p.drawFrom(Sources.<Integer, Broker>map("brokers"));

        // Join the trade stream with the product and broker streams
        BatchStage<String> joined = trades.hashJoin(
                prodEntries, joinMapEntries(Trade::productId),
                brokEntries, joinMapEntries(Trade::brokerId),
                (trade, product, broker) -> trade + ": " + product + ", " + broker
        );
    }

    static void hashJoinBuild() {
        Pipeline p = Pipeline.create();

        // The primary stream: trades
        BatchStage<Trade> trades = p.drawFrom(Sources.<Trade>list("trades"));

        // The enriching streams: products and brokers
        BatchStage<Entry<Integer, Product>> prodEntries = p.drawFrom(Sources.<Integer, Product>map("products"));
        BatchStage<Entry<Integer, Broker>> brokEntries = p.drawFrom(Sources.<Integer, Broker>map("brokers"));
        BatchStage<Entry<Integer, Market>> marketEntries = p.drawFrom(Sources.<Integer, Market>map("markets"));

        HashJoinBuilder<Trade> b = trades.hashJoinBuilder();
        Tag<Product> prodTag = b.add(prodEntries, joinMapEntries(Trade::productId));
        Tag<Broker> brokTag = b.add(brokEntries, joinMapEntries(Trade::brokerId));
        Tag<Market> marketTag = b.add(marketEntries, joinMapEntries(Trade::marketId));
        BatchStage<String> joined = b.build((trade, ibt) -> {
            Product product = ibt.get(prodTag);
            Broker broker = ibt.get(brokTag);
            Market market = ibt.get(marketTag);
            return trade + ": " + product + ", " + broker + ", " + market;
        });
    }
}
