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

import com.hazelcast.jet.datamodel.TimestampedItem;
import io.undertow.Undertow;
import io.undertow.server.HttpServerExchange;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

import static io.undertow.util.Headers.CONTENT_TYPE;
import static java.lang.Runtime.getRuntime;

/**
 * Starts a thread that records a time series of used JVM heap memory.
 * Starts an HTTP server that delivers these results. The HTTP response
 * consists of one timestamped measurement per line. The server delivers
 * the data accumulated since the last request and then forgets it.
 */
public class SystemMonitorHttpService {
    private final Runtime runtime = getRuntime();
    private final Queue<TimestampedItem<Long>> q = new ConcurrentLinkedDeque<>();

    {
        Thread t = new Thread(() -> {
            while (true) {
                long monitoredValue = runtime.totalMemory() - runtime.freeMemory();
                q.add(new TimestampedItem<>(System.currentTimeMillis(), monitoredValue));
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    return;
                }
            }
        });
        t.setName("mock-system-monitor");
        t.setDaemon(true);
        t.start();
    }

    public Undertow httpServer() {
        return Undertow.builder()
                       .addHttpListener(8008, "localhost")
                       .setHandler(this::handleRequest)
                       .build();
    }

    private void handleRequest(HttpServerExchange exchange) {
        exchange.getResponseHeaders().put(CONTENT_TYPE, "text/plain");
        if (q.isEmpty()) {
            return;
        }
        StringBuilder b = new StringBuilder();
        for (int i = q.size(); i > 0; i--) {
            TimestampedItem<Long> event = q.remove();
            b.append(event.timestamp()).append(' ')
             .append(event.item()).append('\n');
        }
        exchange.getResponseSender().send(b.toString());
    }
}
