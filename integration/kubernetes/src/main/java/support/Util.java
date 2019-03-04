/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.JetInstance;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.Comparator.reverseOrder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Contains utility methods for starting/stopping console printer threads
 */
public final class Util {

    private static volatile boolean running = true;
    private static final long PRINT_INTERNAL_MILLIS = 10_000L;

    private Util() {
    }

    public static void startConsolePrinterThread(JetInstance jet, String mapName) {
        new Thread(() -> {
            Map<String, Long> volumes = jet.getMap(mapName);
            while (running) {
                List<Entry<String, Long>> top5 = volumes.entrySet()
                                                        .stream()
                                                        .sorted(comparing(Entry::getValue, reverseOrder()))
                                                        .limit(5)
                                                        .collect(Collectors.toList());
                if (top5.isEmpty()) {
                    continue;
                }

                System.out.println("\n");
                System.out.println("/----------+--------------\\");
                System.out.println("|       Top 5 Volumes     |");
                System.out.println("| Ticker   | Value        |");
                System.out.println("|----------+--------------|");
                top5.forEach((entry) ->
                        System.out.format("| %1$-8s | %2$-13s|%n",
                                entry.getKey(), entry.getValue()));
                System.out.println("\\---------+---------------/");
                LockSupport.parkNanos(MILLISECONDS.toNanos(PRINT_INTERNAL_MILLIS));
            }
        }).start();
    }

    public static void stopConsolePrinterThread() {
        running = false;
    }

}
