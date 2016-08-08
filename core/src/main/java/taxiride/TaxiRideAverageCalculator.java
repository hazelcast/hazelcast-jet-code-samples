/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package taxiride;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.JetPair;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.processor.ContainerProcessor;

import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;

/**
 * Processor that matches a start and end TaxiRideEvent, and emits the average speed for the ride.
 */
public class TaxiRideAverageCalculator implements ContainerProcessor<TaxiRideEvent, Pair<Long, Float>> {

    private Map<Long, TaxiRideEvent> rides = new HashMap<>();

    @Override
    public boolean process(ProducerInputStream<TaxiRideEvent> inputStream,
                           ConsumerOutputStream<Pair<Long, Float>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {

        for (TaxiRideEvent taxiRideEvent : inputStream) {
            if (rides.containsKey(taxiRideEvent.rideId)) {

                // we received the second element. Compute the speed.
                TaxiRideEvent startEvent = taxiRideEvent.isStart ? taxiRideEvent : rides.get(taxiRideEvent.rideId);
                TaxiRideEvent endEvent = taxiRideEvent.isStart ? rides.get(taxiRideEvent.rideId) : taxiRideEvent;


                long timeDiff = ChronoUnit.MILLIS.between(startEvent.time, endEvent.time);
                float avgSpeed;

                if (timeDiff != 0) {
                    // speed = distance / time
                    avgSpeed = (endEvent.travelDistance / timeDiff) * (1000 * 60 * 60);
                } else {
                    avgSpeed = -1f;
                }

                // emit average speed
                rides.remove(taxiRideEvent.rideId);
                outputStream.consume(new JetPair<>(taxiRideEvent.rideId, avgSpeed));
            } else {
                rides.put(taxiRideEvent.rideId, taxiRideEvent);
            }
        }
        return true;
    }
}
