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


import com.hazelcast.jet.Processor;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.OutputCollector;

/**
 * Processor which will filter incoming rides and only emit them if they're in within the geographic boundaries of NYC.
 */
public class TaxiRideFilter implements Processor<TaxiRideEvent, TaxiRideEvent> {

    // geo boundaries of the area of NYC
    private static final double LON_EAST = -73.7;
    private static final double LON_WEST = -74.05;
    private static final double LAT_NORTH = 41.0;
    private static final double LAT_SOUTH = 40.5;

    @Override
    public boolean process(InputChunk<TaxiRideEvent> input,
                           OutputCollector<TaxiRideEvent> output,
                           String sourceName) throws Exception {
        for (TaxiRideEvent taxiRideEvent : input) {
            if (isInNYC(taxiRideEvent)) {
                output.collect(taxiRideEvent);
            }
        }
        return true;
    }

    /**
     * Checks if a TaxiRide within the geo boundaries of New York City.
     *
     * @param taxiRideEvent taxi ride
     * @return true if the location is within NYC boundaries, otherwise false.
     */

    public static boolean isInNYC(TaxiRideEvent taxiRideEvent) {
        return isInNYC(taxiRideEvent.startLon, taxiRideEvent.startLat)
                && isInNYC(taxiRideEvent.endLon, taxiRideEvent.endLat);
    }

    /**
     * Checks if a location specified by longitude and latitude values is
     * within the geo boundaries of New York City.
     *
     * @param lon longitude of the location to check
     * @param lat latitude of the location to check
     * @return true if the location is within NYC boundaries, otherwise false.
     */
    public static boolean isInNYC(float lon, float lat) {
        return !(lon > LON_EAST || lon < LON_WEST)
                && !(lat > LAT_NORTH || lat < LAT_SOUTH);
    }

}
