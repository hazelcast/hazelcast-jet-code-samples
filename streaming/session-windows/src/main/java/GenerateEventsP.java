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

import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.samples.sessionwindows.ProductEvent;
import com.hazelcast.jet.samples.sessionwindows.ProductEventType;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.jet.samples.sessionwindows.ProductEventType.PURCHASE;
import static com.hazelcast.jet.samples.sessionwindows.ProductEventType.VIEW_LISTING;
import static java.lang.Math.max;

/**
 * Generator of random product events.
 *<p>
 * It tries to simulate real-life traffic: it generates certain number of
 * {@link UserTracker} objects, which specify number of listings and number of
 * purchased products. Then emits the events in real time, for 5 users
 * simultaneously.
 */
public class GenerateEventsP extends AbstractProcessor {

    private final Random random = new Random();
    private UserTracker[] userTrackers = new UserTracker[5];

    @Override
    protected void init(@Nonnull Context context) throws Exception {
        Arrays.setAll(userTrackers, i -> randomTracker());
    }

    @Override
    public boolean complete() {
        // Generate one event for each user in userTrackers
        for (int i = 0; i < userTrackers.length; i++) {
            // randomly skip some events
            if (random.nextInt(3) != 0) {
                continue;
            }
            UserTracker track = userTrackers[i];
            if (track.remainingListings > 0) {
                track.remainingListings--;
                emit(randomEvent(track.userId, VIEW_LISTING));
            } else {
                track.remainingPurchases--;
                emit(randomEvent(track.userId, PURCHASE));
            }
            // we are done with this userTracker, generate a new one
            if (track.remainingListings == 0 && track.remainingPurchases == 0) {
                userTrackers[i] = randomTracker();
            }
        }

        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            return true;
        }

        return false;
    }

    @Override
    public boolean isCooperative() {
        // we are doing a blocking sleep so we aren't cooperative
        return false;
    }

    private ProductEvent randomEvent(String userId, ProductEventType viewListing) {
        return new ProductEvent(System.currentTimeMillis(), userId,
                "product" + random.nextInt(20),
                viewListing);
    }

    private UserTracker randomTracker() {
        return new UserTracker(String.format("user%03d", random.nextInt(100)),
                random.nextInt(20),
                max(0, random.nextInt(20) - 16));
    }

    private static final class UserTracker {
        final String userId;
        int remainingListings;
        int remainingPurchases;

        private UserTracker(String userId, int numListings, int numPurchases) {
            this.userId = userId;
            this.remainingListings = numListings;
            this.remainingPurchases = numPurchases;
        }
    }
}
