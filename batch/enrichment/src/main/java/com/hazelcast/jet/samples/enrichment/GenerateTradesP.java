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

package com.hazelcast.jet.samples.enrichment;

import com.hazelcast.jet.AbstractProcessor;

/**
 * Generate fixed number of sample trades. Note that every instance of this
 * processor will emit the same trades.
 */
public final class GenerateTradesP extends AbstractProcessor {

    @Override
    public boolean complete() {
        emit(new Trade(1, "AAAP", 1, 1));
        emit(new Trade(1, "BABY", 1, 1));
        emit(new Trade(1, "CA", 1, 1));
        emit(new Trade(2, "AAAP", 1, 1));
        emit(new Trade(2, "BABY", 1, 1));
        emit(new Trade(2, "CA", 1, 1));
        emit(new Trade(3, "AAAP", 1, 1));
        emit(new Trade(3, "BABY", 1, 1));
        emit(new Trade(3, "CA", 1, 1));
        return true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }
}
