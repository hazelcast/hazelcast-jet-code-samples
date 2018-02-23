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

import java.io.Serializable;
import java.util.Map;

/**
 * DTO for additional information about the ticker.
 */
public final class TickerInfo implements Serializable {
    public final String symbol;
    public final String securityName;

    public TickerInfo(String symbol, String securityName) {
        this.symbol = symbol;
        this.securityName = securityName;
    }

    @Override
    public String toString() {
        return "TickerInfo{symbol='" + symbol + '\'' + ", securityName='" + securityName + '\'' + '}';
    }

    public static void populateMap(Map<String, TickerInfo> map) {
        map.put("AAAP", new TickerInfo("AAAP", "Advanced Accelerator Applications S.A."));
        map.put("BABY", new TickerInfo("BABY", "Natus Medical Incorporated"));
        map.put("CA", new TickerInfo("CA", "CA Inc."));
    }
}
