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

import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.stream.IStreamMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CommandController {

    @Autowired
    JetInstance jetClient;

    @RequestMapping("/count")
    public CommandResponse count() {
        IStreamMap<String, String> map = jetClient.getMap("map");
        long count = map.stream().mapToInt(e -> Integer.parseInt(e.getValue())).count();
        return new CommandResponse(String.valueOf(count));
    }

    @RequestMapping("/put")
    public CommandResponse put(@RequestParam(value = "key") String key, @RequestParam(value = "value") String value) {
        IMap<String, String> map = jetClient.getMap("map");
        String oldValue = map.put(key, value);
        return new CommandResponse(oldValue);
    }

    @RequestMapping("/get")
    public CommandResponse get(@RequestParam(value = "key") String key) {
        IMap<String, String> map = jetClient.getMap("map");
        String value = map.get(key);
        return new CommandResponse(value);
    }

    @RequestMapping("/remove")
    public CommandResponse remove(@RequestParam(value = "key") String key) {
        IMap<String, String> map = jetClient.getMap("map");
        String value = map.remove(key);
        return new CommandResponse(value);
    }

    @RequestMapping("/size")
    public CommandResponse size() {
        IMap<String, String> map = jetClient.getMap("map");
        int size = map.size();
        return new CommandResponse(Integer.toString(size));
    }

    @RequestMapping("/populate")
    public CommandResponse populate() {
        IMap<String, String> map = jetClient.getMap("map");
        for (int i = 0; i < 1000; i++) {
            String s = Integer.toString(i);
            map.put(s, s);
        }
        return new CommandResponse("1000 entry inserted to the map");
    }

    @RequestMapping("/clear")
    public CommandResponse clear() {
        IMap<String, String> map = jetClient.getMap("map");
        map.clear();
        return new CommandResponse("Map cleared");
    }
}
