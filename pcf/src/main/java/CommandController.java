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
import com.hazelcast.jet.config.JobConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.Comparator.comparingLong;

@RestController
public class CommandController {

    @Autowired
    JetInstance jetClient;

    @RequestMapping("/populate")
    public CommandResponse populate(@RequestParam(value = "name") String name) {
        IMap<Integer, String> map = jetClient.getMap(name);
        map.put(0, "It was the best of times,");
        map.put(1, "it was the worst of times,");
        map.put(2, "it was the age of wisdom,");
        map.put(3, "it was the age of foolishness,");
        map.put(4, "it was the epoch of belief,");
        map.put(5, "it was the epoch of incredulity,");
        map.put(6, "it was the season of Light,");
        map.put(7, "it was the season of Darkness");
        map.put(8, "it was the spring of hope,");
        map.put(9, "it was the winter of despair,");
        map.put(10, "we had everything before us,");
        map.put(11, "we had nothing before us,");
        map.put(12, "we were all going direct to Heaven,");
        map.put(13, "we were all going direct the other way --");
        map.put(14, "in short, the period was so far like the present period, that some of "
                + "its noisiest authorities insisted on its being received, for good or for "
                + "evil, in the superlative degree of comparison only.");

        return new CommandResponse("Map is populated with data");
    }

    @RequestMapping("/wordCount")
    public CommandResponse wordCount(@RequestParam(value = "sourceName") String sourceName,
                                     @RequestParam(value = "sinkName") String sinkName) {
        try {
            JobConfig jobConfig = new JobConfig();
            jobConfig.addClass(DagBuilder.class);
            jetClient.newJob(DagBuilder.buildDag(sourceName, sinkName), jobConfig).execute().get();
            IMap<String, Long> counts = jetClient.getMap(sinkName);
            List<Map.Entry<String, Long>> topResult = counts.entrySet()
                                                            .stream()
                                                            .sorted(comparingLong(Map.Entry<String, Long>::getValue).reversed())
                                                            .limit(1)
                                                            .collect(Collectors.toList());
            Map.Entry<String, Long> entry = topResult.get(0);
            return new CommandResponse("Top word is `" + entry.getKey() + "` with the count: " + entry.getValue());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return new CommandResponse("There was an exception during the execution of word-count: " + e.getMessage());
        }
    }

    @RequestMapping("/put")
    public CommandResponse put(@RequestParam(value = "name") String name, @RequestParam(value = "key") Integer key,
                               @RequestParam(value = "value") String value) {
        IMap<Integer, String> map = jetClient.getMap(name);
        String oldValue = map.put(key, value);
        return new CommandResponse(oldValue);
    }

    @RequestMapping("/get")
    public CommandResponse get(@RequestParam(value = "name") String name, @RequestParam(value = "key") Object key) {
        IMap<Object, String> map = jetClient.getMap(name);
        String value = map.get(key);
        return new CommandResponse(value);
    }

    @RequestMapping("/remove")
    public CommandResponse remove(@RequestParam(value = "name") String name, @RequestParam(value = "key") Object key) {
        IMap<Object, String> map = jetClient.getMap(name);
        String value = map.remove(key);
        return new CommandResponse(value);
    }

    @RequestMapping("/size")
    public CommandResponse size(@RequestParam(value = "name") String name) {
        IMap<Integer, String> map = jetClient.getMap(name);
        int size = map.size();
        return new CommandResponse(Integer.toString(size));
    }

    @RequestMapping("/clear")
    public CommandResponse clear(@RequestParam(value = "name") String name) {
        IMap<String, String> map = jetClient.getMap(name);
        map.clear();
        return new CommandResponse("Map is cleared");
    }
}
