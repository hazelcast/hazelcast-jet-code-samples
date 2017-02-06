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
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.util.UuidUtil;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Simple class that showcases Jet's {@code java.util.stream} implementation
 * with the word counting example.
 */
public class WordCount {

    private static final Pattern PATTERN = Pattern.compile("\\W+");
    private static final String[] BOOKS = {
            "books/dracula.txt",
            "books/pride-and-prejudice.txt",
            "books/ulysses.txt",
            "books/war-and-peace.txt",
            "books/a-tale-of-two-cities.txt",
    };

    public static void main(String[] args) throws Exception {
        JetInstance instance1 = Jet.newJetInstance();
        Jet.newJetInstance();
        IStreamMap<String, String> lines = instance1.getMap("lines");
        System.out.println("Populating map...");
        for (String book : BOOKS) {
            populateMap(lines, book);
        }
        System.out.println("Populated map with " + lines.size() + " lines");
        IMap<String, Long> counts = lines
                .stream()
                .flatMap(m -> Stream.of(PATTERN.split(m.getValue().toLowerCase())))
                .collect(DistributedCollectors.toIMap(w -> w, w -> 1L, (left, right) -> left + right));
        System.out.println("Counts=" + counts.entrySet());
        Jet.shutdownAll();
    }

    private static Stream<String> lineStream(String path) throws URISyntaxException, IOException {
        URL resource = WordCount.class.getResource(path);
        return Files.lines(Paths.get(resource.toURI()));
    }

    private static void populateMap(Map<String, String> map, String path) throws IOException, URISyntaxException {
        Map<String, String> lines = lineStream(path)
                .map(l -> new SimpleImmutableEntry<>(UuidUtil.newUnsecureUuidString(), l))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        map.putAll(lines);
    }
}
