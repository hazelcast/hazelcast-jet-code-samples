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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

public class TfIdfUtil {
    public static final Pattern DELIMITER = Pattern.compile("\\W+");

    public static Map<Long, String> buildDocumentInventory() {
        try (BufferedReader r = resourceReader("books")) {
            final long[] docId = {0};
            System.out.println("These books will be indexed:");
            return r.lines()
                    .peek(System.out::println)
                    .collect(toMap(x -> ++docId[0], identity()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Stream<Entry<Long, String>> docLines(Entry<Long, String> idAndName) {
        try {
            return Files.lines(Paths.get(TfIdfUtil.class.getResource("/books/" + idAndName.getValue()).toURI()))
                        .map(String::toLowerCase)
                        .map(line -> entry(idAndName.getKey(), line));
        } catch (IOException | URISyntaxException e) {
            throw new RuntimeException(e);
        }
    }

    public static BufferedReader resourceReader(String resourceName) {
        final ClassLoader cl = TfIdfUtil.class.getClassLoader();
        InputStream in = Objects.requireNonNull(cl.getResourceAsStream(resourceName));
        return new BufferedReader(new InputStreamReader(in, UTF_8));
    }
}
