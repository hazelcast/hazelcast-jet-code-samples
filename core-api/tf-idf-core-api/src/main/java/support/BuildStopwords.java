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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Stream;

import static com.hazelcast.jet.Util.entry;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

/**
 * A utility that rebuilds the stopwords file, only needed when the list of
 * books in the {@code books} module changes.
 */
public class BuildStopwords {
    public static void main(String[] args) throws IOException {
        final Map<Long, String> docId2Name = TfIdfUtil.buildDocumentInventory();
        final long docCount = docId2Name.size();
        System.out.println("Analyzing documents");
        final Map<String, Set<Long>> wordDocs = docId2Name
                .entrySet()
                .parallelStream()
                .flatMap(TfIdfUtil::docLines)
                .flatMap(BuildStopwords::tokenize)
                .collect(groupingBy(Entry::getValue, mapping(Entry::getKey, toSet())));
        final File stopwordsFile = new File("stopwords.txt");
        System.out.println("Writing the stopwords file " + stopwordsFile.getAbsolutePath());
        try (PrintWriter w = new PrintWriter(new OutputStreamWriter(new FileOutputStream(stopwordsFile), UTF_8))) {
            wordDocs.entrySet()
                    .stream()
                    .map(e -> entry(e.getKey(), e.getValue().size()))
                    .filter(e -> e.getValue() == docCount)
                    .sorted(comparing(Entry::getKey))
                    .map(Entry::getKey)
                    .forEach(w::println);
        }
    }

    private static Stream<Entry<Long, String>> tokenize(Entry<Long, String> docLine) {
        return Arrays.stream(TfIdfUtil.DELIMITER.split(docLine.getValue()))
                     .filter(token -> !token.isEmpty())
                     .map(word -> entry(docLine.getKey(), word));
    }
}
