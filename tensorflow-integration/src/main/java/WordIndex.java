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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Map;

public class WordIndex {
    private static final int PAD = 0;
    private static final int START = 1;
    private static final int UNKNOWN = 2;
    private final Map<String, Integer> wordIndex;

    public WordIndex(String[] args) {
        if (args.length != 1) {
            System.err.println("You need to provide data directory as a command-line argument");
            System.exit(1);
        }

        try {
            wordIndex = loadWordIndex(new FileReader(args[0] + '/' + "imdb_word_index.json"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public float[][] createTensorInput(String text) {
        float[] indexedPadded = new float[256];
        Arrays.fill(indexedPadded, PAD);
        indexedPadded[0] = START;
        int i = 1;
        for (String word : text.split("\\W+")) {
            indexedPadded[i++] = wordIndex.getOrDefault(word, UNKNOWN);
            if (i == indexedPadded.length) {
                break;
            }
        }
        return new float[][]{indexedPadded};
    }

    private Map<String, Integer> loadWordIndex(Reader in) {
        Type type = new TypeToken<Map<String, Integer>>() {
        }.getType();
        Map<String, Integer> wordIndex = new Gson().fromJson(in, type);
        // First 3 indices are reserved
        wordIndex.entrySet().forEach(entry -> entry.setValue(entry.getValue() + 3));
        return wordIndex;
    }
}
