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

import com.hazelcast.core.IList;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.ProcessorMetaSupplier;
import com.hazelcast.jet.core.processor.SourceProcessors;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.DistributedStream;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Demonstrates how to use file processor as a source
 */
public class FileSource {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        try {
            JetInstance instance = Jet.newJetInstance();

            Path dir = createTempFile();

            ProcessorMetaSupplier metaSupplier = SourceProcessors.readFilesP(dir.toString(), UTF_8, "*");

            IList<String> sink = DistributedStream
                    .<String>fromSource(instance, metaSupplier)
                    .flatMap(line -> Arrays.stream(line.split(" ")))
                    .collect(DistributedCollectors.toIList("sink"));


            sink.forEach(System.out::println);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static Path createTempFile() throws IOException {
        Path tempDirectory = Files.createTempDirectory("read-file-p");
        Path tempFile = Files.createTempFile(tempDirectory, "tempFile", "txt");
        PrintWriter printWriter = new PrintWriter(new FileOutputStream(tempFile.toFile(), true));
        printWriter.write("Hello World!\n");
        printWriter.write("How are you?\n");
        printWriter.close();
        return tempDirectory;
    }

}
