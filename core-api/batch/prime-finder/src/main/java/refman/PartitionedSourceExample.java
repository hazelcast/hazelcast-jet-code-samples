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

package refman;

import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.core.Edge;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Vertex;

public class PartitionedSourceExample {
public static void main(String[] args) throws Exception {
    System.setProperty("hazelcast.logging.type", "log4j");

    tutorialStep3();

}

private static void tutorialStep1() throws Exception {
JetInstance jet = Jet.newJetInstance();

int upperBound = 10;
DAG dag = new DAG();
Vertex generateNumbers = dag.newVertex("generate-numbers",
        () -> new GenerateNumbersP(upperBound));
Vertex logInput = dag.newVertex("log-input", LogInputP::new);
dag.edge(Edge.between(generateNumbers, logInput));

try {
    jet.newJob(dag).join();
} finally {
    Jet.shutdownAll();
}
}

private static void tutorialStep2() throws Exception {
JetInstance jet = Jet.newJetInstance();

int upperBound = 10;
DAG dag = new DAG();
Vertex generateNumbers = dag.newVertex("generate-numbers",
        new GenerateNumbersPSupplier(upperBound));
Vertex logInput = dag.newVertex("log-input", LogInputP::new);
dag.edge(Edge.between(generateNumbers, logInput));

try {
    jet.newJob(dag).join();
} finally {
    Jet.shutdownAll();
}
}

private static void tutorialStep3() throws Exception {
JetInstance jet = Jet.newJetInstance();
Jet.newJetInstance();

int upperBound = 10;
DAG dag = new DAG();
Vertex generateNumbers = dag.newVertex("generate-numbers",
        new GenerateNumbersPMetaSupplier(upperBound));
Vertex logInput = dag.newVertex("log-input", LogInputP::new);
dag.edge(Edge.between(generateNumbers, logInput));

try {
    jet.newJob(dag).join();
} finally {
    Jet.shutdownAll();
}

}
}
