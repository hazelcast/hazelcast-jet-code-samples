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

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.samples.cogroup.GeneratePersonsP;
import com.hazelcast.jet.samples.cogroup.Person;

import java.util.Arrays;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Edge.from;
import static com.hazelcast.jet.Processors.writeLogger;
import static com.hazelcast.jet.samples.cogroup.Person.EMPLOYEES;
import static com.hazelcast.jet.samples.cogroup.Person.STUDENTS;

/**
 * A sample demonstrating implementation of CoGroup operator. The operator
 * joins two sources.
 * <p>
 * Also see comments in {@link CoGroupP}.
 * <p>
 * The DAG is as follows:
 * <pre>{@code
 *                 +-----------------+                 +-------------------+
 *                 | Students source |                 | Employees source  |
 *                 +-----------+-----+                 +----+--------------+
 *                             |                            |
 *            Person (student) |                            | Person (employee)
 * (distributed, partitioned   |     +------------+         |  (distributed
 *            priority edge)   +-----+  CoGroup   +---------+  partitioned edge)
 *                                   +-----+------+
 *                                         |
 *                                         | Object[]{student, epmloyee}
 *                                         |
 *                                   +-----+------+
 *                                   |    Sink    |
 *                                   +------------+
 * }</pre>
 */
public class CoGroupSample {

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();
//        Jet.newJetInstance();
        try {
            jet.newJob(buildDag()).execute().get();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag() {
        DAG dag = new DAG();

        Vertex studentsSource = dag.newVertex("studentsSource", () -> new GeneratePersonsP(STUDENTS))
                                   .localParallelism(1);
        Vertex employeesSource = dag.newVertex("employeesSource", () -> new GeneratePersonsP(EMPLOYEES))
                                    .localParallelism(1);
        Vertex cogroup = dag.newVertex("cogroup", () -> new CoGroupP<>(Person::getAge, Person::getAge));
        Vertex sink = dag.newVertex("sink", writeLogger(o -> Arrays.toString((Object[]) o)))
                         .localParallelism(1);

        // Edge from studentsSource to CoGroup needs to be prioritized in order
        // to be able to fully accumulate it before processing the other source.
        // Both edges to CoGroup must be distributed and partitioned, so that
        // single processor instance sees all items with particular key.
        dag.edge(from(studentsSource).to(cogroup, 0)
                                     .priority(-1)
                                     .distributed()
                                     .partitioned(Person::getAge))
           .edge(from(employeesSource).to(cogroup, 1)
                                      .distributed()
                                      .partitioned(Person::getAge))
           .edge(between(cogroup, sink));

        return dag;
    }
}
