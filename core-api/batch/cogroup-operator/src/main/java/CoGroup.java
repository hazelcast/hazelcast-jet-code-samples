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
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.core.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.core.processor.DiagnosticProcessors;
import com.hazelcast.jet.core.processor.Processors;
import com.hazelcast.jet.core.processor.SourceProcessors;

import java.util.Arrays;

import static com.hazelcast.jet.core.Edge.between;
import static com.hazelcast.jet.core.Edge.from;
import static com.hazelcast.jet.function.DistributedFunctions.entryValue;
import static java.lang.Runtime.getRuntime;

/**
 * A sample demonstrating implementation of CoGroup processor. The processor
 * does a full outer join on two sources by a given key demonstrating a
 * many-to-many relationship.
 * <p>
 * Each student is enrolled to one more or courses, and each teacher is
 * assigned to a course. A course can have many teachers. The expected
 * output is all pairs of teachers and students where they
 * share the same course.
 * <p>
 * Also see comments in {@link CoGroupP}.
 * <p>
 * The DAG is as follows:
 * <pre>{@code
 *                +-----------------+            +-----------------+
 *                |   enrollments   |            |    teachers     |
 *                +-----------------+            +-----------------+
 *                         |                              |
 *   (id, Enrollment)      |                              |  (id, Teacher)
 *                         V                              V
 *                +--------------------+         +-----------------+
 *                | extract-enrollment |         | extract-teacher |
 *                +--------------------+         +-----------------+
 *                         |                              |
 *           (Enrollment)  |                              |  (Teacher)
 *                         |        +------------+        |
 *                         +------->+  CoGroup   +<-------+
 *                                  +-----+------+
 *                                        |
 *                                        | Object[]{Enrollment, Teacher}
 *                                        V
 *                                  +-----+------+
 *                                  |    Sink    |
 *                                  +------------+
 * }</pre>
 */
public class CoGroup {

    private static JetInstance jet;

    public static void main(String[] args) throws Exception {
        try {
            setup();
            jet.newJob(buildDag()).join();
        } finally {
            Jet.shutdownAll();
        }
    }

    private static void setup() {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetConfig cfg = new JetConfig();
        cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                Math.max(1, getRuntime().availableProcessors() / 2)));

        System.out.println("Creating Jet instance 1");
        jet = Jet.newJetInstance(cfg);
        System.out.println("Creating Jet instance 2");
        Jet.newJetInstance(cfg);

        IMap<Integer, Teacher> teachers = jet.getMap("teachers");
        teachers.put(1, new Teacher("Joe", "CS 101"));
        teachers.put(2, new Teacher("Emily", "CS 101"));
        teachers.put(3, new Teacher("Jack", "MATH 101"));
        teachers.put(4, new Teacher("Martin", "BIO 101")); // BIO 101 has no students

        IMap<Integer, Enrollment> enrollments = jet.getMap("enrollments");
        enrollments.put(1, new Enrollment("Bob", "CS 101"));
        enrollments.put(2, new Enrollment("Alice", "CS 101"));
        enrollments.put(3, new Enrollment("Bob", "MATH 101"));
        enrollments.put(4, new Enrollment("William", "MATH 101"));
        enrollments.put(5, new Enrollment("Bob", "PHYS 101")); // PHYS 101 has no teachers

    }

    private static DAG buildDag() {
        DAG dag = new DAG();

        Vertex enrollments = dag.newVertex("enrollments", SourceProcessors.readMap("enrollments"));
        Vertex extractEnrollment = dag.newVertex("extract-enrollment", Processors.map(entryValue()));
        Vertex teachers = dag.newVertex("teachers", SourceProcessors.readMap("teachers"));
        Vertex extractTeacher = dag.newVertex("extract-teacher", Processors.map(entryValue()));

        Vertex cogroup = dag.newVertex("cogroup", () -> new CoGroupP<>(Teacher::getCourse, Enrollment::getCourse));
        Vertex sink = dag.newVertex("sink", DiagnosticProcessors.writeLogger(o -> Arrays.toString((Object[]) o)))
                         .localParallelism(1);

        // It is likely that there are more enrollments than teachers, so
        // teachers will be accumulated first.

        // Edge from extractTeacher to CoGroup needs to be prioritized in order
        // to be able to fully accumulate it before processing the other source.
        // Both edges to CoGroup must be distributed and partitioned, so that
        // single processor instance sees all items with the same key.
        dag
                .edge(between(enrollments, extractEnrollment).isolated())
                .edge(between(teachers, extractTeacher).isolated())
                .edge(from(extractTeacher).to(cogroup, 0)
                                          .priority(-1)
                                          .distributed()
                                          .partitioned(Teacher::getCourse))
                .edge(from(extractEnrollment).to(cogroup, 1)
                                          .distributed()
                                          .partitioned(Enrollment::getCourse))
                .edge(between(cogroup, sink));

        return dag;
    }
}
