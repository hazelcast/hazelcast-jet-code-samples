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
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.stream.IStreamMap;

import java.util.Arrays;
import java.util.Map;

import static com.hazelcast.jet.stream.impl.StreamUtil.uniqueListName;

public class Sort {

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");
        try {
            JetInstance instance1 = Jet.newJetInstance();
            Jet.newJetInstance();
            IStreamMap<String, Employee> employees = instance1.getMap("employees");
            employees.put("0", new Employee("0", 1000));
            employees.put("1", new Employee("1", 500));
            employees.put("2", new Employee("2", 3000));
            employees.put("3", new Employee("3", 2000));

            IList<Employee> sorted = employees
                    .stream()
                    .map(Map.Entry::getValue)
                    .sorted((left, right) -> Integer.compare(left.getSalary(), right.getSalary()))
                    .collect(DistributedCollectors.toIList(uniqueListName()));

            System.out.println("Sorted=" + Arrays.toString(sorted.toArray()));
        } finally {
            Jet.shutdownAll();
        }
    }
}


