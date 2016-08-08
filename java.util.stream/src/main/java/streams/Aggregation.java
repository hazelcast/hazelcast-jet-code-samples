/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package streams;

import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.stream.IStreamMap;
import models.Employee;

import java.util.IntSummaryStatistics;

public class Aggregation {

    public static void main(String[] args) {
        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance();

        IMap<String, Employee> employees = instance1.getMap("employees");

        employees.put("0", new Employee("0", 1000));
        employees.put("1", new Employee("1", 1500));
        employees.put("2", new Employee("2", 500));
        employees.put("3", new Employee("3", 2000));

        IStreamMap<String, Employee> streamMap = IStreamMap.streamMap(employees);

        IntSummaryStatistics intSummaryStatistics = streamMap.stream()
                .mapToInt(m -> m.getValue().getSalary())
                .summaryStatistics();


        System.out.println("Stats=" + intSummaryStatistics);

        IMap<String, Integer> ints = instance1.getMap("ints");
        IStreamMap<String, Integer> map = IStreamMap.streamMap(ints);
        int result = map.stream().map(m -> m.getValue()).reduce(0, (l, r) -> l + r);
        System.out.println(result);
        Hazelcast.shutdownAll();


    }
}

