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

package averagesalary.processor;

import averagesalary.model.Employee;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.JetPair;
import com.hazelcast.jet.runtime.OutputCollector;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;

/**
 * Holds all the employee salaries and emits the average of them.
 */
public class AverageCalculator implements Processor<Employee, Pair<Integer, Double>> {

    private List<Integer> employeeSalaries = new ArrayList<>();

    @Override
    public boolean process(InputChunk<Employee> input,
                           OutputCollector<Pair<Integer, Double>> output,
                           String sourceName) throws Exception {
        for (Employee employee : input) {
            employeeSalaries.add(employee.getSalary());
        }
        return true;
    }

    @Override
    public boolean complete(OutputCollector<Pair<Integer, Double>> output) throws Exception {
        OptionalDouble average = employeeSalaries
                .stream()
                .mapToDouble(a -> a)
                .average();
        output.collect(new JetPair<>(0, average.getAsDouble()));
        return true;
    }
}
