package averagesalary.processor;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.JetPair;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.processor.ContainerProcessor;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import averagesalary.model.Employee;

/**
 * Holds all the employee salaries and emits the average of them.
 */
public class AverageCalculator implements ContainerProcessor<Employee, Pair<Integer, Double>> {

    private List<Integer> employeeSalaries = new ArrayList<>();

    @Override
    public boolean process(ProducerInputStream<Employee> inputStream,
                           ConsumerOutputStream<Pair<Integer, Double>> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {
        for (Employee employee : inputStream) {
            employeeSalaries.add(employee.getSalary());
        }
        return true;
    }

    @Override
    public boolean finalizeProcessor(ConsumerOutputStream<Pair<Integer, Double>> outputStream, ProcessorContext processorContext) throws Exception {
        OptionalDouble average = employeeSalaries
                .stream()
                .mapToDouble(a -> a)
                .average();
        outputStream.consume(new JetPair<Integer, Double>(0, average.getAsDouble()));
        return true;
    }
}
