package averagesalary.processor;

import com.hazelcast.jet.container.ProcessorContext;
import com.hazelcast.jet.data.io.ConsumerOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.io.Pair;
import com.hazelcast.jet.processor.ContainerProcessor;
import averagesalary.model.Employee;

/**
 * Reads a string record of employee and emits an {@link Employee} object
 */
public class EmployeeParser implements ContainerProcessor<Pair<Integer, String>, Employee> {

    @Override
    public boolean process(ProducerInputStream<Pair<Integer, String>> inputStream,
                           ConsumerOutputStream<Employee> outputStream,
                           String sourceName,
                           ProcessorContext processorContext) throws Exception {

        for (Pair<Integer, String> pair : inputStream) {
            String value = pair.getValue();
            Employee employee = new Employee();
            employee.fromString(value);
            outputStream.consume(employee);
        }
        return true;
    }
}
