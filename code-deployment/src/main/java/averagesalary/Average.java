package averagesalary;

import averagesalary.model.Employee;
import averagesalary.processor.AverageCalculator;
import averagesalary.processor.EmployeeParser;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.jet.JetEngine;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.dag.DAG;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.dag.sink.MapSink;
import com.hazelcast.jet.dag.source.FileSource;
import com.hazelcast.jet.job.Job;
import com.hazelcast.jet.processor.ProcessorDescriptor;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * Reads employee records from files and calculates the average salary of the company.
 * While making this computation, the server doesn't has any idea of the classes that required for this job.
 * So we provide the required classes and they deployed to the server in the job classloader.
 */
public class Average {

    public static void main(String[] args) throws IOException {
        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        DAG dag = new DAG(Average.class.getName());

        Vertex parser = new Vertex("employee parser", ProcessorDescriptor.builder(EmployeeParser.class).build());
        parser.addSource(new FileSource(Average.class.getClassLoader().getResource("employees").getFile()));

        Vertex average = new Vertex("average calculator", ProcessorDescriptor.builder(AverageCalculator.class).build());
        average.addSink(new MapSink("result"));

        dag.addVertex(parser);
        dag.addVertex(average);
        Edge edge = new Edge("parser-to-average-calculator", parser, average);
        dag.addEdge(edge);

        JobConfig config = new JobConfig();
        config.addClass(Employee.class, EmployeeParser.class, AverageCalculator.class);

        Job job = JetEngine.getJob(client, "average salary " + System.currentTimeMillis(), dag, config);
        try {
            job.execute().get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            job.destroy();
        }

        IMap<Object, Object> resultMap = client.getMap("result");
        Object result = resultMap.values().iterator().next();
        System.out.println("average salary = " + result);
        client.shutdown();

    }
}
