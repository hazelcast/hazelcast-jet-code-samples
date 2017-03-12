import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.connector.kafka.ReadKafkaP;
import com.hazelcast.jet.stream.IStreamMap;
import com.hazelcast.map.listener.EntryAddedListener;
import kafka.admin.RackAwareMode;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import kafka.zk.EmbeddedZookeeper;
import org.I0Itec.zkclient.ZkClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeMap;
import static com.hazelcast.jet.connector.kafka.ReadKafkaP.readKafka;
import static kafka.admin.AdminUtils.createTopic;

/**
 * A sample which reads from 2 kafka topics and write to an IMap
 * <p>
 * <p>
 * {@link ReadKafkaP} is a processor that can be used for reading from Kafka.
 * High level consumer API is used to subscribe the topics which will
 * do the assignments of partitions to consumers (processors)
 */
public class ConsumeKafka {

    private static final int MESSAGE_COUNT = 100_000;

    private EmbeddedZookeeper zkServer;
    private ZkUtils zkUtils;
    private KafkaServer kafkaServer;

    public static void main(String[] args) throws Exception {
        new ConsumeKafka().run();
    }

    private void run() throws IOException, InterruptedException {
        createKafkaCluster();
        fillTopics();

        JetInstance instance = Jet.newJetInstance();
        Jet.newJetInstance();
        Jet.newJetInstance();

        DAG dag = new DAG();
        Vertex source = dag.newVertex("source", readKafka(getProperties(), "t1", "t2"));
        Vertex sink = dag.newVertex("sink", writeMap("sink"));

        dag.edge(between(source, sink));

        CountDownLatch latch = new CountDownLatch(2);
        IStreamMap<String, Integer> sinkMap = instance.getMap("sink");
        sinkMap.addEntryListener((EntryAddedListener) entryEvent -> {
            if (entryEvent.getKey().equals("t1-" + MESSAGE_COUNT)) {
                System.out.println("Topic [t1] finished");
                latch.countDown();
            } else if (entryEvent.getKey().equals("t2-" + MESSAGE_COUNT)) {
                System.out.println("Topic [t2] finished");
                latch.countDown();
            }
        }, false);

        instance.newJob(dag).execute();

        latch.await();
        shutdownKafkaCluster();
        System.exit(0);
    }

    /**
     * Creates an embedded zookeeper server and a kafka broker
     *
     * @throws IOException
     */
    private void createKafkaCluster() throws IOException {
        zkServer = new EmbeddedZookeeper();
        String zkConnect = "localhost:" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        Properties brokerProps = new Properties();
        brokerProps.setProperty("zookeeper.connect", zkConnect);
        brokerProps.setProperty("broker.id", "0");
        brokerProps.setProperty("log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString());
        brokerProps.setProperty("listeners", "PLAINTEXT://localhost:9092");
        KafkaConfig config = new KafkaConfig(brokerProps);
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }

    /**
     * Creates 2 topics (t1, t2) with different partition counts (32, 64) and fills them with items
     *
     * @throws IOException
     */
    private void fillTopics() throws IOException {
        createTopic(zkUtils, "t1", 32, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        createTopic(zkUtils, "t2", 64, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        System.out.println("Filling Topics");
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", IntegerSerializer.class.getName());
        KafkaProducer<String, Integer> producer = new KafkaProducer<>(properties);
        for (int i = 1; i <= MESSAGE_COUNT; i++) {
            producer.send(new ProducerRecord<>("t1", "t1-" + i, i));
            producer.send(new ProducerRecord<>("t2", "t2-" + i, i));
        }
        producer.close();
    }

    private void shutdownKafkaCluster() {
        kafkaServer.shutdown();
        zkUtils.close();
        zkServer.shutdown();
    }

    private static Properties getProperties() {
        Properties properties = new Properties();
        properties.setProperty("group.id", "group-" + Math.random());
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getCanonicalName());
        properties.setProperty("value.deserializer", IntegerDeserializer.class.getCanonicalName());
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }

}
