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
import com.hazelcast.jet.Job;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.config.InstanceConfig;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.connector.kafka.ReadKafkaP;
import com.hazelcast.jet.stream.IStreamMap;
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

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Processors.writeMap;
import static com.hazelcast.jet.connector.kafka.ReadKafkaP.readKafka;
import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static kafka.admin.AdminUtils.createTopic;

/**
 * A sample which does a distributed read from 2 Kafka topics and writes to an
 * IMap.
 * <p>
 * {@link ReadKafkaP} is a processor that can be used for reading from Kafka.
 * High-level consumer API is used to subscribe to the topics which will
 * do the assignments of partitions to consumers (processors)
 */
public class ConsumeKafka {

    private static final int MESSAGE_COUNT_PER_TOPIC = 1_000_000;

    private EmbeddedZookeeper zkServer;
    private ZkUtils zkUtils;
    private KafkaServer kafkaServer;

    public static void main(String[] args) throws Exception {
        new ConsumeKafka().run();
    }

    private void run() throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");

        createKafkaCluster();
        fillTopics();

        JetConfig cfg = new JetConfig();
        cfg.setInstanceConfig(new InstanceConfig().setCooperativeThreadCount(
                Math.max(1, getRuntime().availableProcessors() / 2)));

        JetInstance instance = Jet.newJetInstance(cfg);
        Jet.newJetInstance(cfg);
        IStreamMap<String, Integer> sinkMap = instance.getMap("sink");

        Job job = createJetJob(instance);
        long start = System.nanoTime();
        job.execute();
        while (true) {
            int mapSize = sinkMap.size();
            System.out.format("Received %d entries in %d milliseconds.%n",
                    mapSize, NANOSECONDS.toMillis(System.nanoTime() - start));
            if (mapSize == MESSAGE_COUNT_PER_TOPIC * 2) {
                break;
            }
            Thread.sleep(100);
        }
        shutdownKafkaCluster();
        System.exit(0);
    }

    private static Job createJetJob(JetInstance instance) {
        DAG dag = new DAG();
        Properties props = props(
                "group.id", "group-" + Math.random(),
                "bootstrap.servers", "localhost:9092",
                "key.deserializer", StringDeserializer.class.getCanonicalName(),
                "value.deserializer", IntegerDeserializer.class.getCanonicalName(),
                "auto.offset.reset", "earliest");
        Vertex source = dag.newVertex("source", readKafka(props, "t1", "t2"));
        Vertex sink = dag.newVertex("sink", writeMap("sink"));
        dag.edge(between(source, sink));
        return instance.newJob(dag);
    }

    // Creates an embedded zookeeper server and a kafka broker
    private void createKafkaCluster() throws IOException {
        zkServer = new EmbeddedZookeeper();
        String zkConnect = "localhost:" + zkServer.port();
        ZkClient zkClient = new ZkClient(zkConnect, 30000, 30000, ZKStringSerializer$.MODULE$);
        zkUtils = ZkUtils.apply(zkClient, false);

        KafkaConfig config = new KafkaConfig(props(
                "zookeeper.connect", zkConnect,
                "broker.id", "0",
                "log.dirs", Files.createTempDirectory("kafka-").toAbsolutePath().toString(),
                "listeners", "PLAINTEXT://localhost:9092"));
        Time mock = new MockTime();
        kafkaServer = TestUtils.createServer(config, mock);
    }

    // Creates 2 topics (t1, t2) with different partition counts (32, 64) and fills them with items
    private void fillTopics() {
        createTopic(zkUtils, "t1", 32, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
        createTopic(zkUtils, "t2", 64, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);

        System.out.println("Filling Topics");
        Properties props = props(
                "bootstrap.servers", "localhost:9092",
                "key.serializer", StringSerializer.class.getName(),
                "value.serializer", IntegerSerializer.class.getName());
        try (KafkaProducer<String, Integer> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= MESSAGE_COUNT_PER_TOPIC; i++) {
                producer.send(new ProducerRecord<>("t1", "t1-" + i, i));
                producer.send(new ProducerRecord<>("t2", "t2-" + i, i));
            }
            System.out.println("Published " + MESSAGE_COUNT_PER_TOPIC + " messages to t1");
            System.out.println("Published " + MESSAGE_COUNT_PER_TOPIC + " messages to t2");
        }
    }

    private void shutdownKafkaCluster() {
        kafkaServer.shutdown();
        zkUtils.close();
        zkServer.shutdown();
    }

    private static Properties props(String... kvs) {
        final Properties props = new Properties();
        for (int i = 0; i < kvs.length;) {
            props.setProperty(kvs[i++], kvs[i++]);
        }
        return props;
    }
}

