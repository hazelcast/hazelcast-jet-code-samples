/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.TextMessage;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A pipeline which streams messages from a JMS topic, filters them according
 * to the priority and writes a new message with modified properties to another
 * JMS topic.
 */
public class JmsTopicSample {

    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";

    private ScheduledExecutorService scheduledExecutorService;
    private ActiveMQBroker activeMQBroker;
    private JmsMessageProducer producer;
    private JetInstance jet;

    private static Pipeline buildPipeline() {
        Pipeline p = Pipeline.create();

        p.drawFrom(Sources.jmsTopic(() -> new ActiveMQConnectionFactory(ActiveMQBroker.BROKER_URL), INPUT_TOPIC))
         .filter(message -> uncheckCall(() -> message.getJMSPriority() > 3))
         .map(message -> (TextMessage) message)
         // print the message text to the log
         .peek(message -> uncheckCall(message::getText))
         .drainTo(Sinks.<TextMessage>jmsTopicBuilder(() -> new ActiveMQConnectionFactory(ActiveMQBroker.BROKER_URL))
                 .destinationName(OUTPUT_TOPIC)
                 .messageFn((session, message) -> uncheckCall(() -> {
                     TextMessage textMessage = session.createTextMessage(message.getText());
                     textMessage.setBooleanProperty("isActive", true);
                     textMessage.setJMSPriority(8);
                     return textMessage;
                 }))
                 .build());
        return p;
    }

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        new JmsTopicSample().go();
    }

    private void go() throws Exception {
        Job job = null;
        try {
            setup();
            job = jet.newJob(buildPipeline());
            scheduledExecutorService.schedule(job::cancel, 10, SECONDS);
            job.join();
        } catch (CancellationException e) {
            waitForComplete(job);
        } finally {
            cleanup();
        }
    }

    private void setup() throws Exception {
        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

        activeMQBroker = new ActiveMQBroker();
        activeMQBroker.start();

        producer = new JmsMessageProducer(INPUT_TOPIC, JmsMessageProducer.DestinationType.TOPIC);
        producer.start();

        jet = Jet.newJetInstance();
    }

    private void cleanup() {
        scheduledExecutorService.shutdown();
        producer.stop();
        activeMQBroker.stop();
        Jet.shutdownAll();
    }

    private static void waitForComplete(Job job) {
        while (job.getStatus() != JobStatus.COMPLETED) {
            uncheckRun(() -> SECONDS.sleep(1));
        }
    }
}
