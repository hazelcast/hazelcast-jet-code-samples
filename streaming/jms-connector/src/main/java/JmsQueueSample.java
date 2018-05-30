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

import static com.hazelcast.jet.function.DistributedFunction.identity;
import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.concurrent.TimeUnit.SECONDS;
import static javax.jms.Session.AUTO_ACKNOWLEDGE;

/**
 * A pipeline which streams messages from a JMS Queue, filters according to the
 * priority, enriches the messages with some additional property and writes to
 * another JMS Queue
 */
public class JmsQueueSample {

    private static final String INPUT_QUEUE = "inputQueue";
    private static final String OUTPUT_QUEUE = "outputQueue";

    public static void main(String[] args) throws Exception {
        ActiveMQBroker.start();
        JmsMessageProducer producer = new JmsMessageProducer(INPUT_QUEUE, JmsMessageProducer.DestinationType.QUEUE);
        producer.start();

        JetInstance jet = Jet.newJetInstance();

        Pipeline pipeline = Pipeline.create();

        pipeline.drawFrom(Sources.jmsQueue(() -> new ActiveMQConnectionFactory(ActiveMQBroker.BROKER_URL), INPUT_QUEUE))
                .filter(message -> uncheckCall(() -> message.getJMSPriority() > 3))
                .map(message -> (TextMessage) message)
                .peek(message -> uncheckCall(message::getText))
                .drainTo(Sinks.jmsQueue(
                        () -> uncheckCall(new ActiveMQConnectionFactory(ActiveMQBroker.BROKER_URL)::createConnection),
                        connection -> uncheckCall(() -> connection.createSession(false, AUTO_ACKNOWLEDGE)),
                        (session, message) -> uncheckCall(() -> {
                            TextMessage textMessage = session.createTextMessage(message.getText());
                            textMessage.setBooleanProperty("isActive", true);
                            textMessage.setJMSPriority(8);
                            return textMessage;
                        }),
                        (messageProducer, message) -> uncheckRun(() -> messageProducer.send(message)),
                        session -> identity(),
                        OUTPUT_QUEUE));

        Job job = jet.newJob(pipeline);

        SECONDS.sleep(10);

        cancelJob(job);

        producer.stop();
        ActiveMQBroker.stop();
        jet.shutdown();
    }

    private static void cancelJob(Job job) throws InterruptedException {
        job.cancel();
        while (job.getStatus() != JobStatus.COMPLETED) {
            SECONDS.sleep(1);
        }
    }

}
