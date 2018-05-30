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

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * A pipeline which streams messages from a JMS queue, filters them according
 * to the priority and writes a new message with modified properties to another
 * JMS queue.
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
                // print the message text to the log
                .peek(message -> uncheckCall(message::getText))
                .drainTo(Sinks.<TextMessage>jmsQueueBuilder(() -> new ActiveMQConnectionFactory(ActiveMQBroker.BROKER_URL))
                        .destinationName(OUTPUT_QUEUE)
                        .messageFn((session, message) ->
                                uncheckCall(() -> {
                                    // create new text message with the same text and few additional properties
                                    TextMessage textMessage = session.createTextMessage(message.getText());
                                    textMessage.setBooleanProperty("isActive", true);
                                    textMessage.setJMSPriority(8);
                                    return textMessage;
                                })
                        )
                        .build());

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
