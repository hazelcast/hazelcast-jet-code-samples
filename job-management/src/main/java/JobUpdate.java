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

import com.hazelcast.jet.IMapJet;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.JobStateSnapshot;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;

import java.util.Map.Entry;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.jet.pipeline.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.logger;

/**
 * This sample demonstrates how a job's state can be saved and restored to another
 * job. This allows the pipeline to be modified and the job to be
 * updated to a newer version of the pipeline without losing any state.
 *
 * In this sample, we have a producer thread writing integers to a map and a job
 * which reads these integers through the event journal and logs them. We will
 * stop the initial job while preserving its state and add a filtering stage which
 * filters out odd integers. The new job will be started from the saved state but
 * using the newer version of the pipeline.
 */
public class JobUpdate {

    private static Pipeline createInitialPipeline() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Integer, Integer>mapJournal("source", START_FROM_OLDEST))
                .map(Entry::getValue)
                .drainTo(logger());
        return p;
    }

    private static Pipeline createUpdatedPipeline() {
        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Integer, Integer>mapJournal("source", START_FROM_OLDEST))
                .map(Entry::getValue)
                .filter(e -> e % 2 == 0)
                .drainTo(logger());
        return p;
    }

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getMapEventJournalConfig("source")
                .setCapacity(100_000)
                .setEnabled(true);
        JetInstance instance1 = Jet.newJetInstance(config);
        JetInstance instance2 = Jet.newJetInstance(config);

        // start the producer
        Thread producerThread = new Thread(() -> {
            IMapJet<Integer, Integer> map = instance1.getMap("source");
            for (int i = 0; i < 1_000; i++) {
                map.put(i, i);
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(250));
            }
        });
        producerThread.setDaemon(true);
        producerThread.start();

        // start the job
        JobConfig jobConfig = new JobConfig().setName("initial");
        Job job = instance1.newJob(createInitialPipeline(), jobConfig);

        // let the job run for a while
        Thread.sleep(5000);

        // job will be saved and a named snapshot will be created
        System.out.println("Stopping existing job and saving a snapshot");
        JobStateSnapshot snapshot = job.cancelAndExportSnapshot("first");

        // after starting the job again we should see even numbers filtered out, but
        // the job continue from where it left off
        System.out.println("Starting job with new pipeline");
        jobConfig.setInitialSnapshotName(snapshot.name())
                .setName("updated");

        Job updatedJob = instance1.newJob(createUpdatedPipeline(), jobConfig);

        Thread.sleep(20_000L);

        System.out.println("Cancelling job and shutting down instances");

        updatedJob.cancel();

        try {
            updatedJob.join();
        } catch (CancellationException e) {

        }
        instance1.shutdown();
        instance2.shutdown();
    }
}
