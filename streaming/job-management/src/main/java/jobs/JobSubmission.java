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

package jobs;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.core.WatermarkGenerationParams.noWatermarks;

/**
 * We demonstrate how a job can be submitted to Jet
 * and further managed via the {@link Job} interface.
 */
public class JobSubmission {

    public static void main(String[] args) throws InterruptedException {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getMapEventJournalConfig("source").setEnabled(true);
        JetInstance instance1 = Jet.newJetInstance(config);
        JetInstance instance2 = Jet.newJetInstance(config);

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Integer, Integer>mapJournal("source", START_FROM_OLDEST))
                .drainTo(Sinks.list("sink"));

        JobConfig jobConfig = new JobConfig();
        // job name is optional...
        String jobName = "sample";
        jobConfig.setName(jobName);

        Job job = instance1.newJob(p, jobConfig);

        // printing the job name
        System.out.println("Job: " + job.getName() + " is submitted...");

        // job status can be queried. let's wait until the job starts running
        JobStatus status = job.getStatus();
        while (status == JobStatus.NOT_STARTED || status == JobStatus.STARTING) {
            System.out.println("Job is starting...");
            Thread.sleep(1);
            status = job.getStatus();
        }

        System.out.println("Job is started. STATUS: " + job.getStatus());

        // we can cancel the job
        job.cancel();

        try {
            // let's wait until execution of the job is completed on the cluster
            // we can also call job.getFuture().get()
            job.join();
            assert false;
        } catch (CancellationException e) {
            System.out.println("Job is cancelled...");
        }

        // let's query the job status again. Now the status is COMPLETED
        System.out.println("Job is completed. STATUS: " + job.getStatus());

        instance1.shutdown();
        instance2.shutdown();
    }
}
