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
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sinks;
import com.hazelcast.jet.pipeline.Sources;

import java.util.List;
import java.util.concurrent.CancellationException;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static java.util.Objects.requireNonNull;

/**
 * We demonstrate how submitted jobs can be fetched
 * and tracked via any Jet instance.
 */
public class JobTracking {

    public static void main(String[] args) {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getMapEventJournalConfig("source").setEnabled(true);
        JetInstance instance1 = Jet.newJetInstance(config);
        JetInstance instance2 = Jet.newJetInstance(config);

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Integer, Integer>mapJournal("source", START_FROM_OLDEST))
                .drainTo(Sinks.list("sink"));

        JobConfig jobConfig = new JobConfig();
        // job name is optional..
        String jobName = "sample";
        jobConfig.setName(jobName);

        instance1.newJob(p, jobConfig);

        // jobs can be also tracked via other Jet nodes
        List<Job> jobs = instance2.getJobs();

        Job trackedJob1 = jobs.get(0);

        // job status can be queried via the tracked job object
        System.out.println("Tracked job: " + trackedJob1.getName() + " -> STATUS: " + trackedJob1.getStatus());

        // we can use the tracked job object to cancel the job
        trackedJob1.cancel();

        try {
            // let's wait until execution of the job is completed on the cluster
            trackedJob1.join();
            assert false;
        } catch (CancellationException e) {
            System.out.println("Job is cancelled...");
        }

        // let's query the job status again. Now the status is COMPLETED
        System.out.println("Job is completed. STATUS: " + trackedJob1.getStatus());

        // running or completed jobs can be also queried by name
        Job trackedJob2 = requireNonNull(instance1.getJob(jobName));
        System.out.println("Tracked job STATUS: " + trackedJob2.getStatus());

        instance1.shutdown();
        instance2.shutdown();
    }
}
