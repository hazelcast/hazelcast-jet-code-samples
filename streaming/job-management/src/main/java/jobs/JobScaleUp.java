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
import com.hazelcast.jet.core.JobStatus;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.Sources;

import static com.hazelcast.jet.JournalInitialPosition.START_FROM_OLDEST;
import static com.hazelcast.jet.pipeline.Sinks.list;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * We demonstrate how a job can be scaled up
 * after adding new nodes to the Jet cluster
 */
public class JobScaleUp {

    public static void main(String[] args) throws InterruptedException {
        JetConfig config = new JetConfig();
        config.getHazelcastConfig().getMapEventJournalConfig("source").setEnabled(true);
        JetInstance instance1 = Jet.newJetInstance(config);
        JetInstance instance2 = Jet.newJetInstance(config);

        Pipeline p = Pipeline.create();
        p.drawFrom(Sources.<Integer, Integer>mapJournal("source", START_FROM_OLDEST))
                .drainTo(list("sink"));

        Job job = instance1.newJob(p);

        // we wait until the job starts running
        while (job.getStatus() != JobStatus.RUNNING) {
            Thread.sleep(1);
        }

        // we add a new node to the cluster.
        JetInstance instance3 = Jet.newJetInstance(config);

        // we call the restart() method to scale up the job
        boolean restarted = job.restart();
        assert restarted;

        // from now on, the job is running on 3 nodes
        Thread.sleep(SECONDS.toMillis(10));

        job.cancel();

        instance1.shutdown();
        instance2.shutdown();
        instance3.shutdown();
    }

}
