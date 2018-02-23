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

package jet.spring;

import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Pipeline;
import com.hazelcast.jet.Sinks;
import com.hazelcast.jet.config.JobConfig;
import jet.spring.config.AppConfig;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Example of integration Hazelcast Jet with Spring annotation-based config.
 * We create spring context from annotations using {@code AppConfig} class
 * for configuration, obtain {@code JetInstance} bean from context and submit a job.
 * <p>
 * Job uses a custom source implementation which has {@link com.hazelcast.spring.context.SpringAware}
 * annotation. This enables spring to auto-wire beans to created processors.
 */
public class AnnotationBasedConfigurationSample {

    public static void main(String[] args) {
        ApplicationContext context = new AnnotationConfigApplicationContext(AppConfig.class);
        JetInstance jet = context.getBean(JetInstance.class);

        Pipeline pipeline = Pipeline.create();
        pipeline.drawFrom(CustomSourceP.customSource())
                .drainTo(Sinks.logger());

        JobConfig jobConfig = new JobConfig()
                .addClass(AnnotationBasedConfigurationSample.class)
                .addClass(CustomSourceP.class);
        jet.newJob(pipeline, jobConfig).join();

        jet.shutdown();
    }
}
