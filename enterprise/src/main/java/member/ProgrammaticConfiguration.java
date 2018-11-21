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

package member;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;

/**
 * Demonstrates configuring Hazelcast Jet Enterprise with SSL programmatically.
 */
public class ProgrammaticConfiguration {

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");

        JetConfig jetConfig = new JetConfig();
        Config config = jetConfig.getHazelcastConfig();
        config.setLicenseKey("--- SET LICENSE KEY HERE ---");
        NetworkConfig networkConfig = config.getNetworkConfig();
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setEnabled(true)
                 .setFactoryClassName(BasicSSLContextFactory.class.getName())
                 .setProperty("keyStore", "enterprise/hazelcast-jet.ks")
                 .setProperty("keyStorePassword", "password")
                 .setProperty("trustStore", "enterprise/hazelcast-jet.ts")
                 .setProperty("trustStorePassword", "password");
        networkConfig.setSSLConfig(sslConfig);

        JetInstance jet1 = Jet.newJetInstance(jetConfig);
        JetInstance jet2 = Jet.newJetInstance(jetConfig);
    }
}
