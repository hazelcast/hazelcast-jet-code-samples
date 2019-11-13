/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.nio.ssl.BasicSSLContextFactory;

/**
 * Demonstrates configuring Hazelcast Jet Client with SSL programmatically.
 */
public class ProgrammaticConfiguration {

    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("jet");
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        SSLConfig sslConfig = new SSLConfig();
        sslConfig.setEnabled(true)
                 .setFactoryClassName(BasicSSLContextFactory.class.getName())
                 .setProperty("keyStore", "enterprise/hazelcast-jet.ks")
                 .setProperty("keyStorePassword", "password")
                 .setProperty("trustStore", "enterprise/hazelcast-jet.ts")
                 .setProperty("trustStorePassword", "password");
        networkConfig.setSSLConfig(sslConfig);

        JetInstance jetClient = Jet.newJetClient(clientConfig);
    }
}
