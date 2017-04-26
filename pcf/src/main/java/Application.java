/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.json.BasicJsonParser;
import org.springframework.context.annotation.Bean;

import java.util.List;
import java.util.Map;

@SpringBootApplication
public class Application {

    private static ClientConfig clientConfig;

    public static void main(String[] args) {
        String servicesJson = System.getenv("VCAP_SERVICES");
        if (servicesJson == null || servicesJson.isEmpty()) {
            System.err.println("No service found!!!");
            return;
        }
        BasicJsonParser parser = new BasicJsonParser();
        Map<String, Object> json = parser.parseMap(servicesJson);
        List hazelcastJet = (List) json.get("hazelcast-jet");
        Map map = (Map) hazelcastJet.get(0);
        Map credentials = (Map) map.get("credentials");
        String groupName = (String) credentials.get("group_name");
        String groupPassword = (String) credentials.get("group_pass");
        List<String> members = (List<String>) credentials.get("members");

        clientConfig = new ClientConfig();
        GroupConfig groupConfig = clientConfig.getGroupConfig();
        groupConfig.setName(groupName).setPassword(groupPassword);
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        for (String member : members) {
            networkConfig.addAddress(member.replace('"', ' ').trim());
        }
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public JetInstance jetClient() {
        return Jet.newJetClient(clientConfig);
    }
}
