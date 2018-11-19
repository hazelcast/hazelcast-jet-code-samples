# Configuring Hazelcast Jet Enterprise with SSL

You have two options to configure Hazelcast Jet Enterprise with SSL:
- Programmatic
    - [Member](src/main/java/member/ProgrammaticConfiguration.java)
    - [Client](src/main/java/client/ProgrammaticConfiguration.java) 
- Declarative
    - [Member](src/main/java/member/DeclarativeConfiguration.java)
    - [Client](src/main/java/client/DeclarativeConfiguration.java)
 

To create a keystore and truststore, the following commands are used:

```bash
keytool -genkey -alias hazelcast-jet -keyalg RSA -keypass password -keystore hazelcast-jet.ks -storepass password
keytool -export -alias hazelcast-jet -file hazelcast-jet.cer -keystore hazelcast-jet.ks -storepass password
keytool -import -v -trustcacerts -alias hazelcast-jet -keypass password -file hazelcast-jet.cer -keystore hazelcast-jet.ts -storepass password
```