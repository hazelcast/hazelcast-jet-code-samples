## Spring Integration

This sample aims to show how to configure Hazelcast Jet 
as a Spring bean. There are two ways to achieve this:

1. [Programmatic](src/main/java/jet/spring/AnnotationBasedConfigurationSample.java) (annotation-based) configuration

2. [Declarative](src/main/java/jet/spring/XmlConfigurationSample.java) (xml) configuration

## @SpringAware annotation

You can enable `@SpringAware` annotation to auto-wire beans to processors. 
You have to configure Hazelcast Jet to use `SpringManagedContext`.
```java
    /**
     * A {@code ManagedContext} implementation bean which enables {@code @SpringAware}
     * annotation for de-serialized objects.
     */
    @Bean
    public ManagedContext managedContext() {
        return new SpringManagedContext();
    }

    /**
     * {@code JetInstance} bean which configured programmatically with {@code SpringManagedContext}
     */
    @Bean
    public JetInstance instance() {
        Config config = new Config()
                .setManagedContext(managedContext());
        JetConfig jetConfig = new JetConfig()
                .setHazelcastConfig(config);
        return Jet.newJetInstance(jetConfig);
    }
```

```xml
    <hz:config>
        ...
        <hz:spring-aware/>
        ...
    </hz:config>    
```