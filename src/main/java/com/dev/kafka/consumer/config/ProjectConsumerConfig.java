package com.dev.kafka.consumer.config;

import com.dev.kafka.events.Project;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
public class ProjectConsumerConfig {

    @Value("${kafka.project.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${kafka.project.consumer.group-id}")
    private String groupId;

    @Value("${kafka.project.consumer.auto-offset-reset:earliest}")
    private String autoOffsetReset;

    @Bean
    public ConsumerFactory<String, Project> projectConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Project.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "projectKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Project> projectKafkaListenerContainerFactory() {
        return createProjectListenerContainerFactory();
    }

    @Bean
    public ProducerFactory<String, Project> projectRetryDltProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean(name = "projectRetryDltTemplate")
    public KafkaTemplate<String, Project> projectRetryDltTemplate() {
        return new KafkaTemplate<>(projectRetryDltProducerFactory());
    }

    // Listener container factory referenced by @RetryableTopic(listenerContainerFactory = "projectRetryKafkaListener")
    @Bean(name = "projectRetryKafkaListener")
    public ConcurrentKafkaListenerContainerFactory<String, Project> projectRetryKafkaListener() {
        return createProjectListenerContainerFactory();
    }
    
    /**
     * Creates a configured Kafka listener container factory for Project messages
     * with manual acknowledgment mode.
     *
     * @return configured container factory
     */
    private ConcurrentKafkaListenerContainerFactory<String, Project> createProjectListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Project> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(projectConsumerFactory());
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL);
        return factory;
    }

}