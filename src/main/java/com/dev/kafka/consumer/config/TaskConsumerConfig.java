package com.dev.kafka.consumer.config;

import com.dev.kafka.consumer.props.KafkaConsumerProperties;
import com.dev.kafka.events.consumer.task.Task;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@RequiredArgsConstructor
public class TaskConsumerConfig {

    private final Environment environment;
    @Value("${kafka.task.schema-registry-url:http://localhost:8081}")
    private String schemaRegistryUrl;

    private final KafkaConsumerProperties kafkaConsumerProperties;
    @Value("${kafka.task.auto-register-schemas:false}")
    private boolean autoRegisterSchemas;

    @Bean
    public ConsumerFactory<String, Task> taskConsumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConsumerProperties.getTask().getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaConsumerProperties.getTask().getConsumer().getGroupId());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConsumerProperties.getTask().getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        boolean isTestProfile = Arrays.asList(environment.getActiveProfiles()).contains("test");

        if (isTestProfile) {
            // Use JsonDeserializer for tests
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
            props.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
            props.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Task.class.getName());
        } else {
            // Use KafkaJsonSchemaDeserializer for production
            props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, autoRegisterSchemas);
            props.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, Task.class.getName());
        }

        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean(name = "taskKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Task> taskKafkaListenerContainerFactory() {
        return createTaskListenerContainerFactory();
    }

    @Bean
    public ProducerFactory<String, Task> taskRetryDltProducerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConsumerProperties.getTask().getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        boolean isTestProfile = Arrays.asList(environment.getActiveProfiles()).contains("test");

        if (isTestProfile) {
            // Use JsonSerializer for tests
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
            props.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        } else {
            // Use KafkaJsonSchemaSerializer for production
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
            props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
            props.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, autoRegisterSchemas);
        }

        return new DefaultKafkaProducerFactory<>(props);
    }

    @Bean(name = "taskRetryDltTemplate")
    public KafkaTemplate<String, Task> taskRetryDltTemplate() {
        return new KafkaTemplate<>(taskRetryDltProducerFactory());
    }

    // Listener container factory referenced by @RetryableTopic(listenerContainerFactory = "taskRetryKafkaListener")
    @Bean(name = "taskRetryKafkaListener")
    public ConcurrentKafkaListenerContainerFactory<String, Task> taskRetryKafkaListener() {
        return createTaskListenerContainerFactory();
    }

    /**
     * Creates a configured Kafka listener container factory for Task messages
     * with manual acknowledgment mode.
     *
     * @return configured container factory
     */
    private ConcurrentKafkaListenerContainerFactory<String, Task> createTaskListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Task> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(taskConsumerFactory());
        factory.getContainerProperties().setAckMode(org.springframework.kafka.listener.ContainerProperties.AckMode.MANUAL);
        return factory;
    }


}
