package com.dev.kafka.producer.config;

import com.dev.kafka.events.producer.issue.Issue;
import com.dev.kafka.producer.props.IssueProducerProperties;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(IssueProducerProperties.class)
@RequiredArgsConstructor
public class IssueKafkaProducerConfig {

  private final IssueProducerProperties properties;

  @Bean
  public ProducerFactory<String, Issue> issueProducerFactory() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
    configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, properties.getSchemaRegistryUrl());
    configs.put("auto.register.schemas", properties.isAutoRegisterSchemas());
      // Subject naming: keep default TopicNameStrategy so subjects are <topic>-value
      configs.put(AbstractKafkaSchemaSerDeConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());

      // Schema resolution: prefer latest version when writing (use carefully in prod rollouts)
      configs.put("use.latest.version", false);
      configs.put("latest.compatibility.strict", false);

      // JSON Schema derivation/validation options
      // Use jakarta validation annotations on POJOs to influence generated schema
      configs.put("json.use.validation.annotations", true);
      // Fail fast if payload doesn't conform to the JSON Schema
      configs.put("json.fail.invalid.schema", true);
      // Emit nullability using oneOf [null, type] (default true). Keep explicit for clarity.
      configs.put("json.oneof.for.nullables", true);

      return new DefaultKafkaProducerFactory<>(configs);
  }

  @Bean
  public KafkaTemplate<String, Issue> issueKafkaTemplate() {
    return new KafkaTemplate<>(issueProducerFactory());
  }
}
