package com.dev.kafka.producer.config;

import com.dev.kafka.events.Project;
import com.dev.kafka.producer.props.ProjectProducerProperties;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(ProjectProducerProperties.class)
@RequiredArgsConstructor
public class ProjectKafkaTxProducerConfig {

  private final ProjectProducerProperties properties;

  @Bean
  public ProducerFactory<String, Project> projectTxProducerFactory() {
    Map<String, Object> configs = new HashMap<>();
    configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
    configs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    configs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    // Ensure idempotence and EOS
    configs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
    configs.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

    DefaultKafkaProducerFactory<String, Project> pf = new DefaultKafkaProducerFactory<>(configs);
    pf.setTransactionIdPrefix(properties.getTransactionIdPrefix());
    return pf;
  }

  @Bean
  public KafkaTemplate<String, Project> projectTxKafkaTemplate() {
    return new KafkaTemplate<>(projectTxProducerFactory());
  }

  // Required by @RetryableTopic to publish to retry/DLT topics when no single KafkaTemplate is present
  @Bean(name = "defaultRetryTopicKafkaTemplate")
  public KafkaTemplate<String, Project> defaultRetryTopicKafkaTemplate() {
    return new KafkaTemplate<>(projectTxProducerFactory());
  }
}
