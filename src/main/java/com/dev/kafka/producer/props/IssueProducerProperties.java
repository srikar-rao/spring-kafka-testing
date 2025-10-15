package com.dev.kafka.producer.props;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka.issue-producer")
@Getter
@Setter
public class IssueProducerProperties {
  private String bootstrapServers;
  private String topic;
  private String schemaRegistryUrl;
  private boolean autoRegisterSchemas = false;
}
