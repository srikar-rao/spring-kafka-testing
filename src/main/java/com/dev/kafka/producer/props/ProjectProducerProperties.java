package com.dev.kafka.producer.props;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka.project-producer")
@Getter
@Setter
public class ProjectProducerProperties {
  private String bootstrapServers;
  private String topic;
  private String transactionIdPrefix;
}
