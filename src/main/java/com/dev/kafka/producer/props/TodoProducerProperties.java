package com.dev.kafka.producer.props;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kafka.todo-producer")
@Getter
@Setter
public class TodoProducerProperties {
  private String bootstrapServers;
  private String topic;
}
