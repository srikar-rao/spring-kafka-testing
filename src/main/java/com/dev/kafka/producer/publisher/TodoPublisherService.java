package com.dev.kafka.producer.publisher;

import com.dev.kafka.events.producer.todo.Todo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class TodoPublisherService {

  private static final Logger log = LoggerFactory.getLogger(TodoPublisherService.class);

  private final KafkaTemplate<String, Todo> kafkaTemplate;
  private final String topic;

  public TodoPublisherService(KafkaTemplate<String, Todo> kafkaTemplate,
                              @Value("${kafka.todo-producer.topic}") String topic) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
  }

  public void publish(Todo todo) {
    kafkaTemplate.send(topic, todo).whenComplete((result, ex) -> {
      if (ex != null) {
        log.error("Failed to publish todo with id={}", todo.getId(), ex);
      } else {
        log.info("Published todo with id={} to topic={} partition={} offset={}",
            todo.getId(),
            result.getRecordMetadata().topic(),
            result.getRecordMetadata().partition(),
            result.getRecordMetadata().offset());
      }
    });
  }
}
