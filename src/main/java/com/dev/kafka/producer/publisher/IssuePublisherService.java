package com.dev.kafka.producer.publisher;

import com.dev.kafka.events.producer.issue.Issue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class IssuePublisherService {

  private static final Logger log = LoggerFactory.getLogger(IssuePublisherService.class);

  private final KafkaTemplate<String, Issue> kafkaTemplate;
  private final String topic;

  public IssuePublisherService(KafkaTemplate<String, Issue> kafkaTemplate,
                               @Value("${kafka.issue-producer.topic}") String topic) {
    this.kafkaTemplate = kafkaTemplate;
    this.topic = topic;
  }

  public void publish(Issue issue) {
    kafkaTemplate.send(topic, issue).whenComplete((result, ex) -> {
      if (ex != null) {
        log.error("Failed to publish issue id={}", issue.getId(), ex);
      } else {
        log.info("Published issue id={} to topic={} partition={} offset={}",
            issue.getId(),
            result.getRecordMetadata().topic(),
            result.getRecordMetadata().partition(),
            result.getRecordMetadata().offset());
      }
    });
  }
}
