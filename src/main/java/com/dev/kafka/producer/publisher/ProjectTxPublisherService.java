package com.dev.kafka.producer.publisher;

import com.dev.kafka.events.Project;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class ProjectTxPublisherService {

  private static final Logger log = LoggerFactory.getLogger(ProjectTxPublisherService.class);

  private final KafkaTemplate<String, Project> kafkaTemplate;
  private final String topic;

  public ProjectTxPublisherService(KafkaTemplate<String, Project> projectTxKafkaTemplate,
                                   @Value("${kafka.project-producer.topic}") String topic) {
    this.kafkaTemplate = projectTxKafkaTemplate;
    this.topic = topic;
  }

  public void publish(Project project) {
    kafkaTemplate.executeInTransaction(ops -> {
      ops.send(topic, project);
      return null;
    });
    log.info("Published Project in transaction id={}", project.getId());
  }
}
