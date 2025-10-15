package com.dev.kafka.consumer.listener;

import com.dev.kafka.events.Project;
import com.dev.kafka.service.ProjectService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.SameIntervalTopicReuseStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Component
public class ProjectConsumerListener {

    private static final Logger logger = LoggerFactory.getLogger(ProjectConsumerListener.class);

    private final ProjectService projectService;

    public ProjectConsumerListener(ProjectService projectService) {
        this.projectService = projectService;
    }

    // Single EOS-friendly consumer using read_committed isolation and manual acks
    @RetryableTopic(
        attempts = "2", // 1 main + 1 retry
            backoff = @Backoff(delay = 1000),
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            kafkaTemplate = "projectRetryDltTemplate",
            listenerContainerFactory = "projectRetryKafkaListener"
    )
    @KafkaListener(
        topics = "project-topic",
        containerFactory = "projectKafkaListenerContainerFactory"
    )
    public void listen(Project project, Acknowledgment ack) {
        logger.info("Received project message: id={}, name={}, status={}", project.getId(), project.getName(), project.getStatus());
        projectService.handle(project);
        // only acknowledge after successful processing; on exception, framework will route to retry/DLT
        ack.acknowledge();
    }

    @DltHandler
    public void dlt(Project project, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.error("Routed to DLT on topic {} for project id={}, name={}, status={}", topic, project.getId(), project.getName(), project.getStatus());
    }
}
