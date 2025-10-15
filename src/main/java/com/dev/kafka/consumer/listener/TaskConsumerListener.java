package com.dev.kafka.consumer.listener;

import com.dev.kafka.consumer.processor.TaskProcessor;
import com.dev.kafka.events.consumer.task.Task;
import lombok.RequiredArgsConstructor;
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
@RequiredArgsConstructor
public class TaskConsumerListener {

    private static final Logger logger = LoggerFactory.getLogger(TaskConsumerListener.class);

    private final TaskProcessor taskProcessor;

    @RetryableTopic(
            attempts = "2", // 1 main + 1 retry
            backoff = @Backoff(delay = 1000),
            sameIntervalTopicReuseStrategy = SameIntervalTopicReuseStrategy.SINGLE_TOPIC,
            kafkaTemplate = "taskRetryDltTemplate",
            listenerContainerFactory = "taskRetryKafkaListener"
    )
    @KafkaListener(
        topics = "task-topic",
        containerFactory = "taskKafkaListenerContainerFactory"
    )
    public void listen(Task task, Acknowledgment ack) {
        logger.info("Received task message: id={}, name={}, status={}", task.getId(), task.getName(), task.getStatus());
        taskProcessor.process(task);
        // only acknowledge after successful processing; on exception, framework will route to retry/DLT
        ack.acknowledge();
    }

    @DltHandler
    public void dlt(Task task, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        logger.error("Routed to DLT on topic {} for task id={}, name={}, status={}", topic, task.getId(), task.getName(), task.getStatus());
    }
}
