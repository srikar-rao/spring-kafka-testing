package com.dev.kafka.consumer.processor;

import com.dev.kafka.events.consumer.task.Task;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TaskProcessor {

    public void process(Task task) {
        log.info("Processing task id={}", task.getId());
    }
}
