package com.dev.kafka.service;

import com.dev.kafka.events.Project;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class ProjectService {
  private static final Logger log = LoggerFactory.getLogger(ProjectService.class);

  public void handle(Project project) {
    // business logic placeholder
    log.info("Handled project id={}, name={}, status={}", project.getId(), project.getName(), project.getStatus());
  }
}
