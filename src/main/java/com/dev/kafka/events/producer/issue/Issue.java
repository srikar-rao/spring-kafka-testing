package com.dev.kafka.events.producer.issue;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.NoArgsConstructor;
import lombok.Value;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;

@Value
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor(force = true)
public class Issue {
  @NotNull
  @JsonProperty(required = true)
  String id;

  @NotNull
  @JsonProperty(required = true)
  String description;

  @NotNull
  @JsonProperty(required = true)
  IssueStatus status;

  Priority priority; // nullable

  @NotNull
  @JsonProperty(required = true)
  Instant createdAt;

  Instant dueAt; // nullable

  // Additional date/time representations
  LocalDate reportedOn; // nullable
  OffsetDateTime lastUpdatedAt; // nullable
}
