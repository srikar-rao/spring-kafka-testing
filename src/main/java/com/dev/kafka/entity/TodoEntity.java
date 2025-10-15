package com.dev.kafka.entity;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Entity
@Table(name = "todos")
public class TodoEntity {
  @Id
  @Column(length = 40)
  private String id;

  @NotBlank
  @Column(nullable = false)
  private String title;

  @Column(nullable = false)
  private boolean completed;

  @PrePersist
  void prePersist() {
    if (this.id == null || this.id.isBlank()) {
      this.id = UUID.randomUUID().toString();
    }
  }
}
