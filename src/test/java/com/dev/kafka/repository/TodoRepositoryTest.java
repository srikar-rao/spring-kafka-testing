package com.dev.kafka.repository;

import com.dev.kafka.entity.TodoEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest
class TodoRepositoryTest {

  @Autowired
  private TodoRepository repository;

  @Test
  void save_and_findById_shouldWork() {
    TodoEntity saved = repository.save(TodoEntity.builder().title("Repo test").completed(false).build());
    assertThat(saved.getId()).isNotBlank();

    var found = repository.findById(saved.getId());
    assertThat(found).isPresent();
    assertThat(found.get().getTitle()).isEqualTo("Repo test");
  }

  @Test
  void existsById_shouldReturnFalseForMissing() {
    assertThat(repository.existsById("missing-id")).isFalse();
  }
}
