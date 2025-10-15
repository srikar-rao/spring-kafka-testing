package com.dev.kafka.service;

import com.dev.kafka.dto.TodoRequest;
import com.dev.kafka.dto.TodoResponse;
import com.dev.kafka.entity.TodoEntity;
import com.dev.kafka.repository.TodoRepository;
import com.dev.kafka.service.impl.TodoServiceImpl;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class TodoServiceImplTest {

  private TodoRepository repository;
  private TodoService service;

  @BeforeEach
  void setUp() {
    repository = mock(TodoRepository.class);
    service = new TodoServiceImpl(repository);
  }

  @Test
  void create_shouldPersistAndReturnResponse() {
    when(repository.save(any(TodoEntity.class))).thenAnswer(inv -> {
      TodoEntity t = inv.getArgument(0);
      t.setId("1");
      return t;
    });

    TodoResponse resp = service.create(TodoRequest.builder().title("Write tests").completed(false).build());

    assertThat(resp.getId()).isEqualTo("1");
    assertThat(resp.getTitle()).isEqualTo("Write tests");
    assertThat(resp.isCompleted()).isFalse();
  }

  @Test
  void findAll_shouldMapEntities() {
    when(repository.findAll()).thenReturn(List.of(
        TodoEntity.builder().id("1").title("A").completed(false).build(),
        TodoEntity.builder().id("2").title("B").completed(true).build()
    ));

    List<TodoResponse> list = service.findAll();
    assertThat(list).hasSize(2);
    assertThat(list.get(1).isCompleted()).isTrue();
  }

  @Test
  void findById_shouldReturnEmptyWhenMissing() {
    when(repository.findById("42")).thenReturn(Optional.empty());
    assertThat(service.findById("42")).isEmpty();
  }

  @Test
  void update_shouldUpdateFields() {
    when(repository.findById("1")).thenReturn(Optional.of(TodoEntity.builder().id("1").title("Old").completed(false).build()))
        .thenReturn(Optional.of(TodoEntity.builder().id("1").title("New").completed(true).build()));
    when(repository.save(any(TodoEntity.class))).thenAnswer(inv -> inv.getArgument(0));

    Optional<TodoResponse> resp = service.update("1", TodoRequest.builder().title("New").completed(true).build());
    assertThat(resp).isPresent();
    assertThat(resp.get().getTitle()).isEqualTo("New");
    assertThat(resp.get().isCompleted()).isTrue();
  }

  @Test
  void setCompleted_shouldFlipFlag() {
    when(repository.findById("1")).thenReturn(Optional.of(TodoEntity.builder().id("1").title("A").completed(false).build()));
    when(repository.save(any(TodoEntity.class))).thenAnswer(inv -> inv.getArgument(0));

    Optional<TodoResponse> resp = service.setCompleted("1", true);
    assertThat(resp).isPresent();
    assertThat(resp.get().isCompleted()).isTrue();
  }

  @Test
  void delete_shouldReturnFalseWhenMissing() {
    when(repository.existsById("99")).thenReturn(false);
    assertThat(service.delete("99")).isFalse();
  }

  @Test
  void delete_shouldInvokeRepositoryWhenPresent() {
    when(repository.existsById("1")).thenReturn(true);
    boolean result = service.delete("1");
    assertThat(result).isTrue();
    verify(repository).deleteById("1");
  }
}
