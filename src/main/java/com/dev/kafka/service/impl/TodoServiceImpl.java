package com.dev.kafka.service.impl;

import com.dev.kafka.dto.TodoRequest;
import com.dev.kafka.dto.TodoResponse;
import com.dev.kafka.entity.TodoEntity;
import com.dev.kafka.repository.TodoRepository;
import com.dev.kafka.service.TodoService;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Optional;

@Service
@Transactional
public class TodoServiceImpl implements TodoService {

  private final TodoRepository repository;

  public TodoServiceImpl(TodoRepository repository) {
    this.repository = repository;
  }

  @Override
  public TodoResponse create(TodoRequest request) {
    boolean completed = request.getCompleted() != null && request.getCompleted();
    TodoEntity saved = repository.save(TodoEntity.builder()
        .title(request.getTitle())
        .completed(completed)
        .build());
    return toResponse(saved);
  }

  @Override
  @Transactional(readOnly = true)
  public List<TodoResponse> findAll() {
    return repository.findAll().stream().map(this::toResponse).toList();
  }

  @Override
  @Transactional(readOnly = true)
  public Optional<TodoResponse> findById(String id) {
    return repository.findById(id).map(this::toResponse);
  }

  @Override
  public Optional<TodoResponse> update(String id, TodoRequest request) {
    return repository.findById(id).map(existing -> {
      existing.setTitle(request.getTitle());
      if (request.getCompleted() != null) {
        existing.setCompleted(request.getCompleted());
      }
      return toResponse(repository.save(existing));
    });
  }

  @Override
  public Optional<TodoResponse> setCompleted(String id, boolean completed) {
    return repository.findById(id).map(existing -> {
      existing.setCompleted(completed);
      return toResponse(repository.save(existing));
    });
  }

  @Override
  public boolean delete(String id) {
    if (!repository.existsById(id)) return false;
    repository.deleteById(id);
    return true;
  }

  private TodoResponse toResponse(TodoEntity todo) {
    return TodoResponse.builder()
        .id(todo.getId())
        .title(todo.getTitle())
        .completed(todo.isCompleted())
        .build();
  }
}
