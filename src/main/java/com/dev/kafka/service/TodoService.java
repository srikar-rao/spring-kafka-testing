package com.dev.kafka.service;

import com.dev.kafka.dto.TodoRequest;
import com.dev.kafka.dto.TodoResponse;

import java.util.List;
import java.util.Optional;

public interface TodoService {
  TodoResponse create(TodoRequest request);
  List<TodoResponse> findAll();
  Optional<TodoResponse> findById(String id);
  Optional<TodoResponse> update(String id, TodoRequest request);
  Optional<TodoResponse> setCompleted(String id, boolean completed);
  boolean delete(String id);
}
