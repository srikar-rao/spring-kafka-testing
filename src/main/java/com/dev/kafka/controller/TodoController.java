package com.dev.kafka.controller;

import com.dev.kafka.dto.TodoRequest;
import com.dev.kafka.dto.TodoResponse;
import com.dev.kafka.service.TodoService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.net.URI;
import java.util.List;

import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/todos")
public class TodoController {

  private final TodoService todoService;

  public TodoController(TodoService todoService) {
    this.todoService = todoService;
  }

  @PostMapping
  public ResponseEntity<TodoResponse> create(@Valid @RequestBody TodoRequest request) {
    TodoResponse created = todoService.create(request);
    return ResponseEntity.created(URI.create("/api/todos/" + created.getId())).body(created);
  }

  @GetMapping
  public ResponseEntity<List<TodoResponse>> list() {
    return ResponseEntity.ok(todoService.findAll());
  }

  @GetMapping("/{id}")
  public ResponseEntity<TodoResponse> get(@PathVariable String id) {
    return todoService.findById(id)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  @PutMapping("/{id}")
  public ResponseEntity<TodoResponse> update(@PathVariable String id, @Valid @RequestBody TodoRequest request) {
    return todoService.update(id, request)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  @PatchMapping("/{id}/complete")
  public ResponseEntity<TodoResponse> complete(@PathVariable String id) {
    return todoService.setCompleted(id, true)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  @PatchMapping("/{id}/incomplete")
  public ResponseEntity<TodoResponse> incomplete(@PathVariable String id) {
    return todoService.setCompleted(id, false)
        .map(ResponseEntity::ok)
        .orElse(ResponseEntity.notFound().build());
  }

  @DeleteMapping("/{id}")
  public ResponseEntity<Void> delete(@PathVariable String id) {
    boolean removed = todoService.delete(id);
    return removed ? ResponseEntity.noContent().build() : ResponseEntity.status(HttpStatus.NOT_FOUND).build();
  }
}
