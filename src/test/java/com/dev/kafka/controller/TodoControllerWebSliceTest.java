package com.dev.kafka.controller;

import com.dev.kafka.dto.TodoRequest;
import com.dev.kafka.dto.TodoResponse;
import com.dev.kafka.service.TodoService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(TodoController.class)
class TodoControllerWebSliceTest {

  @Autowired MockMvc mockMvc;
  @Autowired ObjectMapper objectMapper;

  @MockitoBean TodoService todoService;

  @Test
  void create_shouldReturn201() throws Exception {
    TodoRequest req = TodoRequest.builder().title("Learn Kafka").completed(false).build();
    TodoResponse resp = TodoResponse.builder().id("1").title("Learn Kafka").completed(false).build();
    when(todoService.create(any(TodoRequest.class))).thenReturn(resp);

    mockMvc.perform(post("/api/todos")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(req)))
        .andExpect(status().isCreated())
        .andExpect(header().string("Location", "/api/todos/1"))
        .andExpect(jsonPath("$.id", is("1")))
        .andExpect(jsonPath("$.title", is("Learn Kafka")))
        .andExpect(jsonPath("$.completed", is(false)));
  }

  @Test
  void list_shouldReturnArray() throws Exception {
    when(todoService.findAll()).thenReturn(List.of(
        TodoResponse.builder().id("1").title("A").completed(false).build(),
        TodoResponse.builder().id("2").title("B").completed(true).build()
    ));

    mockMvc.perform(get("/api/todos"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$[0].id", is("1")))
        .andExpect(jsonPath("$[1].completed", is(true)));
  }

  @Test
  void get_shouldReturn404_whenNotFound() throws Exception {
    when(todoService.findById("99")).thenReturn(Optional.empty());
    mockMvc.perform(get("/api/todos/99"))
        .andExpect(status().isNotFound());
  }

  @Test
  void update_shouldReturn200() throws Exception {
    TodoRequest req = TodoRequest.builder().title("Updated").completed(true).build();
    when(todoService.update(eq("1"), any(TodoRequest.class)))
        .thenReturn(Optional.of(TodoResponse.builder().id("1").title("Updated").completed(true).build()));

    mockMvc.perform(put("/api/todos/1")
            .contentType(MediaType.APPLICATION_JSON)
            .content(objectMapper.writeValueAsString(req)))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.completed", is(true)));
  }

  @Test
  void complete_shouldReturn200() throws Exception {
    when(todoService.setCompleted("1", true))
        .thenReturn(Optional.of(TodoResponse.builder().id("1").title("A").completed(true).build()));

    mockMvc.perform(patch("/api/todos/1/complete"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.completed", is(true)));
  }

  @Test
  void delete_shouldReturn204_whenDeleted() throws Exception {
    when(todoService.delete("1")).thenReturn(true);
    mockMvc.perform(delete("/api/todos/1"))
        .andExpect(status().isNoContent());
  }
}
