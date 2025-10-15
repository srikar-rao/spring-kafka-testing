package com.dev.kafka.producer;

import com.dev.kafka.events.producer.todo.Todo;
import com.dev.kafka.producer.publisher.TodoPublisherService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.kafka.test.utils.KafkaTestUtils.getSingleRecord;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"todo-topic"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(properties = {
    "kafka.todo-producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "kafka.todo-producer.topic=todo-topic"
})
class TodoPublisherServiceIT {

  @Autowired
  private TodoPublisherService publisherService;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafka;

  private org.apache.kafka.clients.consumer.Consumer<String, Todo> consumer;

  @BeforeAll
  void setUp() {
    Map<String, Object> consumerProps = new HashMap<>(
        KafkaTestUtils.consumerProps("todo-consumer-it", "false", embeddedKafka)
    );
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Todo.class.getName());
    consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

    ConsumerFactory<String, Todo> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
    consumer = cf.createConsumer();
    embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "todo-topic");
  }

  @AfterAll
  void tearDown() {
    if (consumer != null) {
      consumer.close(ofSeconds(1));
    }
  }

  @Test
  void publish_shouldSendTodoToTopic() {
    Todo todo = Todo.builder()
        .id("42")
        .title("Write IT test")
        .completed(false)
        .build();

    publisherService.publish(todo);

    var record = getSingleRecord(consumer, "todo-topic", ofSeconds(10));
    assertThat(record).isNotNull();
    assertThat(record.key()).isNull();
    Todo value = record.value();
    assertThat(value.getId()).isEqualTo(todo.getId());
    assertThat(value.getTitle()).isEqualTo(todo.getTitle());
    assertThat(value.isCompleted()).isEqualTo(todo.isCompleted());
  }
}
