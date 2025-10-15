package com.dev.kafka.producer;

import com.dev.kafka.events.producer.issue.Issue;
import com.dev.kafka.events.producer.issue.IssueStatus;
import com.dev.kafka.events.producer.issue.Priority;
import com.dev.kafka.producer.publisher.IssuePublisherService;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@EmbeddedKafka(partitions = 1, topics = {"issue-topic"})
@TestPropertySource(properties = {
        // Point the application producer (IssueKafkaProducerConfig) to mock registry with auto-register disabled
        "kafka.issue-producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "kafka.issue-producer.topic=issue-topic",
        "kafka.issue-producer.schema-registry-url=mock://issue-registry",
        "kafka.issue-producer.auto-register-schemas=false"
})
class IssuePublisherServiceIT {

  @Autowired
  private IssuePublisherService issuePublisherService;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafka;

  private org.apache.kafka.clients.consumer.Consumer<String, Issue> consumer;

    @BeforeEach
  void setUp() {
    // Use scoped MockSchemaRegistry client that matches the mock:// URL scope
        SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("issue-registry");
    // Pre-register strict schema (prod-like)
    String subject = "issue-topic-value";
    String issueJsonSchema = """
    {
      "$schema": "http://json-schema.org/draft-07/schema#",
      "title": "Issue",
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "id": { "type": "string" },
        "description": { "type": "string" },
        "status": { "type": "string", "enum": ["OPEN", "IN_PROGRESS", "CLOSED"] },
        "priority": {
          "oneOf": [
            { "type": "null", "title": "Not included" },
            { "type": "string", "enum": ["LOW", "MEDIUM", "HIGH"] }
          ]
        },
        "createdAt": { "type": "number" },
        "dueAt": {
          "oneOf": [
            { "type": "null", "title": "Not included" },
            { "type": "number" }
          ]
        },
        "reportedOn": {
          "oneOf": [
            { "type": "null", "title": "Not included" },
            { "type": "array", "items": { "type": "integer" } }
          ]
        },
        "lastUpdatedAt": {
          "oneOf": [
            { "type": "null", "title": "Not included" },
            { "type": "number" }
          ]
        }
      },
      "required": ["id", "description", "status", "createdAt"]
    }
    """;
    try {
      schemaRegistryClient.register(subject, new JsonSchema(issueJsonSchema));
    } catch (Exception e) {
      throw new RuntimeException("Failed to register Issue schema in mock registry", e);
    }

    // Set up the consumer
    Map<String, Object> consumerProps = new HashMap<>(
        KafkaTestUtils.consumerProps("issue-consumer-it", "false", embeddedKafka)
    );
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
    consumerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://issue-registry");
    consumerProps.put(KafkaJsonSchemaDeserializerConfig.JSON_VALUE_TYPE, Issue.class);

    ConsumerFactory<String, Issue> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
    consumer = cf.createConsumer();
    embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "issue-topic");
  }

  @AfterEach
  void tearDown() throws Exception {
    if (consumer != null) {
      consumer.close();
    }
    // Drop scope to avoid cross-test interference
    MockSchemaRegistry.dropScope("issue-registry");
  }

  @Test
  void publish_shouldSendIssueToTopic() {
    Issue issue = Issue.builder()
        .id("17")
        .description("Fix intermittent test")
        .status(IssueStatus.OPEN)
        .createdAt(Instant.parse("2025-01-01T00:00:00Z"))
        .dueAt(Instant.parse("2025-12-31T23:59:59Z"))
        .priority(Priority.HIGH)
        .build();

    issuePublisherService.publish(issue);

    var record = KafkaTestUtils.getSingleRecord(consumer, "issue-topic", Duration.ofSeconds(10));
    assertThat(record).isNotNull();
    assertThat(record.key()).isNull();
    Issue value = record.value();
    assertThat(value.getId()).isEqualTo(issue.getId());
    assertThat(value.getDescription()).isEqualTo(issue.getDescription());
    assertThat(value.getStatus()).isEqualTo(issue.getStatus());
    assertThat(value.getPriority()).isEqualTo(issue.getPriority());
  }

  @Test
    void publish_shouldFailWhenNullId() {
        Issue issue = Issue.builder()
                .description("Fix intermittent test")
                .status(IssueStatus.OPEN)
                .createdAt(Instant.parse("2025-01-01T00:00:00Z"))
                .priority(Priority.HIGH)
                .build();

        Assertions.assertThrows(SerializationException.class, () -> issuePublisherService.publish(issue));
    }

}
