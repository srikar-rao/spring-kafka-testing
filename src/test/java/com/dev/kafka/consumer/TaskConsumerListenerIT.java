package com.dev.kafka.consumer;

import com.dev.kafka.consumer.listener.TaskConsumerListener;
import com.dev.kafka.consumer.processor.TaskProcessor;
import com.dev.kafka.events.consumer.task.Status;
import com.dev.kafka.events.consumer.task.Task;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.time.Instant;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;

import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"task-topic", "task-topic-retry", "task-topic-dlt"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
@TestPropertySource(properties = {
    "kafka.task.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "kafka.task.consumer.group-id=task-consumer-it",
        "kafka.task.consumer.auto-offset-reset=earliest",
        "kafka.task.schema-registry-url=mock://task-registry",
        "spring.kafka.producer.value-serializer=org.springframework.kafka.support.serializer.JsonSerializer",
        "spring.kafka.consumer.value-deserializer=org.springframework.kafka.support.serializer.JsonDeserializer"
})
class TaskConsumerListenerIT {

    private static final String TOPIC = "task-topic";

    @Autowired
    private EmbeddedKafkaBroker embeddedKafka;

    private KafkaTemplate<String, Task> kafkaTemplate;

    private org.apache.kafka.clients.consumer.Consumer<String, Task> testConsumer;

    @MockitoSpyBean
    private TaskProcessor taskProcessor;

    @MockitoSpyBean
    private TaskConsumerListener taskConsumerListener;

    @BeforeAll
    void setUp() {
        // Use scoped MockSchemaRegistry client that matches the mock:// URL scope
        SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope("task-registry");

        // Pre-register strict schema for Task
        String subject = "task-topic-value";
        String taskJsonSchema = """
                {
                  "$schema": "http://json-schema.org/draft-07/schema#",
                  "title": "Task",
                  "type": "object",
                  "additionalProperties": false,
                  "properties": {
                    "id": { "type": "string" },
                    "name": { "type": "string" },
                    "dueDate": { 
                      "type": "string",
                      "format": "date"
                    },
                    "status": { 
                      "type": "string", 
                      "enum": ["OPEN", "IN_PROGRESS", "CLOSED"] 
                    },
                    "createdAt": { "type": "number" }
                  },
                  "required": ["id", "name", "dueDate", "status", "createdAt"]
                }
                """;

        try {
            schemaRegistryClient.register(subject, new JsonSchema(taskJsonSchema));
        } catch (Exception e) {
            throw new RuntimeException("Failed to register Task schema in mock registry", e);
        }

        // For functional tests, use JsonSerializer/JsonDeserializer
        setupFunctionalTestKafkaTemplate();

        // Set up consumer with JsonDeserializer for functional tests
        Map<String, Object> consumerProps = new HashMap<>(
                KafkaTestUtils.consumerProps("task-consumer-it-assert", "false", embeddedKafka)
        );
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Task.class.getName());
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        ConsumerFactory<String, Task> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
        testConsumer = cf.createConsumer();
        embeddedKafka.consumeFromAnEmbeddedTopic(testConsumer, TOPIC);
    }

    /**
     * Sets up a KafkaTemplate with JsonSerializer for functional tests
     */
    private void setupFunctionalTestKafkaTemplate() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);

        ProducerFactory<String, Task> pf = new DefaultKafkaProducerFactory<>(producerProps);
        kafkaTemplate = new KafkaTemplate<>(pf);
    }

    /**
     * Sets up a KafkaTemplate with KafkaJsonSchemaSerializer for schema validation tests
     */
    private KafkaTemplate<String, Task> createSchemaValidationKafkaTemplate() {
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://task-registry");
        producerProps.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

        ProducerFactory<String, Task> pf = new DefaultKafkaProducerFactory<>(producerProps);
        return new KafkaTemplate<>(pf);
    }

    @Test
    void taskConsumerReceivesMessage() {
        // key sequencing and headers
        String key = "task-key-001";
        Task payload = Task.builder()
                .id("t-1")
                .name("Task A")
                .status(Status.OPEN)
                .dueDate(LocalDate.now().plusDays(7))
                .createdAt(Instant.now())
                .build();

        Message<Task> message = MessageBuilder
                .withPayload(payload)
                .setHeader(KafkaHeaders.TOPIC, TOPIC)
                .setHeader(KafkaHeaders.KEY, key)
                .setHeader("x-test", "task-it")
                .setHeader("x-seq", 1)
                .build();

        kafkaTemplate.send(message);
        kafkaTemplate.flush();

        // verify business handling invoked with matching fields
        await().atMost(ofSeconds(10)).untilAsserted(() -> {
            ArgumentCaptor<Task> captor = forClass(Task.class);
            verify(taskProcessor, atLeastOnce()).process(captor.capture());
            Task captured = captor.getValue();
            assertThat(captured.getId()).isEqualTo(payload.getId());
            assertThat(captured.getName()).isEqualTo(payload.getName());
            assertThat(captured.getStatus()).isEqualTo(payload.getStatus());
        });

        // additionally verify listener method was invoked
        verify(taskConsumerListener, atLeastOnce()).listen(any(Task.class), any(Acknowledgment.class));
    }

    @Test
    void taskMessageRetriesThenSucceeds() {
        // First call fails, second call succeeds
        doThrow(new RuntimeException("boom"))
                .doNothing()
                .when(taskProcessor).process(any(Task.class));

        Task payload = Task.builder()
                .id("retry-ok-1")
                .name("Task Retry OK")
                .status(Status.IN_PROGRESS)
                .dueDate(LocalDate.now().plusDays(3))
                .createdAt(Instant.now())
                .build();

        kafkaTemplate.send(TOPIC, payload);
        kafkaTemplate.flush();

        // Await until listener was invoked twice (main + retry)
        await().atMost(ofSeconds(15)).untilAsserted(() ->
                verify(taskConsumerListener, times(2)).listen(any(Task.class), any(Acknowledgment.class))
        );
    }

    @Test
    void taskMessageRetriesThenDlt() {
        // Force processing failure to exercise retry and DLT
        doThrow(new RuntimeException("boom")).when(taskProcessor).process(any(Task.class));

        Task payload = Task.builder()
                .id("fail-1")
                .name("Task Fail")
                .status(Status.CLOSED)
                .dueDate(LocalDate.now())
                .createdAt(Instant.now())
                .build();

        kafkaTemplate.send(TOPIC, payload);
        kafkaTemplate.flush();

        // Assert via interception that DLT handler was invoked
        await().atMost(ofSeconds(20)).untilAsserted(() ->
                verify(taskConsumerListener, atLeastOnce()).dlt(any(Task.class), any(String.class))
        );
    }

    @AfterAll
    void tearDown() {
        if (kafkaTemplate != null) {
            kafkaTemplate.destroy();
        }
        if (testConsumer != null) {
            testConsumer.close(java.time.Duration.ofSeconds(1));
        }
        // Drop scope to avoid cross-test interference
        MockSchemaRegistry.dropScope("task-registry");
    }

    @Test
    @DisplayName("Should fail when publishing a task with missing required field")
    void publish_shouldFailWhenMissingRequiredField() {
        // Create a Task without a required field (name)
        Task invalidTask = Task.builder()
                .id("invalid-1")
                // name is missing
                .status(Status.OPEN)
                .dueDate(LocalDate.now())
                .createdAt(Instant.now())
                .build();

        // Use schema validation KafkaTemplate for this test
        KafkaTemplate<String, Task> schemaValidationTemplate = createSchemaValidationKafkaTemplate();

        // Sending this message should fail with SerializationException due to schema validation
        Assertions.assertThrows(SerializationException.class, () ->
                schemaValidationTemplate.send(TOPIC, invalidTask)
        );

        schemaValidationTemplate.destroy();
    }

    @Test
    @DisplayName("Should fail when publishing a task with invalid enum value")
    void publish_shouldFailWhenInvalidEnumValue() {
        // We need to create an invalid message that would fail schema validation
        // Since we can't directly create an invalid enum through the Task class,
        // we'll use a raw Map to create a message with an invalid status

        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://task-registry");
        producerProps.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

        // Create a producer that can send raw objects
        ProducerFactory<String, Object> rawPf = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<String, Object> rawKafkaTemplate = new KafkaTemplate<>(rawPf);

        // Create a task with an invalid status value
        Map<String, Object> invalidTask = new HashMap<>();
        invalidTask.put("id", "invalid-status-1");
        invalidTask.put("name", "Task with invalid status");
        invalidTask.put("status", "INVALID_STATUS"); // This is not in the enum
        invalidTask.put("dueDate", LocalDate.now().toString());
        invalidTask.put("createdAt", Instant.now().toEpochMilli());

        // Sending this message should fail with SerializationException due to schema validation
        Assertions.assertThrows(SerializationException.class, () ->
                rawKafkaTemplate.send(TOPIC, invalidTask)
        );

        rawKafkaTemplate.destroy();
    }

    @Test
    @DisplayName("Should fail when publishing a task with additional properties")
    void publish_shouldFailWhenAdditionalProperties() {
        // Create a producer that can send raw objects
        Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class);
        producerProps.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://task-registry");
        producerProps.put(AbstractKafkaSchemaSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

        ProducerFactory<String, Object> rawPf = new DefaultKafkaProducerFactory<>(producerProps);
        KafkaTemplate<String, Object> rawKafkaTemplate = new KafkaTemplate<>(rawPf);

        // Create a task with an additional property not defined in the schema
        Map<String, Object> invalidTask = new HashMap<>();
        invalidTask.put("id", "invalid-props-1");
        invalidTask.put("name", "Task with extra properties");
        invalidTask.put("status", "OPEN");
        invalidTask.put("dueDate", LocalDate.now().toString());
        invalidTask.put("createdAt", Instant.now().toEpochMilli());
        invalidTask.put("extraProperty", "This property is not in the schema"); // Additional property

        // Sending this message should fail with SerializationException due to schema validation
        Assertions.assertThrows(SerializationException.class, () ->
                rawKafkaTemplate.send(TOPIC, invalidTask)
        );

        rawKafkaTemplate.destroy();
    }
}
