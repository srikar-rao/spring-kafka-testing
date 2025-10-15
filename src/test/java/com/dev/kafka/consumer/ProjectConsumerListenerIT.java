package com.dev.kafka.consumer;

import com.dev.kafka.consumer.listener.ProjectConsumerListener;
import com.dev.kafka.events.Project;
import com.dev.kafka.service.ProjectService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
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

import java.util.Map;

import static java.time.Duration.ofSeconds;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@DirtiesContext
@EmbeddedKafka(partitions = 1, topics = {"project-topic", "project-topic-retry", "project-topic-dlt"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest
// Using MockitoSpyBean (Boot 3.4+) instead of deprecated SpyBean
@TestPropertySource(properties = {
    "kafka.project.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "kafka.project.consumer.group-id=project-consumer-it",
    "kafka.project.consumer.auto-offset-reset=earliest",
    // ensure retry/DLT publisher uses embedded broker
    "kafka.project-producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "kafka.project-producer.topic=project-topic",
    "kafka.project-producer.transaction-id-prefix=it-retry-tx-"
})
class ProjectConsumerListenerIT {

  private static final String TOPIC = "project-topic";

  @Autowired
  private EmbeddedKafkaBroker embeddedKafka;

  private KafkaTemplate<String, Project> kafkaTemplate;

  private org.apache.kafka.clients.consumer.Consumer<String, Project> testConsumer;

  @MockitoSpyBean
  private ProjectService projectService;

  @MockitoSpyBean
  private ProjectConsumerListener projectConsumerListener;

  @BeforeAll
  void setUp() {
    Map<String, Object> producerProps = KafkaTestUtils.producerProps(embeddedKafka);
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
    // disable type headers to keep JSON clean
    producerProps.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
    ProducerFactory<String, Project> pf = new DefaultKafkaProducerFactory<>(producerProps);
    kafkaTemplate = new KafkaTemplate<>(pf);

    // separate test consumer in its own group to assert key and headers
    Map<String, Object> consumerProps = new java.util.HashMap<>(
        KafkaTestUtils.consumerProps("project-consumer-it-assert", "false", embeddedKafka)
    );
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Project.class.getName());
    consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    ConsumerFactory<String, Project> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
    testConsumer = cf.createConsumer();
    embeddedKafka.consumeFromAnEmbeddedTopic(testConsumer, TOPIC);
    }

  @Test
  void projectMessageRetriesThenSucceeds() {
    // First call fails, second call succeeds
    org.mockito.Mockito.doThrow(new RuntimeException("boom"))
        .doNothing()
        .when(projectService).handle(any(Project.class));

    Project payload = Project.builder().id("retry-ok-1").name("Proj Retry OK").status("ACTIVE").build();
    kafkaTemplate.send(TOPIC, payload);
    kafkaTemplate.flush();

    // Await until service was invoked twice (main + retry)
    await().atMost(ofSeconds(15)).untilAsserted(() ->
            org.mockito.Mockito.verify(projectConsumerListener, org.mockito.Mockito.times(2)).listen(any(Project.class), any(Acknowledgment.class))
    );
  }

  @Test
  void projectMessageRetriesThenDlt() {
    // Force processing failure to exercise retry and DLT
    doThrow(new RuntimeException("boom")).when(projectService).handle(any(Project.class));

    Project payload = Project.builder().id("fail-1").name("Proj Fail").status("BROKEN").build();
    kafkaTemplate.send(TOPIC, payload);
    kafkaTemplate.flush();

    // Assert via interception that DLT handler was invoked
    await().atMost(ofSeconds(20)).untilAsserted(() ->
            verify(projectConsumerListener, atLeastOnce()).dlt(any(Project.class), any(String.class))
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
  }

  @Test
  void projectConsumerReceivesMessage() {
    // key sequencing and headers
    String key = "proj-key-001";
    Project payload = Project.builder().id("p-1").name("Proj A").status("ACTIVE").build();

    Message<Project> message = MessageBuilder
        .withPayload(payload)
        .setHeader(KafkaHeaders.TOPIC, TOPIC)
        .setHeader(KafkaHeaders.KEY, key)
        .setHeader("x-test", "project-it")
        .setHeader("x-seq", 1)
        .build();

    kafkaTemplate.send(message);
    kafkaTemplate.flush();

    // verify business handling invoked with matching fields
    await().atMost(ofSeconds(10)).untilAsserted(() -> {
      var captor = forClass(Project.class);
      verify(projectService, atLeastOnce()).handle(captor.capture());
      Project captured = captor.getValue();
      org.assertj.core.api.Assertions.assertThat(captured.getId()).isEqualTo(payload.getId());
      org.assertj.core.api.Assertions.assertThat(captured.getName()).isEqualTo(payload.getName());
      org.assertj.core.api.Assertions.assertThat(captured.getStatus()).isEqualTo(payload.getStatus());
    });

    // additionally verify listener method was invoked
      verify(projectConsumerListener, atLeastOnce()).listen(any(Project.class), any(Acknowledgment.class));
  }



}
