package com.dev.kafka.producer;

import com.dev.kafka.events.Project;
import com.dev.kafka.producer.publisher.ProjectTxPublisherService;
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
@EmbeddedKafka(partitions = 1, topics = {"project-topic"})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestPropertySource(properties = {
    "kafka.project-producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "kafka.project-producer.topic=project-topic",
    "kafka.project-producer.transaction-id-prefix=it-project-tx-"
})
class ProjectTxPublisherServiceIT {

  @Autowired
  private ProjectTxPublisherService publisherService;

  @Autowired
  private EmbeddedKafkaBroker embeddedKafka;

  private org.apache.kafka.clients.consumer.Consumer<String, Project> consumer;

  @BeforeAll
  void setUp() {
    Map<String, Object> consumerProps = new HashMap<>(
        KafkaTestUtils.consumerProps("project-tx-consumer-it", "false", embeddedKafka)
    );
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
    consumerProps.put(JsonDeserializer.VALUE_DEFAULT_TYPE, Project.class.getName());
    consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
    // Ensure we read only committed data
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

    ConsumerFactory<String, Project> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
    consumer = cf.createConsumer();
    embeddedKafka.consumeFromAnEmbeddedTopic(consumer, "project-topic");
  }

  @AfterAll
  void tearDown() {
    if (consumer != null) {
      consumer.close(ofSeconds(1));
    }
  }

  @Test
  void publish_shouldSendProjectTransactionally() {
    Project project = Project.builder()
        .id("tx-1")
        .name("Project TX")
        .status("ACTIVE")
        .build();

    publisherService.publish(project);

    var record = getSingleRecord(consumer, "project-topic", ofSeconds(10));
    assertThat(record).isNotNull();
    Project value = record.value();
    assertThat(value.getId()).isEqualTo(project.getId());
    assertThat(value.getName()).isEqualTo(project.getName());
    assertThat(value.getStatus()).isEqualTo(project.getStatus());
  }
}
