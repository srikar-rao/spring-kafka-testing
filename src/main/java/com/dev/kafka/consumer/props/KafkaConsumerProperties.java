package com.dev.kafka.consumer.props;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;


@Configuration
@ConfigurationProperties(prefix = "kafka")
@Getter
public class KafkaConsumerProperties {

    private final TopicConfig task = new TopicConfig();
    private final TopicConfig project = new TopicConfig();

    @Getter
    @Setter
    public static class TopicConfig {
        private String bootstrapServers;
        private String topic;
        private final ConsumerConfig consumer = new ConsumerConfig();

    }

    @Getter
    @Setter
    public static class ConsumerConfig {
        private String groupId;
        private String keyDeserializer;
        private String valueDeserializer;
        private String autoOffsetReset;
    }
}
