package com.dev.kafka;

import com.dev.kafka.consumer.props.KafkaConsumerProperties;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;

@SpringBootApplication
@EnableConfigurationProperties(KafkaConsumerProperties.class)
public class KafkaIntegrationTestingApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaIntegrationTestingApplication.class, args);
	}

}
