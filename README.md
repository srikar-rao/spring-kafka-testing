# Spring Kafka Integration Testing

A reference implementation demonstrating best practices for Kafka integration testing in Spring Boot applications using embedded Kafka brokers and schema validation.

## Project Overview

This project serves as a comprehensive reference for implementing and testing Kafka-based messaging in Spring Boot applications. It demonstrates how to properly set up producers, consumers, and integration tests for Kafka messaging with a focus on reliability, schema validation, and proper error handling.

### Key Features

- **Embedded Kafka Testing**: Integration tests using Spring's embedded Kafka broker
- **Schema Validation**: JSON Schema validation with Confluent's KafkaJsonSchema serializer/deserializer
- **Error Handling**: Retry mechanisms and Dead Letter Topic (DLT) handling
- **Multiple Environment Configurations**: Development, testing, local, and production configurations
- **Transactional Messaging**: Examples of transactional Kafka producers
- **Mock Schema Registry**: Testing schema validation without external dependencies

## Architecture

The project is structured around several key components:

### Consumers
- `TaskConsumerListener`: Processes task messages with JSON schema validation
- `ProjectConsumerListener`: Processes project messages with retry and DLT handling

### Producers
- `IssueKafkaProducer`: Publishes issue events with schema validation
- `TodoKafkaProducer`: Publishes todo events
- `ProjectKafkaProducer`: Publishes project events with transactional support

### Models
- `Task`: Represents a task with id, name, dueDate, status, and createdAt fields
- `Project`: Represents a project entity
- `Issue`: Represents an issue with schema validation
- `Todo`: Represents a todo item

## Integration Testing Approach

The project demonstrates several approaches to Kafka integration testing:

1. **Embedded Kafka**: Using `@EmbeddedKafka` annotation to spin up an in-memory Kafka broker for tests
2. **Mock Schema Registry**: Using Confluent's `MockSchemaRegistry` to test schema validation without external dependencies
3. **Hybrid Testing**: Using both standard JSON serializers and schema-aware serializers in tests
4. **Test Profiles**: Separate configuration for test environments

### Example Test Cases

- Basic message processing
- Retry and recovery scenarios
- Dead Letter Topic handling
- Schema validation failures:
  - Missing required fields
  - Invalid enum values
  - Additional properties not defined in the schema

## Getting Started

### Prerequisites
- Java 17+
- Gradle
- Docker (optional, for running Kafka locally)

### Running Locally
1. Clone the repository
2. Start local Kafka and Schema Registry (or use Docker Compose)
3. Run the application with the local profile:
   ```
   ./gradlew bootRun --args='--spring.profiles.active=local'
   ```

### Running Tests
```
./gradlew test
```

## Environment Configuration

The project includes configuration for multiple environments:

- **application-dev.yml**: Development environment with mock Kafka broker
- **application-test.yml**: Testing environment with embedded Kafka
- **application-local.yml**: Local development with Kafka at localhost:9092 and Schema Registry at localhost:8081
- **application-prod.yml**: Production environment configuration

## Key Learnings

This project demonstrates several important concepts for Kafka integration in Spring Boot:

1. **Schema Evolution**: How to handle schema changes safely
2. **Error Handling**: Proper retry and DLT patterns
3. **Testing Strategies**: How to test Kafka consumers and producers without external dependencies
4. **Configuration Management**: Environment-specific Kafka configurations

## For Students

This project serves as a learning resource for:
- Spring Boot integration with Kafka
- Writing reliable integration tests
- Implementing schema validation
- Handling errors in asynchronous messaging

## For Developers

Use this project as a reference implementation for:
- Setting up Kafka consumers and producers in Spring Boot
- Implementing proper error handling for Kafka consumers
- Testing Kafka components with embedded brokers
- Implementing schema validation for message contracts

## For Recruiters

This project demonstrates expertise in:
- Event-driven architecture
- Asynchronous messaging patterns
- Testing methodologies for distributed systems
- Schema management and validation
- Spring Boot application development

## License

This project is open source and available under the MIT License.
