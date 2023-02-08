# Kafka Streams API for Developers


## Set up Kafka Environment using Docker

- This should set up the Zookeeper and Kafka Broker in your local environment

```aidl
docker-compose up
```

### Verify the Local Kafka Environment

- Run this below command

```
docker ps
```

- You should be below containers up and running in local

```
CONTAINER ID   IMAGE                                   COMMAND                  CREATED          STATUS          PORTS                                            NAMES
fb28f7f91b0e   confluentinc/cp-server:7.1.0            "/etc/confluent/dock…"   50 seconds ago   Up 49 seconds   0.0.0.0:9092->9092/tcp, 0.0.0.0:9101->9101/tcp   broker
d00a0f845a45   confluentinc/cp-zookeeper:7.1.0         "/etc/confluent/dock…"   50 seconds ago   Up 49 seconds   2888/tcp, 0.0.0.0:2181->2181/tcp, 3888/tcp       zookeeper
```

### Interacting with Kafka

#### Produce Messages

- This  command should take care of logging in to the Kafka container.

```
docker exec -it broker bash
```

- Command to produce messages in to the Kafka topic.

```
kafka-console-producer --broker-list localhost:9092 --topic greetings
```

- Publish to **greetings** topic with key and value

```
kafka-console-producer --broker-list localhost:9092 --topic greetings --property "key.separator=-" --property "parse.key=true"

```

- Publish to **greetings-spanish** topic with key and value

```
 kafka-console-producer --broker-list localhost:9092 --topic greetings_spanish --property "key.separator=-" --property "parse.key=true"
```

#### Consume Messages

- This  command should take care of logging in to the Kafka container.

```
docker exec -it broker bash
```
- Command to consume messages from the Kafka topic.

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings_uppercase
```

- Command to consume with Key

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings_uppercase --from-beginning -property "key.separator= - " --property "print.key=true"
```

- Other Helpful Kafka Consumer commands

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic general_orders
```

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic restaurant_orders
```

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic ktable-words-store-changelog --from-beginning
```

- Command to read from the Internal Aggregate topic

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic aggregate-KSTREAM-AGGREGATE-STATE-STORE-0000000003-changelog --from-beginning -property "key.separator= - " --property "print.key=true"
```


### List Topics

- This  command should take care of logging in to the Kafka container.

```
docker exec -it broker bash
```

- Command to list the topics.

```
kafka-topics --bootstrap-server localhost:9092 --list
```


## KafkaStreams using SpringBoot 

### How AutoConfiguration works ?

- Adding the annotation @EnableKafkaStreams is going to invoke the **KafkaStreamsDefaultConfiguration** class
  - **KafkaStreamsAnnotationDrivenConfiguration** supplies the **KafkaStreamsConfiguration** bean
  - This class takes care of building the **StreamsBuilderFactoryBean** which is responsible for supplying the StreamsBuilder instance.
      - This **StreamsBuilderFactoryBean** class also takes care of managing the Lifecycle of the **KafkaStreams** App.
