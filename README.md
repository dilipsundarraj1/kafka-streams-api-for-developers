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

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic greetings
```

```
./kafka-console-producer.sh --broker-list localhost:9092 --topic greetings --property "key.separator=-" --property "parse.key=true"

```

```
 ./kafka-console-producer.sh --broker-list localhost:9092 --topic greetings-spanish --property "key.separator=-" --property "parse.key=true"
```

#### Consume Messages

- This  command should take care of logging in to the Kafka container.

```
docker exec -it broker bash
```
- Command to produce messages in to the Kafka topic.

```
kafka-console-consumer --bootstrap-server localhost:9092 --topic greetings-uppercase
```

```
./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic greetings-uppercase --from-beginning

```

### List Topics

```
./kafka-topics.sh --bootstrap-server localhost:9092 --list
```
