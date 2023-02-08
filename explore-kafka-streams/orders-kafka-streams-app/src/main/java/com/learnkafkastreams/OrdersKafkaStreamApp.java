package com.learnkafkastreams;


import com.learnkafkastreams.topology.OrdersTopology;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.learnkafkastreams.topology.OrdersTopology.*;

@Slf4j
public class OrdersKafkaStreamApp {


    public static void main(String[] args) {

        // create an instance of the topology
        var topology = OrdersTopology.buildTopology();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-app-2"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // read only the new messages
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000"); // commit interval
        //config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, OrderTimeStampExtractor.class); // timestamp extractor
        createTopics(config, List.of(STORES, ORDERS, GENERAL_ORDERS, RESTAURANT_ORDERS));

        //Create an instance of KafkaStreams
        var kafkaStreams = new KafkaStreams(topology, config);

        //This closes the streams anytime the JVM shuts down normally or abruptly.
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        try{
            kafkaStreams.start();
        }catch (Exception e ){
            log.error("Exception in starting the Streams : {}", e.getMessage(), e);
        }

    }

    private static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 2;
        short replication  = 1;

        var newTopics = greetings
                .stream()
                .map(topic ->{
                    return new NewTopic(topic, partitions, replication);
                })
                .collect(Collectors.toList());

        var createTopicResult = admin.createTopics(newTopics);
        try {
            createTopicResult
                    .all().get();
            log.info("topics are created successfully");
        } catch (Exception e) {
            log.error("Exception creating topics : {} ",e.getMessage(), e);
        }
    }

}
