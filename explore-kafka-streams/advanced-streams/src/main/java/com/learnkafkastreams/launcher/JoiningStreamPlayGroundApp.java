package com.learnkafkastreams.launcher;

import com.learnkafkastreams.topology.ExploreJoinsOperatorsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.learnkafkastreams.topology.ExploreJoinsOperatorsTopology.ALPHABETS;
import static com.learnkafkastreams.topology.ExploreJoinsOperatorsTopology.ALPHABETS_ABBREVATIONS;

@Slf4j
public class JoiningStreamPlayGroundApp {


    public static void main(String[] args) {

      var kTableTopology = ExploreJoinsOperatorsTopology.build();

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "joins1"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000");

        createTopics(config, List.of(ALPHABETS,ALPHABETS_ABBREVATIONS ));

        //createTopicsCopartitioningDemo(config, List.of(ALPHABETS,ALPHABETS_ABBREVATIONS ));
         var kafkaStreams = new KafkaStreams(kTableTopology, config);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        log.info("Starting Greeting streams");
        kafkaStreams.start();
    }

    private static void createTopicsCopartitioningDemo(Properties config, List<String> alphabets) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
        short replication  = 1;

        var newTopics = alphabets
                .stream()
                .map(topic ->{
                    if(topic.equals(ALPHABETS_ABBREVATIONS)){
                        return new NewTopic(topic, 3, replication);
                    }
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

    private static void createTopics(Properties config, List<String> greetings) {

        AdminClient admin = AdminClient.create(config);
        var partitions = 1;
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
