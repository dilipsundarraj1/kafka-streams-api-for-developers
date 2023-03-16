package com.learnkafkastreams.launcher;

import com.learnkafkastreams.exceptionhandler.StreamsDeserializationErrorHandler;
import com.learnkafkastreams.exceptionhandler.StreamsProcessorCustomErrorHandler;
import com.learnkafkastreams.exceptionhandler.StreamsSerializationExceptionHandler;
import com.learnkafkastreams.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static com.learnkafkastreams.topology.GreetingsTopology.*;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;

@Slf4j
public class GreetingsStreamApp {

    public static void main(String[] args) {

        var greetingsTopology = GreetingsTopology.buildTopology();


        var streamThreads = Runtime.getRuntime().availableProcessors();
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app"); // consumer group
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest"); // read only the new messages
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
       // config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads+""); // read only the new messages

        //error-handling config
        //desrialization errors
        config.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
        //        LogAndContinueExceptionHandler.class
                StreamsDeserializationErrorHandler.class
        );

        //serialization errors
        config.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG,
                StreamsSerializationExceptionHandler.class
        );

       // config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, streamThreads+"");
        createTopics(config, List.of(GREETINGS, GREETINGS_UPPERCASE, GREETINGS_SPANISH));

        var kafkaStreams = new KafkaStreams(greetingsTopology, config);

        //set the processor exceptiopn handler
        kafkaStreams.setUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandler());

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
