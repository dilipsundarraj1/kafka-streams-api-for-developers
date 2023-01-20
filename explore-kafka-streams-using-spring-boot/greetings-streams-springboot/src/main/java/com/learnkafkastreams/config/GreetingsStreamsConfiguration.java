package com.learnkafkastreams.config;

import com.learnkafkastreams.exceptionhandler.StreamsProcessorCustomErrorHandler;
import com.learnkafkastreams.streams.GreetingsStreamsProcessor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.autoconfigure.kafka.StreamsBuilderFactoryBeanCustomizer;
import org.springframework.boot.context.properties.source.InvalidConfigurationPropertyValueException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import java.util.Map;

import static org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_BUILDER_BEAN_NAME;


@Configuration
@Slf4j
public class GreetingsStreamsConfiguration {
    //KafkaStreamsDefaultConfiguration -> Class Responsible for configuring the KafkaStreams in SpringBoot
    @Autowired
    KafkaProperties kafkaProperties;

    @Autowired
    KafkaTemplate<String, String> kafkaTemplate;

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamConfig() {
//        return new KafkaStreamsConfiguration(Map.of(
//                StreamsConfig.APPLICATION_ID_CONFIG, "greeting-streams-springboot",
//                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
//                StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class,
//                StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class,
//                StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class
//        ));

        var streamProperties = kafkaProperties.buildStreamsProperties();

        streamProperties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, RecoveringDeserializationExceptionHandler.class);
        streamProperties.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, consumerRecordRecoverer);

        return new KafkaStreamsConfiguration(streamProperties);
    }

    @Bean
    public StreamsBuilderFactoryBeanConfigurer streamsBuilderFactoryBeanConfigurer(){
        log.info("Inside streamsBuilderFactoryBeanConfigurer");
        return factoryBean -> {
            factoryBean.setStreamsUncaughtExceptionHandler(new StreamsProcessorCustomErrorHandler());
        };
    }


    public DeadLetterPublishingRecoverer recoverer() {
        return new DeadLetterPublishingRecoverer(kafkaTemplate,
                (record, ex) -> {
                    log.error("Exception in Deserializing the message : {} and the record is : {}", ex.getMessage(),record,  ex);
                    return new TopicPartition("recovererDLQ", record.partition());
                });
    }


    ConsumerRecordRecoverer consumerRecordRecoverer = (record, exception) -> {
        log.error("Exception is : {} Failed Record : {} ", exception, record);
    };

    @Bean
    public NewTopic topicBuilder() {
        return TopicBuilder.name(GreetingsStreamsProcessor.GREETINGS)
                .partitions(2)
                .replicas(1)
                .build();

    }
}
