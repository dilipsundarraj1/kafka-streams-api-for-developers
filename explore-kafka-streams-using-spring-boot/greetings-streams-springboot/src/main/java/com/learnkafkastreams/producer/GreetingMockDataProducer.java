package com.learnkafkastreams.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;
import java.util.List;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class GreetingMockDataProducer {
    public static String GREETINGS= "greetings";

    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

      //greetingsString();
        englishGreetings(objectMapper);

    }

    private static void greetingsString() {
        var greetings = List.of(
                "Hello, Good Morning!",
                "Hello, Good Evening!",
                "Hello, Good Night!"
        );

        greetings
                .forEach(greeting -> {
                    publishMessageSync(GREETINGS, null, greeting);
                });



    }

    private static void englishGreetings(ObjectMapper objectMapper) {
        var englishGreetings = List.of(
              //  new Greeting("Error", LocalDateTime.now()),
                new Greeting("Hello, Good Morning!", LocalDateTime.now()),
                new Greeting("Hello, Good Evening!", LocalDateTime.now()),
                new Greeting("Hello, Good Night!", LocalDateTime.now())
        );
        englishGreetings
                .forEach(greeting -> {
                    try {
                        var greetingJSON = objectMapper.writeValueAsString(greeting);
                        var recordMetaData = publishMessageSync(GREETINGS, null, greetingJSON);
                        log.info("Published the alphabet message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        throw new RuntimeException(e);
                    }
                });
    }


}

