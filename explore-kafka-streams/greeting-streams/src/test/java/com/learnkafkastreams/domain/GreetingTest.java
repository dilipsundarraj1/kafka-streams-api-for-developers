package com.learnkafkastreams.domain;


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

public class GreetingTest {

    @Test
    void greetingsJson() throws JsonProcessingException {
        JavaTimeModule module = new JavaTimeModule();
        var objectMapper = new ObjectMapper()
                .registerModule(module)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                ;

        var greeting = new Greeting("Good Morning", LocalDateTime.now());

        var greetingJSON = objectMapper.writeValueAsString(greeting);

        System.out.println(objectMapper.writeValueAsString(greeting));

        var greetingObj = objectMapper.readValue(greetingJSON,Greeting.class);


        System.out.println("greetingObj : " + greetingObj);
    }
}
