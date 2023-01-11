package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.Alphabet;
import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<Greeting> greetingSerdeUsingGenerics(){

        JsonSerializer<Greeting> jsonSerializer = new JsonSerializer<>();

        JsonDeserializer<Greeting> jsonDeSerializer = new JsonDeserializer<>(Greeting.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }

    public static Serde<Greeting> greetingSerdes(){

        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        return  new GreetingSerdes();
    }

    public static Serde<AlphabetWordAggregate> alphabetWordAggregate() {

        JsonSerializer<AlphabetWordAggregate> jsonSerializer = new JsonSerializer<>();

        JsonDeserializer<AlphabetWordAggregate> jsonDeSerializer = new JsonDeserializer<>(AlphabetWordAggregate.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }


    public static Serde<Alphabet> alphabet() {

        JsonSerializer<Alphabet> jsonSerializer = new JsonSerializer<>();

        JsonDeserializer<Alphabet> jsonDeSerializer = new JsonDeserializer<>(Alphabet.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }
}
