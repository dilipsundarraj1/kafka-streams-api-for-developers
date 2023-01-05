package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Alphabet;
import com.learnkafkastreams.domain.AlphabetWordAggregate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {


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
