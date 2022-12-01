package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

@Slf4j
public class GreetingsTopology {

    public static String GREETINGS = "greetings";
    public static String GREETINGS_UPPERCASE = "greetings-uppercase";

    public static Topology buildTopology(){

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var greetingsStream = streamsBuilder.stream(GREETINGS,
                Consumed.with(Serdes.String(), Serdes.String()));

        greetingsStream
                .print(Printed.<String, String>toSysOut().withLabel("greeting"));

        var upperCaseStream = greetingsStream
                .filter((key, value) -> value.length()>5)
                .mapValues((readOnlyKey, value) -> value.toUpperCase())
                .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));

        upperCaseStream
                .print(Printed.<String, String>toSysOut().withLabel("greeting-uppercase"));

        upperCaseStream.to(GREETINGS_UPPERCASE,
                        Produced.with(Serdes.String(), Serdes.String()));


        return streamsBuilder.build();

    }
}
