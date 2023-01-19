package com.learnkafkastreams.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GreetingsStreamsProcessor {

    public static String GREETINGS ="greetings";

    @Autowired
    public void process(StreamsBuilder streamsBuilder){

        var greetingsStream = streamsBuilder
                .stream(GREETINGS,
                        Consumed.with(Serdes.String(), Serdes.String()));


        var modifiedStream = greetingsStream
                .mapValues((readOnlyKey, value) -> value.toUpperCase());


        modifiedStream
                .print(Printed.<String, String>toSysOut().withLabel("greeting-streams"));

    }
}
