package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.stream.Collectors;

@Slf4j
public class GreetingsTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_SPANISH = "greetings-spanish";
    public static String GREETINGS_UPPERCASE = "greetings-uppercase";

    public static Topology buildTopology() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        var greetingsStream = streamsBuilder.stream(GREETINGS,
                Consumed.with(Serdes.String(), Serdes.String()));//gm-googmorning

        greetingsStream
                .print(Printed.<String, String>toSysOut().withLabel("greeting"));

        var greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH,
                Consumed.with(Serdes.String(), Serdes.String())); // gm-goodmorningspanish

        greetingsSpanishStream
                .print(Printed.<String, String>toSysOut().withLabel("greeting"));


        var mergedStream = greetingsStream.merge(greetingsSpanishStream);


        //var upperCaseStream = greetingsStream
        var upperCaseStream =mergedStream
                .filter((key, value) -> value.length() > 5)//gm-gm,gm-gm
//                .peek((key, value) -> {
//                    log.info("after filter : key : {} , value : {} ", key, value);
//                })
//                .flatMapValues((readOnlyKey, value) -> {
//                    var newValue = Arrays.asList(value.split(""));
//                    return newValue;
//                }) // gm-good morning
//                .peek((key, value) -> {
//                    log.info("after filter : key : {} , value : {} ", key, value);
//                })
 /*               .flatMap((key, value) -> {
                    var newValue = Arrays.asList(value.split(""));
                    var keyValueList = newValue
                            .stream().map(t -> KeyValue.pair(key.toUpperCase(), t))
                            .collect(Collectors.toList());
                    return keyValueList;
                }) // // gm-good morning*/

                .mapValues((readOnlyKey, value) -> value.toUpperCase()) //good morning, good evening
                //.map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()))//gm-good morning, ge-good evening
                ;

        upperCaseStream
                .print(Printed.<String, String>toSysOut().withLabel("greeting-uppercase"));

        upperCaseStream.to(GREETINGS_UPPERCASE,
                Produced.with(Serdes.String(), Serdes.String()));


        return streamsBuilder.build();

    }
}
