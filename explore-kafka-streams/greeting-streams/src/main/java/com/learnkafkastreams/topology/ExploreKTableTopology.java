package com.learnkafkastreams.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Printed;

@Slf4j
public class ExploreKTableTopology {


    public static String WORDS = "words";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();


        var wordsTable = streamsBuilder
                .table("words", Consumed.with(Serdes.String(), Serdes.String())
                  , Materialized.as("words-store")
                );



        var wordsGlobalTable = streamsBuilder
                .globalTable("words", Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("words-global-store")
                );


        wordsTable
                .filter((key, value) -> value.length() > 2)
                .toStream()
                .peek(((key, value) -> log.info("Key : {} , value : {} ", key,value)))
                .print(Printed.<String,String>toSysOut().withLabel("words-ktable"));


        wordsTable
                .filter((key, value) -> value.length()>3)
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("words-ktable-filtered"));



        return streamsBuilder.build();
    }

}