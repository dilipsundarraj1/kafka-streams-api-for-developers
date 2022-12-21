package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import com.learnkafkastreams.domain.AlphabetWordAggregate;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        join(streamsBuilder);
      //  joinKTables(streamsBuilder);


        return streamsBuilder.build();
    }

    private static void joinKTables(StreamsBuilder streamsBuilder) {

        var alphabetsAbbreviation = streamsBuilder
                .table(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("alphabets-abbreviations-store"));

        alphabetsAbbreviation
                 .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("alphabets-abbreviations"));

        var alphabetsTable = streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("alphabets-store"));

        alphabetsTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;


        var joinedTable = alphabetsAbbreviation
                .join(alphabetsTable,
                        valueJoiner);

        joinedTable
                .toStream()
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-with-abbreviations"));
    }


    private static void join(StreamsBuilder streamsBuilder) {

        var alphabetsAbbreviation = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbreviation
                .print(Printed.<String, String>toSysOut().withLabel("alphabets-abbreviations"));

        var alphabetsTable = streamsBuilder
                .table(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("alphabets-store"));
        alphabetsTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var joinedStream = alphabetsAbbreviation
                .join(alphabetsTable,
                        valueJoiner);

        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-with-abbreviations"));
    }

}
