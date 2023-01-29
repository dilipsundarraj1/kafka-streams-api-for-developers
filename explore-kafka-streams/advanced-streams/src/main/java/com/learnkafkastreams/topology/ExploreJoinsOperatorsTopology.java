package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Alphabet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

@Slf4j
public class ExploreJoinsOperatorsTopology {


    public static String ALPHABETS = "alphabets"; // A => First letter in the english alphabet
    public static String ALPHABETS_ABBREVATIONS = "alphabets_abbreviations"; // A=> Apple


    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

//        joinKStreamWithKTable(streamsBuilder);
//        joinKStreamWithGlobalKTable(streamsBuilder);
//        joinKTables(streamsBuilder);
        joinKStreams(streamsBuilder);


        return streamsBuilder.build();
    }


    private static void joinKStreams(StreamsBuilder streamsBuilder) {

        var alphabetsAbbreviation = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbreviation
                .print(Printed.<String, String>toSysOut().withLabel("alphabets-abbreviations"));

        var alphabetsStream = streamsBuilder
                .stream(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String())
                );

        alphabetsStream
                .print(Printed.<String, String>toSysOut().withLabel("alphabets"));

        var joinedParams =
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
                        .withName("alphabets-join")
                        .withStoreName("alphabets-join")
                ;

        JoinWindows fiveSecondWindow = JoinWindows
                .ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5)) // 5th second is not inclusive
                //.ofTimeDifferenceAndGrace(Duration.ofSeconds(5), Duration.ofSeconds(2))
                ;

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        var joinedStream = alphabetsAbbreviation
                .join(alphabetsStream,
                        valueJoiner,
                        fiveSecondWindow,
                        joinedParams
                );

    /* leftJoin:
             If there is an event for the alphabetsAbbreviation stream , the join will be triggered even if there is no event in the alphabetsStream.
             The joinedValue will have null for the description.*/
//        var joinedStream = alphabetsAbbreviation
//                .leftJoin(alphabetsStream,
//                        valueJoiner,
//                        fiveSecondWindow
//                        , joinedParams
//                );

        /* outerJoin:*/
//                var joinedStream = alphabetsAbbreviation
//                .outerJoin(alphabetsStream,
//                        valueJoiner,
//                        fiveSecondWindow
//                        , joinedParams
//                );

        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-with-abbreviations-kstream"));
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


//        var joinedTable = alphabetsAbbreviation
//                .join(alphabetsTable,
//                        valueJoiner);

        var joinedTable = alphabetsAbbreviation
                .leftJoin(alphabetsTable,
                        valueJoiner);

        joinedTable
                .toStream()
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-with-abbreviations"));
    }


    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {

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

        //[alphabets-with-abbreviations]: A, Alphabet[abbreviation=Apple, description=A is the first letter in English Alphabets.]
        //[alphabets-with-abbreviations]: B, Alphabet[abbreviation=Bus, description=B is the second letter in English Alphabets.]

        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-with-abbreviations"));
    }

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {

        var alphabetsAbbreviation = streamsBuilder
                .stream(ALPHABETS_ABBREVATIONS, Consumed.with(Serdes.String(), Serdes.String()));

        alphabetsAbbreviation
                .print(Printed.<String, String>toSysOut().withLabel("alphabets-abbreviations"));

        var alphabetsTable = streamsBuilder
                .globalTable(ALPHABETS,
                        Consumed.with(Serdes.String(), Serdes.String())
                        , Materialized.as("alphabets-store"));

        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KeyValueMapper<String, String, String> keyMapper =
                (leftKey, rightKey) -> leftKey;

        var joinedStream = alphabetsAbbreviation
                .join(alphabetsTable
                        , keyMapper, valueJoiner
                );

        joinedStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabets-with-abbreviations"));
    }

}
