package com.learnkafkastreams.topology;

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
public class ExploreAggregateOperatorsTopology {


    public static String AGGREGATE = "aggregate";

    public static Topology build(){
        StreamsBuilder streamsBuilder = new StreamsBuilder();


        var inputStream = streamsBuilder
                .stream(AGGREGATE, Consumed.with(Serdes.String(), Serdes.String()));

        inputStream
                .print(Printed.<String,String>toSysOut().withLabel(AGGREGATE));

        var groupedString = inputStream
                //.selectKey((key, value) -> String.valueOf(value.charAt(0)))// Apple -> A, Ambulance -> A
                //.map((key, value) -> KeyValue.pair(String.valueOf(value.charAt(0)), value))
                //.groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
                .groupBy((key, value) -> value,
                        Grouped.with(Serdes.String(), Serdes.String()));

        exploreCount(groupedString);
        exploreReduce(groupedString);
        //exploreAggregate(groupedString);
        return streamsBuilder.build();
    }

    private static void exploreAggregate(KGroupedStream<String, String> groupedString) {

        //var alphabetWordAggregateInitializer = AlphabetWordAggregate::new;
        Initializer<AlphabetWordAggregate> alphabetWordAggregateInitializer = AlphabetWordAggregate::new;

        Aggregator<String, String, AlphabetWordAggregate> aggregator   = (key,value, alphabetWordAggregate )-> {
            return alphabetWordAggregate.updateNewEvents(key, value);
        };

        var aggregatedStream = groupedString
                .aggregate(
                        alphabetWordAggregateInitializer,
                        aggregator,
                        Materialized
                                .<String, AlphabetWordAggregate, KeyValueStore< Bytes, byte[]>>as("aggregated-words")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.alphabetWordAggregate())
                );

        aggregatedStream
                .toStream()
                .print(Printed.<String,AlphabetWordAggregate>toSysOut().withLabel("aggregated-Words"));
    }

    private static void exploreReduce(KGroupedStream<String, String> groupedString) {

        var reducedStream = groupedString
                .reduce((value1, value2) -> {
                    log.info("value1 : {} , value2 : {} ", value1, value2);
                    return value1.toUpperCase()+"-"+value2.toUpperCase();
                }
                , Materialized
                                .<String, String, KeyValueStore< Bytes, byte[]>>as("reduced-words")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String())
                );

        reducedStream
                .toStream()
                .print(Printed.<String,String>toSysOut().withLabel("Reduced-Words"));
    }

    private static void exploreCount(KGroupedStream<String, String> groupedString) {
        var countByAlphabet = groupedString
                //.count(Named.as("count-per-alphabet"));
                .count(Named.as("count-per-alphabet"),
                        Materialized.as("count-per-alphabet"));// defining a state-store and saving the state data in rocksDB and changelog topic


        countByAlphabet
                .toStream()
                .peek((key, value) -> log.info("Key : {} , Value : {}", key, value))
                .print(Printed.<String,Long>toSysOut().withLabel("Words-Count-Per-Alphabet"));
    }

}
