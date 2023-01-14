package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

@Slf4j
public class GreetingsTopology {

    public static String GREETINGS = "greetings";

    public static String GREETINGS_SPANISH = "greetings-spanish";
    public static String GREETINGS_UPPERCASE = "greetings_uppercase";

    public static Topology buildTopology() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

//        KStream<String, String> mergedStream = getStringGreetingKStream(streamsBuilder);

        KStream<String, Greeting> mergedStream = getCustomGreetingKStream(streamsBuilder);

        var modifiedStream = exploreErrors(mergedStream);

        //KStream<String, Greeting> modifiedStream = exploreOperators(mergedStream);

        modifiedStream
                .print(Printed.<String, Greeting>toSysOut().withLabel("greeting-uppercase"));
        modifiedStream
                .to(GREETINGS_UPPERCASE,
                        //Produced.with(Serdes.String(), SerdesFactory.greetingSerdeUsingGenerics()));
                        Produced.with(Serdes.String(), SerdesFactory.greetingSerdes()));


        return streamsBuilder.build();

    }

    private static KStream<String, Greeting> exploreOperators(KStream<String, Greeting> mergedStream) {
        var modifiedStream = mergedStream
                //.filter((key, value) -> value.length() > 5)//gm-gm,gm-gm
                //.filter((key, value) -> value.getMessage().length() > 5)
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

                //.mapValues((readOnlyKey, value) -> value.toUpperCase()) //good morning, good evening

                .mapValues((readOnlyKey, value) -> {
                    return new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp());
                }) //good morning, good evening
                //.map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()))//gm-good morning, ge-good evening
                ;
        return modifiedStream;
    }

    private static KStream<String, Greeting> exploreErrors(KStream<String, Greeting> mergedStream) {

        return mergedStream
                .mapValues((readOnlyKey, value) -> {
                    if (value.getMessage().equals("Transient Error")) {
                        try {
                            throw new IllegalStateException(value.getMessage());
                        } catch (Exception e) {
                            log.error("Exception Caught : {}", e.getMessage(), e);
                           // throw e;
                            return null;
                        }

                    }
                    return new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp());
                })
                .filter((key, value) -> key != null && value != null);
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {

        var greetingsStream = streamsBuilder.stream(GREETINGS,
                //Consumed.with(Serdes.String(), SerdesFactory.greetingSerdeUsingGenerics()));//gm-googmorning
                Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes()));//gm-googmorning
        greetingsStream
                .print(Printed.<String, Greeting>toSysOut().withLabel("greeting"));

        var greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH,
                //Consumed.with(Serdes.String(), SerdesFactory.greetingSerdeUsingGenerics())); // gm-goodmorningspanish
                Consumed.with(Serdes.String(), SerdesFactory.greetingSerdes())); // gm-goodmorningspanish

        greetingsSpanishStream
                .print(Printed.<String, Greeting>toSysOut().withLabel("greeting"));

        return greetingsStream.merge(greetingsSpanishStream);
    }

    private static KStream<String, String> getStringGreetingKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> greetingsStream = streamsBuilder.stream(GREETINGS,
                Consumed.with(Serdes.String(), Serdes.String()));
        //gm-googmorning
        greetingsStream
                .print(Printed.<String, String>toSysOut().withLabel("greeting"));

        var greetingsSpanishStream = streamsBuilder.stream(GREETINGS_SPANISH,
                Consumed.with(Serdes.String(), Serdes.String())); // gm-goodmorningspanish

        greetingsSpanishStream
                .print(Printed.<String, String>toSysOut().withLabel("greeting-spanish"));

        return greetingsStream.merge(greetingsSpanishStream);
    }
}
