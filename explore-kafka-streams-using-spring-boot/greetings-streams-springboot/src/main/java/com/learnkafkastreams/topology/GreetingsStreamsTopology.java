package com.learnkafkastreams.topology;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.learnkafkastreams.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class GreetingsStreamsTopology {

    private ObjectMapper objectMapper;

    public GreetingsStreamsTopology(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public static String GREETINGS = "greetings";
    public static String GREETINGS_OUTPUT = "greetings-output";

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

        var greetingsStream = streamsBuilder
                .stream(GREETINGS,
                        Consumed.with(Serdes.String(),
                                //Serdes.String())
                                new JsonSerde<>(Greeting.class, objectMapper)
                        ));


        var modifiedStream = greetingsStream
                //.mapValues((readOnlyKey, value) -> value.toUpperCase())
                .mapValues((readOnlyKey, value) -> {
                    if(value.getMessage().equals("Error")){
                        try{
                           // throw new IllegalStateException("Error Occurred");
                        }catch (Exception e){
                            log.error("Exception is : {} ", e.getMessage(), e);
                           // throw e;
                        }

                    }
                    return new Greeting(value.getMessage().toUpperCase(), value.getTimeStamp());
                });


        modifiedStream
                //.print(Printed.<String, String>toSysOut().withLabel("greeting-streams"));
                .print(Printed.<String, Greeting>toSysOut().withLabel("greeting-streams"));

        modifiedStream
                .to(GREETINGS_OUTPUT,
                        Produced.with(Serdes.String(), new JsonSerde(Greeting.class, objectMapper)));

    }
}
