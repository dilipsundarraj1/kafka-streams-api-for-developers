package com.learnkafkastreams.producer;

import com.learnkafkastreams.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class GreetingMockDataProducer {

    public static void main(String[] args) throws InterruptedException {

        var greetings = List.of( "Hello", "Good Morning!", "Good Evening");
        greetings
                .forEach(greeting -> {
                    var recordMetaData = publishMessageSync(GreetingsTopology.GREETINGS, null,greeting);
                    log.info("Published the alphabet message : {} ", recordMetaData);
                });

    }

}
