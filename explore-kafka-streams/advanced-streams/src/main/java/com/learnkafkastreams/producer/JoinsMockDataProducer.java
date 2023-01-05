package com.learnkafkastreams.producer;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;
import static com.learnkafkastreams.topology.ExploreJoinsOperatorsTopology.ALPHABETS_ABBREVATIONS;

@Slf4j
public class JoinsMockDataProducer {


    public static void main(String[] args) throws InterruptedException {


        var alphabetMap = Map.of(
                "A", "A is the first letter in English Alphabets.",
                "B", "B is the second letter in English Alphabets."
  //              ,"E", "E is the fifth letter in English Alphabets."
//                ,
//                "A", "A is the First letter in English Alphabets.",
//                "B", "B is the Second letter in English Alphabets."
        );
       // publishMessages(alphabetMap, ALPHABETS);

       // sleep(6000);

        var alphabetAbbrevationMap = Map.of(
                "A", "Apple",
                "B", "Bus."
                ,"C", "Cat."

        );
       publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);

        alphabetAbbrevationMap = Map.of(
                "A", "Airplane",
                "B", "Baby."

        );
       // publishMessages(alphabetAbbrevationMap, ALPHABETS_ABBREVATIONS);

    }

    private static void publishMessages(Map<String, String> alphabetMap, String topic) {

        alphabetMap
                .forEach((key, value) -> {
                    var recordMetaData = publishMessageSync(topic, key,value);
                    log.info("Published the alphabet message : {} ", recordMetaData);
                });
    }



}
