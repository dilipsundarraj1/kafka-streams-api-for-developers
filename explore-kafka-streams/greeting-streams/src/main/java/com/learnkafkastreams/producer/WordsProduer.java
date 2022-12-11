package com.learnkafkastreams.producer;

import lombok.extern.slf4j.Slf4j;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class WordsProduer {

    static String topicName = "words";

    public static void main(String[] args) throws InterruptedException {

        var key = "A";

        var word = "Apple";
        var word1 = "Alligator";
        var word2 = "Ambulance";

        var recordMetaData = publishMessageSync(topicName, key,word);
        log.info("Published the alphabet message : {} ", recordMetaData);

        var recordMetaData1 = publishMessageSync(topicName, key,word1);
        log.info("Published the alphabet message : {} ", recordMetaData1);

        var recordMetaData2 = publishMessageSync(topicName, key,word2);
        log.info("Published the alphabet message : {} ", recordMetaData2);

    }



}
