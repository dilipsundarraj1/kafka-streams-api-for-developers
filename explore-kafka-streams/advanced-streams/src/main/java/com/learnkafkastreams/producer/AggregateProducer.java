package com.learnkafkastreams.producer;

import lombok.extern.slf4j.Slf4j;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;
import static com.learnkafkastreams.topology.ExploreAggregateOperatorsTopology.AGGREGATE;

@Slf4j
public class AggregateProducer {


    public static void main(String[] args) throws InterruptedException {

        var key = "A";
        //String key = null;

        var word = "Apple";
        var word1 = "Alligator";
        var word2 = "Ambulance";

        var recordMetaData = publishMessageSync(AGGREGATE, key,word);
        log.info("Published the alphabet message : {} ", recordMetaData);

        var recordMetaData1 = publishMessageSync(AGGREGATE, key,word1);
        log.info("Published the alphabet message : {} ", recordMetaData1);

        var recordMetaData2 = publishMessageSync(AGGREGATE, key,word2);
        log.info("Published the alphabet message : {} ", recordMetaData2);

        var bKey = "B";
        //String bKey = null;

        var bWord1 = "Bus";
        var bWord2 = "Baby";
        var recordMetaData3 = publishMessageSync(AGGREGATE, bKey,bWord1);
        log.info("Published the alphabet message : {} ", recordMetaData2);

        var recordMetaData4 = publishMessageSync(AGGREGATE, bKey,bWord2);
        log.info("Published the alphabet message : {} ", recordMetaData2);

    }



}
