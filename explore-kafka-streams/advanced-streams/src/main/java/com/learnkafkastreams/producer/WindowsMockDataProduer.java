package com.learnkafkastreams.producer;

import lombok.extern.slf4j.Slf4j;

import static com.learnkafkastreams.topology.ExploreWindowTopology.WINDOW_WORDS;
import static java.lang.Thread.sleep;
import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class WindowsMockDataProduer {



    public static void main(String[] args) throws InterruptedException {

        bulkMockDataProducer();
        //bulkMockDataProducer_SlidingWindows();

    }

    private static void bulkMockDataProducer() throws InterruptedException {
        var key = "A";
        var word = "Apple";
        int count = 0;
        while(count<100){
            var recordMetaData = publishMessageSync(WINDOW_WORDS, key,word);
            log.info("Published the alphabet message : {} ", recordMetaData);
            sleep(1000);
            count++;
        }
    }

    private static void bulkMockDataProducer_SlidingWindows() throws InterruptedException {
        var key = "A";
        var word = "Apple";
        int count = 0;
        while(count<10){
            var recordMetaData = publishMessageSync(WINDOW_WORDS, key,word);
            log.info("Published the alphabet message : {} ", recordMetaData);
            sleep(1000);
            count++;
        }
    }


}
