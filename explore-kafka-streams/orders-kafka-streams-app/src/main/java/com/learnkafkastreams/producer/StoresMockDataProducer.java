package com.learnkafkastreams.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.Address;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;

@Slf4j
public class StoresMockDataProducer {

    public static void main(String[] args) {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);


        var address1 = new Address("1234 Street 1 ", "", "City1", "State1", "12345");
        var store1 = new Store("store_1234",
                address1,
                "1234567890"
        );

        var address2 = new Address("1234 Street 2 ", "", "City2", "State2", "541321");
        var store2 = new Store("store_4567",
                address2,
                "0987654321"
        );


        var stores = List.of(store1, store2);
        stores
                .forEach(store -> {
                    try {
                        var storeJSON = objectMapper.writeValueAsString(store);
                        var recordMetaData = publishMessageSync(OrdersTopology.STORES, store.locationId(), storeJSON);
                        log.info("Published the store message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        log.error("JsonProcessingException : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                    catch (Exception e) {
                        log.error("Exception : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                });

    }

}