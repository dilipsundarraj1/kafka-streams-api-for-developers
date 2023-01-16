package com.learnkafkastreams.service;


import com.learnkafkastreams.domain.TotalRevenue;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

@Slf4j
public class OrdersService<T> {

    private KafkaStreams streams;

    ReadOnlyKeyValueStore<String, T> getStore(String storeName) {
        return streams.store(
                StoreQueryParameters.fromNameAndType(
                        // state store name
                        storeName,
                        // state store type
                        QueryableStoreTypes.keyValueStore()));
    }



}
