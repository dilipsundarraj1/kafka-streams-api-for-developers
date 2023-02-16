package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.domain.TotalRevenueWithAddress;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

@Service
public class OrderStoreService {

    StreamsBuilderFactoryBean streamsBuilderFactoryBean;
    public OrderStoreService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public ReadOnlyKeyValueStore<String, Long> ordersCountStore(String storeName){

        return streamsBuilderFactoryBean.getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(
                        // state store name
                        storeName,
                        // state store type
                        QueryableStoreTypes.keyValueStore()));

    }

    public ReadOnlyKeyValueStore<String, TotalRevenue> ordersRevenueWithAddressStore(String storeName){

        return streamsBuilderFactoryBean.getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(
                        // state store name
                        storeName,
                        // state store type
                        QueryableStoreTypes.keyValueStore()));

    }

    public ReadOnlyWindowStore<String, Long> ordersWindowCountStore(String storeName){

        return streamsBuilderFactoryBean.getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(
                        // state store name
                        storeName,
                        // state store type
                        QueryableStoreTypes.windowStore()));

    }


    public ReadOnlyWindowStore<String, TotalRevenue> ordersWindowRevenueStore(String storeName) {

        return streamsBuilderFactoryBean.getKafkaStreams()
                .store(StoreQueryParameters.fromNameAndType(
                        // state store name
                        storeName,
                        // state store type
                        QueryableStoreTypes.windowStore()));
    }
}
