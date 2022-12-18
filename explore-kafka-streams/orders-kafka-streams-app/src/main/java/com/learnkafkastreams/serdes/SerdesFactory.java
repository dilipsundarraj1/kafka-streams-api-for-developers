package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.domain.TotalRevenue;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<Order> orderSerdes(){

        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();

        JsonDeserializer<Order> jsonDeSerializer = new JsonDeserializer<>(Order.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }

    public static Serde<Revenue> revenueSerdes() {
        JsonSerializer<Revenue> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Revenue> jsonDeSerializer = new JsonDeserializer<>(Revenue.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }

    public static Serde<TotalRevenue> totalRevenueSerdes() {

        JsonSerializer<TotalRevenue> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<TotalRevenue> jsonDeSerializer = new JsonDeserializer<>(TotalRevenue.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);

    }
}
