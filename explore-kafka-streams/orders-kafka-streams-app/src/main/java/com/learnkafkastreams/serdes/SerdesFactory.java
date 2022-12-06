package com.learnkafkastreams.serdes;

import com.learnkafkastreams.domain.Order;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class SerdesFactory {

    public static Serde<Order> orderSerdes(){

        JsonSerializer<Order> jsonSerializer = new JsonSerializer<>();

        JsonDeserializer<Order> jsonDeSerializer = new JsonDeserializer<>(Order.class);
        return  Serdes.serdeFrom(jsonSerializer, jsonDeSerializer);
    }

}
