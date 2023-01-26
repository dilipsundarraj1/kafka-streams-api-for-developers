package com.learnkafkastreams.topology;

import org.apache.kafka.streams.StreamsBuilder;
import org.springframework.beans.factory.annotation.Autowired;

public class OrdersTopology {

    public static String ORDERS = "orders";

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

    }
}
