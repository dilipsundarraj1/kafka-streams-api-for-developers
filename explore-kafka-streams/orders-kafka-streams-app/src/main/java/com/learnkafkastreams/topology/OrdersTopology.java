package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.Revenue;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.math.BigDecimal;


@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";


    public static Topology buildTopology() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);


        var orderStreams = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerdes()));

        orderStreams
                .print(Printed.<String, Order>toSysOut().withLabel("orders"));

        ValueMapper<Order, Revenue> revenueMapper = (order) -> new Revenue(order.locationId(), order.finalAmount());

        orderStreams
                .filter((key, value) -> value.finalAmount().compareTo( new BigDecimal("10.00")) > 0)
                .split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrdersStream -> {
                            generalOrdersStream
                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
                                    .to(GENERAL_ORDERS,
                                    //        Produced.with(Serdes.String(), SerdesFactory.orderSerdes())
                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes())
                                    );
                        }))
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
                            restaurantOrdersStream
                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
                                    .to(RESTAURANT_ORDERS,
                                    //        Produced.with(Serdes.String(), SerdesFactory.orderSerdes())
                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes())
                                    );
                        }));



        return streamsBuilder.build();
    }
}
