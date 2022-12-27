package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.math.BigDecimal;


@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String RESTAURANT_ORDERS = "restaurant_orders";

    public static final String STORES = "stores";


    public static Topology buildTopology() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);


        var orderStreams = streamsBuilder
                .stream(ORDERS, Consumed.with(Serdes.String(), SerdesFactory.orderSerdes()));

        var storesTable = streamsBuilder
                .table(STORES,
                        Consumed.with(Serdes.String(), SerdesFactory.storeSerdes()));

        storesTable
                .toStream()
                        .print(Printed.<String,Store>toSysOut().withLabel("stores"));

        orderStreams
                .print(Printed.<String, Order>toSysOut().withLabel("orders"));

        ValueMapper<Order, Revenue> revenueMapper = (order) -> new Revenue(order.locationId(), order.finalAmount());


        orderStreams
                .filter((key, value) -> value.finalAmount().compareTo( new BigDecimal("10.00")) > 0)
                .split(Named.as("General-restaurant-stream"))
                .branch(generalPredicate,
                        Branched.withConsumer(generalOrdersStream -> {
//                            generalOrdersStream
//                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
//                                    .to(GENERAL_ORDERS,
//                                    //        Produced.with(Serdes.String(), SerdesFactory.orderSerdes())
//                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes())
//                                    );

                            aggregateOrdersByCount(generalOrdersStream, "general-orders-count");
                            aggregateOrdersByRevenue(generalOrdersStream, "general-orders-revenue", storesTable);
                        }))
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
//                            restaurantOrdersStream
//                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
//                                    .to(RESTAURANT_ORDERS,
//                                    //        Produced.with(Serdes.String(), SerdesFactory.orderSerdes())
//                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes())
//                                    );

                            aggregateOrdersByCount(restaurantOrdersStream, "restaurant-orders-count");
                            aggregateOrdersByRevenue(restaurantOrdersStream, "restaurant-orders-revenue", storesTable);
                        }));



        return streamsBuilder.build();
    }

    private static void aggregateOrdersByRevenue(KStream<String, Order> generalOrdersStream, String aggregateStoreName, KTable<String, Store> storesTable) {


        Initializer<TotalRevenue> alphabetWordAggregateInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator   = (key,order, totalRevenue )-> {
            return totalRevenue.updateRunningRevenue(key, order);
        };

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .aggregate(alphabetWordAggregateInitializer,
                        aggregator,
                        Materialized
                                .<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(aggregateStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())
                        );

        revenueTable
                .toStream()
                .print(Printed.<String,TotalRevenue>toSysOut().withLabel(aggregateStoreName));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var revenueWithStoreTable = revenueTable
                .leftJoin(storesTable,valueJoiner);

        revenueWithStoreTable
                .toStream()
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(aggregateStoreName+"-bystore"));


    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName) {

        var generalOrdersCount = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .count(Named.as(storeName));

        generalOrdersCount
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel(storeName));

    }
}
