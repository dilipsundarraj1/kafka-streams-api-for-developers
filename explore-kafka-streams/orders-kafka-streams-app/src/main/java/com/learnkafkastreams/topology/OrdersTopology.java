package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.serdes.SerdesFactory;
import com.learnkafkastreams.util.OrderTimeStampExtractor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;


@Slf4j
public class OrdersTopology {
    public static final String ORDERS = "orders";
    public static final String GENERAL_ORDERS = "general_orders";
    public static final String GENERAL_ORDERS_COUNT = "general_orders_count";
    public static final String GENERAL_ORDERS_COUNT_WINDOWS = "general_orders_count_window";
    public static final String GENERAL_ORDERS_REVENUE = "general_orders_revenue";
    public static final String GENERAL_ORDERS_REVENUE_WINDOWS = "general_orders_revenue_window";

    public static final String RESTAURANT_ORDERS = "restaurant_orders";
    public static final String RESTAURANT_ORDERS_COUNT = "restaurant_orders_count";
    public static final String RESTAURANT_ORDERS_REVENUE = "restaurant_orders_revenue";
    public static final String RESTAURANT_ORDERS_COUNT_WINDOWS = "restaurant_orders_count_window";
    public static final String RESTAURANT_ORDERS_REVENUE_WINDOWS = "restaurant_orders_revenue_window";




    public static final String STORES = "stores";


    public static Topology buildTopology() {

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        Predicate<String, Order> generalPredicate = (key, order) -> order.orderType().equals(OrderType.GENERAL);
        Predicate<String, Order> restaurantPredicate = (key, order) -> order.orderType().equals(OrderType.RESTAURANT);


        var orderStreams = streamsBuilder
                .stream(ORDERS,
                        Consumed.with(Serdes.String(), SerdesFactory.orderSerdes())
                        .withTimestampExtractor(new OrderTimeStampExtractor())
                );

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

                            aggregateOrdersByCount(generalOrdersStream, GENERAL_ORDERS_COUNT);
                           aggregateOrdersCountByTimeWindows(generalOrdersStream, GENERAL_ORDERS_COUNT_WINDOWS);
                            aggregateOrdersByRevenue(generalOrdersStream, GENERAL_ORDERS_REVENUE, storesTable);
                            aggregateOrdersRevenueByWindows(generalOrdersStream, GENERAL_ORDERS_REVENUE_WINDOWS, storesTable);

                        }))
                .branch(restaurantPredicate,
                        Branched.withConsumer(restaurantOrdersStream -> {
//                            restaurantOrdersStream
//                                    .mapValues((readOnlyKey, value) -> revenueMapper.apply(value))
//                                    .to(RESTAURANT_ORDERS,
//                                    //        Produced.with(Serdes.String(), SerdesFactory.orderSerdes())
//                                            Produced.with(Serdes.String(), SerdesFactory.revenueSerdes())
//                                    );

                            aggregateOrdersByCount(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT);
                           aggregateOrdersCountByTimeWindows(restaurantOrdersStream, RESTAURANT_ORDERS_COUNT_WINDOWS);
                            aggregateOrdersByRevenue(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE, storesTable);
                           aggregateOrdersRevenueByWindows(restaurantOrdersStream, RESTAURANT_ORDERS_REVENUE_WINDOWS, storesTable);
                        }));



        return streamsBuilder.build();
    }

    private static void aggregateOrdersByRevenue(KStream<String, Order> generalOrdersStream, String aggregateStoreName, KTable<String, Store> storesTable) {


        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator   = (key,order, totalRevenue )-> {
            return totalRevenue.updateRunningRevenue(key, order);
        };

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .aggregate(totalRevenueInitializer,
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

    private static void aggregateOrdersRevenueByWindows(KStream<String, Order> generalOrdersStream, String aggregateStoreName, KTable<String, Store> storesTable) {

        Duration windowSize = Duration.ofSeconds(15);
        Duration graceWindowsSize = Duration.ofSeconds(5);

       // TimeWindows timeWindow = TimeWindows.ofSizeWithNoGrace(windowSize);
        TimeWindows timeWindow = TimeWindows.ofSizeAndGrace(windowSize, graceWindowsSize);
        Initializer<TotalRevenue> alphabetWordAggregateInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator   = (key,order, totalRevenue )-> {
            return totalRevenue.updateRunningRevenue(key, order);
        };

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindow)
                .aggregate(alphabetWordAggregateInitializer,
                        aggregator
                        ,Materialized
                                .<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(aggregateStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.totalRevenueSerdes())
                );

        revenueTable
                .toStream()
                .peek(((key, value) -> {
                    log.info(" {} : tumblingWindow : key : {}, value : {}",aggregateStoreName, key, value);
                    printLocalDateTimes(key, value);
                }))
                .print(Printed.<Windowed<String>,TotalRevenue>toSysOut().withLabel(aggregateStoreName));

        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;

        var joinedParams =
                Joined.with(Serdes.String(), SerdesFactory.totalRevenueSerdes(), SerdesFactory.storeSerdes());
        revenueTable
                .toStream()
                .peek(((key, value) -> {
                    log.info(" {} : tumblingWindow : key : {}, value : {}",aggregateStoreName, key, value);
                    printLocalDateTimes(key, value);
                }))
                .map((key, value) -> KeyValue.pair(key.key(), value))
                .join(storesTable,valueJoiner
                        ,joinedParams
                )
                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(aggregateStoreName+"-bystore"));

    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName) {

        var generalOrdersCount = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .count(Named.as(storeName),
                        Materialized.as(storeName));

        generalOrdersCount
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel(storeName));

    }

    private static void aggregateOrdersCountByTimeWindows(KStream<String, Order> generalOrdersStream, String storeName) {

        Duration windowSize = Duration.ofSeconds(15);
        Duration graceWindowsSize = Duration.ofSeconds(5);

        //TimeWindows timeWindow = TimeWindows.ofSizeWithNoGrace(windowSize);
        TimeWindows timeWindow = TimeWindows.ofSizeAndGrace(windowSize, graceWindowsSize);

        var generalOrdersCount = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), SerdesFactory.orderSerdes()))
                .windowedBy(timeWindow)
                .count(Named.as(storeName),
                        Materialized.as(storeName))
                .suppress(Suppressed
                        .untilWindowCloses(Suppressed.BufferConfig.unbounded().shutDownWhenFull())
                );

        generalOrdersCount
                .toStream()
                .peek(((key, value) -> {
                    log.info(" {} : tumblingWindow : key : {}, value : {}",storeName, key, value);
                    printLocalDateTimes(key, value);
                }))
                .print(Printed.<Windowed<String>, Long>toSysOut().withLabel(storeName));

    }

    private static void printLocalDateTimes(Windowed<String> key, Object value) {
        var startTime = key.window().startTime();
        var endTime = key.window().endTime();
        log.info("startTime : {} , endTime : {}, Count : {}", startTime, endTime, value);
        LocalDateTime startLDT = LocalDateTime.ofInstant(startTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        LocalDateTime endLDT = LocalDateTime.ofInstant(endTime, ZoneId.of(ZoneId.SHORT_IDS.get("CST")));
        log.info("startLDT : {} , endLDT : {}, Count : {}", startLDT, endLDT, value);
    }
}
