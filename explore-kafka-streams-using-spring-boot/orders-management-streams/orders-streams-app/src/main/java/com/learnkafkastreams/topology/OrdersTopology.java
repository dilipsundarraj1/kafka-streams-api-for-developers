package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.Store;
import com.learnkafkastreams.domain.TotalRevenue;
import com.learnkafkastreams.domain.TotalRevenueWithAddress;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneId;

@Slf4j
public class OrdersTopology {

    public static String ORDERS = "orders";

    @Autowired
    public void process(StreamsBuilder streamsBuilder) {

    }


    private static void aggregateOrdersByRevenue(KStream<String, Order> generalOrdersStream, String aggregateStoreName, KTable<String, Store> storesTable) {


        Initializer<TotalRevenue> totalRevenueInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator   = (key, order, totalRevenue )-> {
            return totalRevenue.updateRunningRevenue(key, order);
        };

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(),new JsonSerde<>(Order.class)))
                .aggregate(totalRevenueInitializer,
                        aggregator,
                        Materialized
                                .<String, TotalRevenue, KeyValueStore<Bytes, byte[]>>as(aggregateStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class))
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

        var windowSize =30;
        Duration windowSizeDuration = Duration.ofSeconds(windowSize);
        Duration graceWindowsSize = Duration.ofSeconds(5);

        TimeWindows hoppingWindow = TimeWindows.ofSizeAndGrace(windowSizeDuration, graceWindowsSize);

        Initializer<TotalRevenue> alphabetWordAggregateInitializer = TotalRevenue::new;

        Aggregator<String, Order, TotalRevenue> aggregator   = (key,order, totalRevenue )-> {
            return totalRevenue.updateRunningRevenue(key, order);
        };

        var revenueTable = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .windowedBy(hoppingWindow)
                .aggregate(alphabetWordAggregateInitializer,
                        aggregator
                        ,Materialized
                                .<String, TotalRevenue, WindowStore<Bytes, byte[]>>as(aggregateStoreName)
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new JsonSerde<>(TotalRevenue.class))
                );

        revenueTable
                .toStream()
                .peek(((key, value) -> {
                    log.info(" {} : tumblingWindow : key : {}, value : {}",aggregateStoreName, key, value);
                    printLocalDateTimes(key, value);
                }))
                .print(Printed.<Windowed<String>,TotalRevenue>toSysOut().withLabel(aggregateStoreName));
//
//        ValueJoiner<TotalRevenue, Store, TotalRevenueWithAddress> valueJoiner = TotalRevenueWithAddress::new;
//
//        revenueTable
//                .toStream()
//                .map((key, value) -> KeyValue.pair(key.key(), value))
//                .leftJoin(storesTable,valueJoiner)
//                .print(Printed.<String,TotalRevenueWithAddress>toSysOut().withLabel(aggregateStoreName+"-bystore"));

    }

    private static void aggregateOrdersByCount(KStream<String, Order> generalOrdersStream, String storeName) {

        var generalOrdersCount = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .count(Named.as(storeName),
                        Materialized.as(storeName));

        generalOrdersCount
                .toStream()
                .print(Printed.<String,Long>toSysOut().withLabel(storeName));

    }

    private static void aggregateOrdersCountByTimeWindows(KStream<String, Order> generalOrdersStream, String storeName) {

        Duration windowSize = Duration.ofSeconds(30);
        Duration graceWindowsSize = Duration.ofSeconds(10);

        TimeWindows hoppingWindow = TimeWindows.ofSizeAndGrace(windowSize, graceWindowsSize);

        var generalOrdersCount = generalOrdersStream
                .map((key, value) -> KeyValue.pair(value.locationId(), value))
                .groupByKey(Grouped.with(Serdes.String(), new JsonSerde<>(Order.class)))
                .windowedBy(hoppingWindow)
                .count(Named.as(storeName))
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
