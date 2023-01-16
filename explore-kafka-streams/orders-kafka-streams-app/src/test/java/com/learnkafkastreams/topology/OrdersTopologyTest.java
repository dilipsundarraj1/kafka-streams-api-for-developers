package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;

import static com.learnkafkastreams.topology.OrdersTopology.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

class OrdersTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, Order> ordersInputTopic = null;

    TestInputTopic<String, Store> storesInputTopic = null;
    static String INPUT_TOPIC = ORDERS;

    static String STORES_INPUT_TOPIC = STORES;


    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(OrdersTopology.buildTopology());

        ordersInputTopic =
                topologyTestDriver.
                        createInputTopic(
                                INPUT_TOPIC, Serdes.String().serializer(), SerdesFactory.orderSerdes().serializer());


        storesInputTopic =
                topologyTestDriver.
                        createInputTopic(
                                STORES, Serdes.String().serializer(), SerdesFactory.storeSerdes().serializer());

    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void ordersRevenue() {

        var address1 = new Address("1234 Street 1 ", "", "City1", "State1", "12345");
        var store1 = new Store("store_1234",
                address1,
                "1234567890"
        );

        storesInputTopic.pipeInput(store1.locationId(), store1);

        ordersInputTopic.pipeKeyValueList(orders());

        ReadOnlyKeyValueStore<String, TotalRevenue> generalOrdersRevenue = topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_REVENUE);
        System.out.println("ordersRevenueStore : " + generalOrdersRevenue);

        var generalOrderWithRevenue = generalOrdersRevenue.get("store_1234");
        assertEquals(1, generalOrderWithRevenue.runnuingOrderCount());
        assertEquals(new BigDecimal("27.00"), generalOrderWithRevenue.runningRevenue());



        ReadOnlyKeyValueStore<String, TotalRevenue> restaurantOrdersRevenue = topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_REVENUE);
        System.out.println("restaurantOrdersRevenue : " + restaurantOrdersRevenue);

        var restaurantOrderWithRevenue = restaurantOrdersRevenue.get("store_1234");
        assertEquals(1, restaurantOrderWithRevenue.runnuingOrderCount());
        assertEquals(new BigDecimal("15.00"), restaurantOrderWithRevenue.runningRevenue());

    }

    @Test
    void ordersRevenue_multipleOrdersPerStore() {

        var address1 = new Address("1234 Street 1 ", "", "City1", "State1", "12345");
        var store1 = new Store("store_1234",
                address1,
                "1234567890"
        );

        storesInputTopic.pipeInput(store1.locationId(), store1);

        ordersInputTopic.pipeKeyValueList(orders());

        ordersInputTopic.pipeKeyValueList(orders());

        ReadOnlyKeyValueStore<String, TotalRevenue> generalOrdersRevenue = topologyTestDriver.getKeyValueStore(GENERAL_ORDERS_REVENUE);
        System.out.println("ordersRevenueStore : " + generalOrdersRevenue);

        var generalOrderWithRevenue = generalOrdersRevenue.get("store_1234");
        assertEquals(2, generalOrderWithRevenue.runnuingOrderCount());
        assertEquals(new BigDecimal("54.00"), generalOrderWithRevenue.runningRevenue());



        ReadOnlyKeyValueStore<String, TotalRevenue> restaurantOrdersRevenue = topologyTestDriver.getKeyValueStore(RESTAURANT_ORDERS_REVENUE);
        System.out.println("restaurantOrdersRevenue : " + restaurantOrdersRevenue);

        var restaurantOrderWithRevenue = restaurantOrdersRevenue.get("store_1234");
        assertEquals(2, restaurantOrderWithRevenue.runnuingOrderCount());
        assertEquals(new BigDecimal("30.00"), restaurantOrderWithRevenue.runningRevenue());

    }

    @Test
    void ordersRevenue_byWindows() {

        var address1 = new Address("1234 Street 1 ", "", "City1", "State1", "12345");
        var store1 = new Store("store_1234",
                address1,
                "1234567890"
        );

        storesInputTopic.pipeInput(store1.locationId(), store1);

        ordersInputTopic.pipeKeyValueList(orders());
        ordersInputTopic.pipeKeyValueList(orders());
        //topologyTestDriver.advanceWallClockTime(Duration.ofSeconds(30));

        var generalOrdersRevenue = topologyTestDriver.getWindowStore(GENERAL_ORDERS_REVENUE_WINDOWS);
        System.out.println("ordersRevenueStore : " + generalOrdersRevenue);
        generalOrdersRevenue
                .all()
                .forEachRemaining(totalRevenueKeyValue -> {
                    System.out.println("Key : " + totalRevenueKeyValue.key);
                    System.out.println("Value : " + totalRevenueKeyValue.value);
                });

//        var generalOrderWithRevenue = generalOrdersRevenue.get("store_1234");
//        assertEquals(2, generalOrderWithRevenue.runnuingOrderCount());
//        assertEquals(new BigDecimal("54.00"), generalOrderWithRevenue.runningRevenue());



        var restaurantOrdersRevenue = topologyTestDriver.getWindowStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
        System.out.println("restaurantOrdersRevenue : " + restaurantOrdersRevenue);

//        var restaurantOrderWithRevenue = restaurantOrdersRevenue.get("store_1234");
//        assertEquals(2, restaurantOrderWithRevenue.runnuingOrderCount());
//        assertEquals(new BigDecimal("30.00"), restaurantOrderWithRevenue.runningRevenue());

    }


    static List<KeyValue<String, Order>> orders(){

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()
                //LocalDateTime.now(ZoneId.of("UTC"))
        );

        var order2 = new Order(54321, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.now()
                //LocalDateTime.now(ZoneId.of("UTC"))
        );
        var keyValue1 = KeyValue.pair( order1.orderId().toString()
                , order1);

        var keyValue2 = KeyValue.pair( order2.orderId().toString()
                , order2);


        return  List.of(keyValue1, keyValue2);

    }
}