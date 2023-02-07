package com.learnkafkastreams.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.learnkafkastreams.domain.OrderLineItem;
import com.learnkafkastreams.domain.Order;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static com.learnkafkastreams.producer.ProducerUtil.publishMessageSync;
import static java.lang.Thread.sleep;

@Slf4j
public class OrdersMockDataProducer {

    public static void main(String[] args) throws InterruptedException {
        ObjectMapper objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

        publishOrders(objectMapper, buildOrders());
        //publishBulkOrders(objectMapper);

        //grace-period
//        publishOrdersForGracePeriod(objectMapper, buildOrders());

        //Future and Old Records
//        publishFutureRecords(objectMapper);
//        publishExpiredRecords(objectMapper);


    }

    private static void publishFutureRecords(ObjectMapper objectMapper) {
        var localDateTime = LocalDateTime.now().plusDays(1);

        var newOrders = buildOrders()
                .stream()
                .map(order ->
                        new Order(order.orderId(),
                                order.locationId(),
                                order.finalAmount(),
                                order.orderType(),
                                order.orderLineItems(),
                                localDateTime))
                .toList();
        publishOrders(objectMapper, newOrders);
    }

    private static void publishExpiredRecords(ObjectMapper objectMapper) {

        var localDateTime = LocalDateTime.now().minusDays(1);

        var newOrders = buildOrders()
                .stream()
                .map(order ->
                        new Order(order.orderId(),
                                order.locationId(),
                                order.finalAmount(),
                                order.orderType(),
                                order.orderLineItems(),
                                localDateTime))
                .toList();
        publishOrders(objectMapper, newOrders);

    }

    private static void publishOrdersForGracePeriod(ObjectMapper objectMapper, List<Order> orders) {

        var localTime = LocalDateTime.now().toLocalTime();
        var modifiedTime = LocalTime.of(localTime.getHour(), localTime.getMinute(), 18);
        var localDateTime = LocalDateTime.now().with(modifiedTime);

        //With Grace Period
        //[general_orders_revenue_window]: , TotalRevenue[locationId=store_4567, runnuingOrderCount=1, runningRevenue=27.00]
        //[general_orders_revenue_window]: TotalRevenue[locationId=store_1234, runnuingOrderCount=1, runningRevenue=27.00]
        //[general_orders_revenue_window]:  TotalRevenue[locationId=store_4567, runnuingOrderCount=1, runningRevenue=27.00]
        //[general_orders_revenue_window]:  TotalRevenue[locationId=store_4567, runnuingOrderCount=1, runningRevenue=27.00]

        //Without Grace Period
        //[general_orders_revenue_window]: , TotalRevenue[locationId=store_4567, runnuingOrderCount=1, runningRevenue=27.00]
        //[general_orders_revenue_window]: TotalRevenue[locationId=store_1234, runnuingOrderCount=1, runningRevenue=27.00]
        //[general_orders_revenue_window]:  TotalRevenue[locationId=store_4567, runnuingOrderCount=1, runningRevenue=27.00]
        //[general_orders_revenue_window]:  TotalRevenue[locationId=store_4567, runnuingOrderCount=1, runningRevenue=27.00]



        var generalOrdersWithCustomTime = orders
                .stream()
                .filter(order -> order.orderType().equals(OrderType.GENERAL))
                .map(order ->
                        new Order(order.orderId(),
                                order.locationId(),
                                order.finalAmount(),
                                order.orderType(),
                                order.orderLineItems(),
                                localDateTime))
                .toList();

        var generalOrders = orders
                .stream()
                .filter(order -> order.orderType().equals(OrderType.GENERAL))
                .toList();

        publishOrders(objectMapper, generalOrders);

        //orders with the timestamp as 18th second
        publishRecordsWithDelay(generalOrdersWithCustomTime, localDateTime, objectMapper, 18);

    }

    private static void publishRecordsWithDelay(List<Order> newOrders, LocalDateTime localDateTime, ObjectMapper objectMapper) {

        publishOrders(objectMapper, newOrders);
    }

    private static void publishRecordsWithDelay(List<Order> newOrders, LocalDateTime localDateTime, ObjectMapper objectMapper, int timeToPublish) {

        var flag = true;
        while (flag) {
            var dateTime = LocalDateTime.now();
            if (dateTime.toLocalTime().getMinute() == localDateTime.getMinute()
                    && dateTime.toLocalTime().getSecond() == timeToPublish) {
                System.out.printf("Publishing the record with delay ");
                publishOrders(objectMapper, newOrders);
                flag = false;
            } else {
                System.out.println(" Current Time is  and the record will be published at the 16th second: " + dateTime);
                System.out.println("Record Date Time : " + localDateTime);
            }
        }
    }

    private static List<Order> buildOrdersForGracePeriod() {

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var orderItemsRestaurant = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order1 = new Order(12345, "store_999",
                new BigDecimal("27.00"),
                OrderType.RESTAURANT,
                orderItems,
                LocalDateTime.parse("2023-01-06T18:50:21")
        );

        var order2 = new Order(54321, "store_999",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.parse("2023-01-06T18:50:21")
        );

        var order3 = new Order(54321, "store_999",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItemsRestaurant,
                LocalDateTime.parse("2023-01-06T18:50:22")
        );

        return List.of(
                order1,
                order2,
                order3
        );

    }

    private static List<Order> buildOrders() {
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

        var order3 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.now()
                //LocalDateTime.now(ZoneId.of("UTC"))
        );

        var order4 = new Order(12345, "store_4567",
                new BigDecimal("27.00"),
                OrderType.RESTAURANT,
                orderItems,
                LocalDateTime.now()
                //LocalDateTime.now(ZoneId.of("UTC"))
        );

        return List.of(
                order1,
                order2,
                order3,
                order4
        );
    }

    private static void publishBulkOrders(ObjectMapper objectMapper) throws InterruptedException {

        int count = 0;
        while (count < 100) {
            var orders = buildOrders();
            publishOrders(objectMapper, orders);
            sleep(1000);
            count++;
        }
    }

    private static void publishOrders(ObjectMapper objectMapper, List<Order> orders) {

        orders
                .forEach(order -> {
                    try {
                        var ordersJSON = objectMapper.writeValueAsString(order);
                        var recordMetaData = publishMessageSync(OrdersTopology.ORDERS, order.orderId() + "", ordersJSON);
                        log.info("Published the order message : {} ", recordMetaData);
                    } catch (JsonProcessingException e) {
                        log.error("JsonProcessingException : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    } catch (Exception e) {
                        log.error("Exception : {} ", e.getMessage(), e);
                        throw new RuntimeException(e);
                    }
                });
    }


}
