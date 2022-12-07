package com.learnkafkastreams.domain;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class OrderTest {


    private final ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule())
            .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);


    @Test
    void orderDomainTest() throws JsonProcessingException {

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var order = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.parse("2022-12-05T08:55:27")
                );

        var orderJSON = objectMapper.writeValueAsString(order);
        System.out.println("orderJSON :"+orderJSON);
        var expectedJSON = "{\"orderId\":12345,\"locationId\":\"store_1234\",\"finalAmount\":27.00,\"orderType\":\"GENERAL\",\"orderLineItems\":[{\"item\":\"Bananas\",\"count\":2,\"amount\":2.00},{\"item\":\"Iphone Charger\",\"count\":1,\"amount\":25.00}],\"orderedDateTime\":\"2022-12-05T08:55:27\"}";

        assertEquals(expectedJSON, orderJSON);
    }

    @Test
    void orderRecordDomainTest() throws JsonProcessingException {

        var orderItems = List.of(
                new OrderLineItem("Bananas", 2, new BigDecimal("2.00")),
                new OrderLineItem("Iphone Charger", 1, new BigDecimal("25.00"))
        );

        var order = new Order(12345, "store_1234",
                new BigDecimal("27.00"),
                OrderType.GENERAL,
                orderItems,
                LocalDateTime.parse("2022-12-05T08:55:27")
        );

        var orderJSON = objectMapper.writeValueAsString(order);
        System.out.println("orderJSON :"+orderJSON);
        var expectedJSON = "{\"orderId\":12345,\"locationId\":\"store_1234\",\"finalAmount\":27.00,\"orderType\":\"GENERAL\",\"orderLineItems\":[{\"item\":\"Bananas\",\"count\":2,\"amount\":2.00},{\"item\":\"Iphone Charger\",\"count\":1,\"amount\":25.00}],\"orderedDateTime\":\"2022-12-05T08:55:27\"}";

        assertEquals(expectedJSON, orderJSON);
    }

    @Test
    void orderDomainRestaurantTest() throws JsonProcessingException {

        var orderItems = List.of(
                new OrderLineItem("Pizza", 2, new BigDecimal("12.00")),
                new OrderLineItem("Coffee", 1, new BigDecimal("3.00"))
        );

        var order = new Order(12345, "store_1234",
                new BigDecimal("15.00"),
                OrderType.RESTAURANT,
                orderItems,
                LocalDateTime.parse("2022-12-05T08:55:27")
        );

        var orderJSON = objectMapper.writeValueAsString(order);
        System.out.println("orderJSON :"+orderJSON);
        var expectedJSON = "{\"orderId\":12345,\"locationId\":\"store_1234\",\"finalAmount\":15.00,\"orderType\":\"RESTAURANT\",\"orderLineItems\":[{\"item\":\"Pizza\",\"count\":2,\"amount\":12.00},{\"item\":\"Coffee\",\"count\":1,\"amount\":3.00}],\"orderedDateTime\":\"2022-12-05T08:55:27\"}";

        assertEquals(expectedJSON, orderJSON);
    }
}
