package com.learnkafkastreams.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record Order(Integer orderId,
                    String locationId,
                    BigDecimal finalAmount,
                    OrderType orderType,
                    List<OrderLineItem> orderLineItems,
                    LocalDateTime orderedDateTime) {
    public static record Order(Integer orderId,
                               String locationId,
                               BigDecimal finalAmount,
                               OrderType orderType,
                               List<OrderLineItem> orderLineItems,
                               LocalDateTime orderedDateTime) {
    }

    public static record OrderLineItem(
            String item,
            Integer count,
            BigDecimal amount) {
    }

    public enum OrderType {
        GENERAL,
        RESTAURANT
    }

    public static record Revenue(String locationId,
                                 BigDecimal finalAmount) {
    }

    public static record Store(String locationId,
                               Address address,
                               String contactNum) {
    }

    public static record TotalRevenue(String locationId,
                                      Integer runnuingOrderCount,
                                      BigDecimal runningRevenue) {

        public TotalRevenue() {
            this("", 0, BigDecimal.valueOf(0.0));
        }

        public TotalRevenue updateRunningRevenue(String key, com.learnkafkastreams.domain.Order order) {

            var newOrdersCount = this.runnuingOrderCount+1;
            var newRevenue = this.runningRevenue.add( order.finalAmount());
            return new TotalRevenue(key, newOrdersCount, newRevenue);

        }
    }

    public static record TotalRevenueWithAddress(com.learnkafkastreams.domain.TotalRevenue totalRevenue,
                                                 com.learnkafkastreams.domain.Store store) {
    }
}
