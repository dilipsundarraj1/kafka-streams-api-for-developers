package com.learnkafkastreams.domain;

public record OrderRevenuePerStore(
        String locationId,

        OrderType orderType,
        TotalRevenue totalRevenue
) {
}
