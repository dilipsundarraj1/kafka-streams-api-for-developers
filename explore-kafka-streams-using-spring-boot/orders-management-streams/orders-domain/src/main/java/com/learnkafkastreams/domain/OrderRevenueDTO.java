package com.learnkafkastreams.domain;

public record OrderRevenueDTO(
        String locationId,

        OrderType orderType,
        TotalRevenue totalRevenue
) {
}
