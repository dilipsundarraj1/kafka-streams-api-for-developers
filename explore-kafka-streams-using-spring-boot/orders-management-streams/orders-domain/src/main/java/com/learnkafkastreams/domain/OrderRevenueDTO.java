package com.learnkafkastreams.domain;

import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.TotalRevenue;

public record OrderRevenueDTO(
        String locationId,

        OrderType orderType,
        TotalRevenue totalRevenue
) {
}
