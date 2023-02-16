package com.learnkafkastreams.domain;

import java.time.LocalDateTime;

public record OrdersRevenuePerStoreByWindows(String locationId,
                                             TotalRevenue totalRevenue,
                                             OrderType orderType,
                                             LocalDateTime startWindow,
                                             LocalDateTime endWindow) {
}
