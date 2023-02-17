package com.learnkafkastreams.domain;

import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.TotalRevenue;

import java.time.LocalDateTime;

public record OrdersRevenuePerStoreByWindowsDTO(String locationId,
                                                TotalRevenue totalRevenue,
                                                OrderType orderType,
                                                LocalDateTime startWindow,
                                                LocalDateTime endWindow) {
}
