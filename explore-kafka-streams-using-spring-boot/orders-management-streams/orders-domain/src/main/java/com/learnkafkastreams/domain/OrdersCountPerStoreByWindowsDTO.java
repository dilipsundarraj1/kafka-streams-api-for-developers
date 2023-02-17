package com.learnkafkastreams.domain;

import com.learnkafkastreams.domain.OrderType;

import java.time.LocalDateTime;

public record OrdersCountPerStoreByWindowsDTO(String locationId,
                                              Long orderCount,
                                              OrderType orderType,
                                              LocalDateTime startWindow,
                                              LocalDateTime endWindow) {
}
