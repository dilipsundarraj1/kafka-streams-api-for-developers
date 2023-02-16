package com.learnkafkastreams.domain;

import java.time.LocalDateTime;

public record OrdersCountPerStoreByWindows(String locationId,
                                           Long orderCount,
                                           OrderType orderType,
                                           LocalDateTime startWindow,
                                           LocalDateTime endWindow) {
}
