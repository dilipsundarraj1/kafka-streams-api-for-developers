package com.learnkafkastreams.domain;

import com.learnkafkastreams.domain.OrderType;

public record AllOrdersCountPerStoreDTO(String locationId,
                                        Long orderCount,
                                        OrderType orderType) {
}
