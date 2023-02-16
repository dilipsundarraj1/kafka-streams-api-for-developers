package com.learnkafkastreams.domain;

public record AllOrdersCountPerStoreDTO(String locationId,
                                        Long orderCount,
                                        OrderType orderType) {
}
