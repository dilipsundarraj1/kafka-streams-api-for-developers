package com.learnkafkastreams.domain;

public record AllOrdersCountPerStore(String locationId,
                                     Long orderCount,
                                     OrderType orderType) {
}
