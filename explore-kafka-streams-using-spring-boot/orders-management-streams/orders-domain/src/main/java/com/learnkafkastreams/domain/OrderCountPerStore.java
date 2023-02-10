package com.learnkafkastreams.domain;

public record OrderCountPerStore(String locationId,
                                 Long orderCount) {
}
