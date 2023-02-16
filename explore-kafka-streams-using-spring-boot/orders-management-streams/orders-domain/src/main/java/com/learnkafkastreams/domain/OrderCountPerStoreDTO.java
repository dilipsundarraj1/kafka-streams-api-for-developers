package com.learnkafkastreams.domain;

public record OrderCountPerStoreDTO(String locationId,
                                    Long orderCount) {
}
