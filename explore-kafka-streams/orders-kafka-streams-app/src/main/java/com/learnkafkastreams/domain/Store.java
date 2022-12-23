package com.learnkafkastreams.domain;

public record Store(String locationId,
                    Address address,
                    String contactNum) {
}
