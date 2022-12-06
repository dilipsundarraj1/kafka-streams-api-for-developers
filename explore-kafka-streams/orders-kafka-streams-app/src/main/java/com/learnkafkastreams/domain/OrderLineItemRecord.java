package com.learnkafkastreams.domain;

import java.math.BigDecimal;

public record OrderLineItemRecord(
        String item,
        Integer count,
        BigDecimal amount) {
}
