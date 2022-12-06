package com.learnkafkastreams.domain;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public record OrderRecord(Integer orderId,
                          String locationId,
                          BigDecimal finalAmount,
                          OrderType orderType,
                          List<OrderLineItemRecord> orderLineItems,
                          LocalDateTime orderedDateTime) {
}
