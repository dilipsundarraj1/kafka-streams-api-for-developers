package com.learnkafkastreams.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class Order {
    private Integer orderId;
    private String locationId;
    private BigDecimal finalAmount;
    private OrderType orderType;
    private List<OrderLineItem> orderLineItems;
    private LocalDateTime orderedDateTime;

}
