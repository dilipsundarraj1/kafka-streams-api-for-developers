package com.learnkafkastreams.domain;

import java.math.BigDecimal;

public record TotalRevenue(String locationId,
                           Integer runnuingOrderCount,
                           BigDecimal runningRevenue) {

    public TotalRevenue() {
        this("", 0, BigDecimal.valueOf(0.0));
    }

    public TotalRevenue updateRunningRevenue(String key, Order order) {

        var newOrdersCount = this.runnuingOrderCount+1;
        var newRevenue = this.runningRevenue.add( order.finalAmount());
        return new TotalRevenue(key, newOrdersCount, newRevenue);

    }
}
