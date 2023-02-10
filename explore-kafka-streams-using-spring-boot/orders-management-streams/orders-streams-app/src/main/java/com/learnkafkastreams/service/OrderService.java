package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.OrderCountPerStore;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.util.ArrayList;

import static com.learnkafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrderService {
    OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {

        this.orderStoreService = orderStoreService;
    }

    public ArrayList<OrderCountPerStore> getOrdersCount(String orderType) {

        ReadOnlyKeyValueStore<String, Long> orderStore = getOrderStore(orderType);

        var orderCountPerLocations = new ArrayList<OrderCountPerStore>();

        var orders = orderStore.all();

        while (orders.hasNext()) {
            var orderPerStore = orders.next();
            orderCountPerLocations.add(new OrderCountPerStore(orderPerStore.key, orderPerStore.value));

        }
        log.info("orderCountPerLocations : {}  ", orderCountPerLocations);
        return orderCountPerLocations;
    }

    public OrderCountPerStore getOrdersCountByLocationId(String orderType, String locationId) {

        ReadOnlyKeyValueStore<String, Long> orderStore = getOrderStore(orderType);
        var orderCount = orderStore.get(locationId);

        if (orderCount != null) {
            return new OrderCountPerStore(locationId, orderCount);
        } else {
            return null;
        }
    }

    private ReadOnlyKeyValueStore<String, Long> getOrderStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
            case RESTAURANT_ORDERS -> orderStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
            default -> throw new IllegalStateException("Not a Valid Option");
        };
    }
}
