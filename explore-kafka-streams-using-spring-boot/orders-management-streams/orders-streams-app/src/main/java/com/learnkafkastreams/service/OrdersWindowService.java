package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.List;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrdersWindowService {

    private OrderStoreService orderStoreService;

    public OrdersWindowService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrdersCountPerStoreByWindows> getAllOrdersCountByWindows() {

        var generalOrdersCountByWindows = getAllOrdersCountWindowsByType(GENERAL_ORDERS_COUNT_WINDOWS, OrderType.GENERAL);

        var restaurantOrdersCountByWindows = getAllOrdersCountWindowsByType(RESTAURANT_ORDERS_COUNT_WINDOWS, OrderType.RESTAURANT);

        return Stream.of(generalOrdersCountByWindows, restaurantOrdersCountByWindows)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrdersCountPerStoreByWindows> getAllOrdersCountByWindows(LocalDateTime fromTime, LocalDateTime toTime) {

        var fromTimeInstant = fromTime.toInstant(ZoneOffset.UTC);
        var toTimeInstant = toTime.toInstant(ZoneOffset.UTC);

        log.info("fromTimeInstant : {} , toTimeInstant : {} ", fromTimeInstant, toTimeInstant);

        var generalOrdersCountByWindows = orderStoreService
                .ordersWindowCountStore(GENERAL_ORDERS_COUNT_WINDOWS)
                .fetchAll(fromTimeInstant, toTimeInstant);

        var generalAllOrderCountPerStoreByWindows = mapToAllOrderCountPerStoreByWindows(generalOrdersCountByWindows, OrderType.GENERAL);

        var restaurantOrdersCountByWindows = orderStoreService
                .ordersWindowCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS)
                .fetchAll(fromTimeInstant, toTimeInstant);
        var restaurantOrderCountPerStoreByWindows = mapToAllOrderCountPerStoreByWindows(restaurantOrdersCountByWindows, OrderType.RESTAURANT);

        return Stream.of(generalAllOrderCountPerStoreByWindows, restaurantOrderCountPerStoreByWindows)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());


    }

    public List<OrdersCountPerStoreByWindows> getAllOrdersCountWindowsByType(String storeName, OrderType orderType) {
        var ordersCountByWindows = orderStoreService
                .ordersWindowCountStore(storeName)
                .all();

        return mapToAllOrderCountPerStoreByWindows(ordersCountByWindows, orderType);
    }

    private static List<OrdersCountPerStoreByWindows> mapToAllOrderCountPerStoreByWindows(KeyValueIterator<Windowed<String>, Long> ordersCountByWindows, OrderType orderType) {
        var spliterator = Spliterators.spliteratorUnknownSize(ordersCountByWindows, 0);
        return StreamSupport.stream(spliterator, false)
                .map(windowedLongKeyValue -> {
                    printLocalDateTimes(windowedLongKeyValue.key, windowedLongKeyValue.value);
                    return new OrdersCountPerStoreByWindows(
                            windowedLongKeyValue.key.key(),
                            windowedLongKeyValue.value,
                            orderType,
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().startTime(),
                                    ZoneId.of("GMT")),
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().endTime(),
                                    ZoneId.of("GMT"))

                    );
                })
                .toList();
    }

    public List<OrdersCountPerStoreByWindows> getAllOrdersCountWindowsByType(String orderType) {

        return getAllOrdersCountWindowsByType(mapOrderCountType(orderType), mapOrderType(orderType));
    }

    private OrderType mapOrderType(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Not a Valid Option");
        };
    }

    private String  mapOrderCountType(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> GENERAL_ORDERS_COUNT_WINDOWS;
            case RESTAURANT_ORDERS -> RESTAURANT_ORDERS_COUNT_WINDOWS;
            default -> throw new IllegalStateException("Not a Valid Option");
        };
    }


    public ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType){

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersWindowCountStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowCountStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Not a Valid Option");
        };
    }

}
