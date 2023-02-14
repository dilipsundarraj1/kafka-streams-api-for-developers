package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.AllOrdersCountPerStore;
import com.learnkafkastreams.domain.AllOrdersCountPerStoreByWindows;
import com.learnkafkastreams.domain.OrderCountPerStore;
import com.learnkafkastreams.domain.OrderType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.learnkafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrderService {
    OrderStoreService orderStoreService;

    public OrderService(OrderStoreService orderStoreService) {

        this.orderStoreService = orderStoreService;
    }

    public List<OrderCountPerStore> getOrdersCount(String orderType) {

        ReadOnlyKeyValueStore<String, Long> orderStore = getOrderStore(orderType);

        var orders = orderStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue ->
                        new OrderCountPerStore(keyValue.key, keyValue.value))
                .collect(Collectors.toList());
    }

    public List<OrderCountPerStore> buildRecordsFromStore(KeyValueIterator<String, Long> orderStore) {

        var spliterator = Spliterators.spliteratorUnknownSize(orderStore, 0);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue ->
                        new OrderCountPerStore(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

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

    public List<AllOrdersCountPerStore> getAllOrdersCount() {

        BiFunction<OrderCountPerStore, OrderType, AllOrdersCountPerStore> mapper
                = (orderCountPerStore, orderType) -> new AllOrdersCountPerStore(orderCountPerStore.locationId(),
                orderCountPerStore.orderCount(), orderType);


        var generalOrders = orderStoreService.ordersCountStore(GENERAL_ORDERS_COUNT);
        var restaurantOrders = orderStoreService.ordersCountStore(RESTAURANT_ORDERS_COUNT);
        var generalOrdersCount =
                buildRecordsFromStore(generalOrders.all())
                        .stream()
                        .map(orderCountPerStore -> mapper.apply(orderCountPerStore, OrderType.GENERAL))
                        .collect(Collectors.toList());

        var restaurantOrdersCount = buildRecordsFromStore(restaurantOrders.all())
                .stream()
                .map(orderCountPerStore -> mapper.apply(orderCountPerStore, OrderType.RESTAURANT))
                .toList();

        generalOrdersCount
                .addAll(restaurantOrdersCount);
        return generalOrdersCount;

    }

    public List<AllOrdersCountPerStoreByWindows> getAllOrdersCountByWindows() {

        var generalOrdersCountByWindows = getAllOrdersCountWindowsByType(GENERAL_ORDERS_COUNT_WINDOWS, OrderType.GENERAL);

        var restaurantOrdersCountByWindows = getAllOrdersCountWindowsByType(RESTAURANT_ORDERS_COUNT_WINDOWS, OrderType.RESTAURANT);

        return Stream.of(generalOrdersCountByWindows, restaurantOrdersCountByWindows)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<AllOrdersCountPerStoreByWindows> getAllOrdersCountWindowsByType(String storeName, OrderType orderType) {
        var ordersCountByWindows = orderStoreService
                .ordersWindowCountStore(storeName)
                .all();

        return mapToAllOrderCountPerStoreByWindows(ordersCountByWindows, orderType);
    }

    private static List<AllOrdersCountPerStoreByWindows> mapToAllOrderCountPerStoreByWindows(KeyValueIterator<Windowed<String>, Long> ordersCountByWindows, OrderType orderType) {
        var spliterator = Spliterators.spliteratorUnknownSize(ordersCountByWindows, 0);
        return StreamSupport.stream(spliterator, false)
                .map(windowedLongKeyValue -> {
                    printLocalDateTimes(windowedLongKeyValue.key, windowedLongKeyValue.value);
                    return new AllOrdersCountPerStoreByWindows(
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

    public List<AllOrdersCountPerStoreByWindows> getAllOrdersCountByWindows(LocalDateTime fromTime, LocalDateTime toTime) {

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
}
