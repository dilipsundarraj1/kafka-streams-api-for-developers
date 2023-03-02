package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.ZoneId;
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

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType) {

        ReadOnlyKeyValueStore<String, Long> orderStore = getOrderStore(orderType);

        var orders = orderStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue ->
                        new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());
    }

    public List<OrderCountPerStoreDTO> buildRecordsFromStore(KeyValueIterator<String, Long> orderStore) {

        var spliterator = Spliterators.spliteratorUnknownSize(orderStore, 0);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue ->
                        new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

    }

    public OrderCountPerStoreDTO getOrdersCountByLocationId(String orderType, String locationId) {

        ReadOnlyKeyValueStore<String, Long> orderStore = getOrderStore(orderType);
        var orderCount = orderStore.get(locationId);
        if (orderCount != null) {
            return new OrderCountPerStoreDTO(locationId, orderCount);
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

    private ReadOnlyKeyValueStore<String, TotalRevenue> getRevenueStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersRevenueWithAddressStore(GENERAL_ORDERS_REVENUE);
            case RESTAURANT_ORDERS -> orderStoreService.ordersRevenueWithAddressStore(RESTAURANT_ORDERS_REVENUE);
            default -> throw new IllegalStateException("Not a Valid Option");
        };
    }

    public List<AllOrdersCountPerStoreDTO> getAllOrdersCount() {

        BiFunction<OrderCountPerStoreDTO, OrderType, AllOrdersCountPerStoreDTO> mapper
                = (orderCountPerStoreDTO, orderType) -> new AllOrdersCountPerStoreDTO(orderCountPerStoreDTO.locationId(),
                orderCountPerStoreDTO.orderCount(), orderType);


        var generalOrdersCount =
                getOrdersCount(GENERAL_ORDERS)
                        .stream()
                        .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
                        .collect(Collectors.toList());

        var restaurantOrdersCount =   getOrdersCount(RESTAURANT_ORDERS)
                .stream()
                .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.RESTAURANT))
                .toList();

        generalOrdersCount
                .addAll(restaurantOrdersCount);
        return generalOrdersCount;

    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountWindowsByType(String storeName, OrderType orderType) {
        var ordersCountByWindows = orderStoreService
                .ordersWindowCountStore(storeName)
                .all();

        return mapToAllOrderCountPerStoreByWindows(ordersCountByWindows, orderType);
    }

    private static List<OrdersCountPerStoreByWindowsDTO> mapToAllOrderCountPerStoreByWindows(KeyValueIterator<Windowed<String>, Long> ordersCountByWindows, OrderType orderType) {
        var spliterator = Spliterators.spliteratorUnknownSize(ordersCountByWindows, 0);
        return StreamSupport.stream(spliterator, false)
                .map(windowedLongKeyValue -> {
                    printLocalDateTimes(windowedLongKeyValue.key, windowedLongKeyValue.value);
                    return new OrdersCountPerStoreByWindowsDTO(
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


    public List<OrderRevenueDTO> revenueByOrderType(String orderType) {

        var revenueStoreByType =getRevenueStore(orderType);

        var revenueIterator = revenueStoreByType.all();
        var spliterator = Spliterators.spliteratorUnknownSize(revenueIterator, 0);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue ->
                        new OrderRevenueDTO(keyValue.key, mapOrderType(orderType), keyValue.value))
                .collect(Collectors.toList());

    }

    public OrderRevenueDTO getRevenueByLocationId(String orderType, String locationId) {
        var revenueStoreByType =getRevenueStore(orderType);

        var totalRevenue = revenueStoreByType.get(locationId);
        if (totalRevenue != null) {
            return new OrderRevenueDTO(locationId,mapOrderType(orderType), totalRevenue);
        } else {
            return null;
        }
    }

    public static OrderType mapOrderType(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> OrderType.GENERAL;
            case RESTAURANT_ORDERS -> OrderType.RESTAURANT;
            default -> throw new IllegalStateException("Not a Valid Option");
        };
    }

    public List<OrderRevenueDTO> allRevenue() {

        var generalOrdersRevenue =revenueByOrderType(GENERAL_ORDERS);
        var restaurantOrdersRevenue =revenueByOrderType(RESTAURANT_ORDERS);

        return Stream.of(generalOrdersRevenue, restaurantOrdersRevenue)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }



}
