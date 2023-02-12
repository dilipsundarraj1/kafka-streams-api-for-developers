package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.AllOrdersCountPerStore;
import com.learnkafkastreams.domain.AllOrdersCountPerStoreByWindows;
import com.learnkafkastreams.domain.OrderCountPerStore;
import com.learnkafkastreams.domain.OrderType;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.stereotype.Service;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Spliterators;
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

        var ordersCountByWindows = orderStoreService
               // .ordersWindowCountStore(GENERAL_ORDERS_COUNT_WINDOWS)
                .ordersWindowCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS)
                .all();

        var spliterator = Spliterators.spliteratorUnknownSize(ordersCountByWindows, 0);

//        return StreamSupport.stream(spliterator, false)
//                .map(keyValue ->
//                        new OrderCountPerStore(keyValue.key, keyValue.value))
//                .collect(Collectors.toList());

     var generalOrdersByWindows =    StreamSupport.stream(spliterator, false)
                .map(windowedLongKeyValue -> {
                    log.info("Start time : {}  , endTime : {}" , windowedLongKeyValue.key.window().startTime(),
                            windowedLongKeyValue.key.window().endTime());
                   return new AllOrdersCountPerStoreByWindows(
                            windowedLongKeyValue.key.key(),
                            windowedLongKeyValue.value,
                            OrderType.GENERAL,
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().startTime(),
                                    ZoneId.of(ZoneId.SHORT_IDS.get("GMT"))),
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().endTime(),
                                    ZoneId.of(ZoneId.SHORT_IDS.get("GMT")))

                    );
                })
             .collect(Collectors.toList());

        return generalOrdersByWindows;


    }

    public List<AllOrdersCountPerStoreByWindows> getAllOrdersCountWindowsByType(String orderType) {
        var ordersCountByWindows = orderStoreService
                .ordersWindowCountStore(orderType)
                .all();
        var spliterator = Spliterators.spliteratorUnknownSize(ordersCountByWindows, 0);

        var generalOrdersByWindows =    StreamSupport.stream(spliterator, false)
                .map(windowedLongKeyValue -> {
                    log.info("Start time : {}  , endTime : {}" , windowedLongKeyValue.key.window().startTime(),
                            windowedLongKeyValue.key.window().endTime());
                    return new AllOrdersCountPerStoreByWindows(
                            windowedLongKeyValue.key.key(),
                            windowedLongKeyValue.value,
                            OrderType.GENERAL,
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().startTime(),
                                    ZoneId.of(ZoneId.SHORT_IDS.get("GMT"))),
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().endTime(),
                                    ZoneId.of(ZoneId.SHORT_IDS.get("GMT")))

                    );
                })
                .toList();

        return generalOrdersByWindows;

    }
}
