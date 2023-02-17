package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
import com.learnkafkastreams.domain.TotalRevenue;
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

import static com.learnkafkastreams.service.OrderService.mapOrderType;
import static com.learnkafkastreams.topology.OrdersTopology.*;

@Service
@Slf4j
public class OrdersWindowService {

    private OrderStoreService orderStoreService;

    public OrdersWindowService(OrderStoreService orderStoreService) {
        this.orderStoreService = orderStoreService;
    }

    public List<OrdersCountPerStoreByWindowsDTO> getOrdersCountWindowsByType(String orderType) {

        var countWindowsStore = getCountWindowsStore(orderType);

        var orderTypeEnum = mapOrderType(orderType);

        var countWindowsIterator = countWindowsStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(countWindowsIterator, 0);
        return StreamSupport.stream(spliterator, false)
                .map(windowedLongKeyValue -> {
                    printLocalDateTimes(windowedLongKeyValue.key, windowedLongKeyValue.value);
                    return new OrdersCountPerStoreByWindowsDTO(
                            windowedLongKeyValue.key.key(),
                            windowedLongKeyValue.value,
                            orderTypeEnum,
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().startTime(),
                                    ZoneId.of("GMT")),
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().endTime(),
                                    ZoneId.of("GMT"))

                    );
                })
                .toList();

    }

    public ReadOnlyWindowStore<String, Long> getCountWindowsStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersWindowCountStore(GENERAL_ORDERS_COUNT_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowCountStore(RESTAURANT_ORDERS_COUNT_WINDOWS);
            default -> throw new IllegalStateException("Not a Valid Option");
        };
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows() {

        var generalOrdersCountByWindows = getOrdersCountWindowsByType(GENERAL_ORDERS);

        var restaurantOrdersCountByWindows = getOrdersCountWindowsByType(RESTAURANT_ORDERS);

        return Stream.of(generalOrdersCountByWindows, restaurantOrdersCountByWindows)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows(LocalDateTime fromTime, LocalDateTime toTime) {

        var fromTimeInstant = fromTime.toInstant(ZoneOffset.UTC);
        var toTimeInstant = toTime.toInstant(ZoneOffset.UTC);

        log.info("fromTimeInstant : {} , toTimeInstant : {} ", fromTimeInstant, toTimeInstant);

        var generalOrdersCountByWindows = getCountWindowsStore(GENERAL_ORDERS)
                .fetchAll(fromTimeInstant, toTimeInstant)
                //.backwardAll() //This is to send the results in the reverse order
                ;

        var generalOrdersCountByWindowsDTO = mapToOrderCountPerStoreByWindows(generalOrdersCountByWindows, OrderType.GENERAL);

        var restaurantOrdersCountByWindowsDTO = getCountWindowsStore(RESTAURANT_ORDERS)
                .fetchAll(fromTimeInstant, toTimeInstant);

        var restaurantOrderCountPerStoreByWindows = mapToOrderCountPerStoreByWindows(restaurantOrdersCountByWindowsDTO, OrderType.RESTAURANT);

        return Stream.of(generalOrdersCountByWindowsDTO, restaurantOrderCountPerStoreByWindows)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());


    }


    private static List<OrdersCountPerStoreByWindowsDTO> mapToOrderCountPerStoreByWindows(KeyValueIterator<Windowed<String>, Long> ordersCountByWindows, OrderType orderType) {
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


    public List<OrdersRevenuePerStoreByWindowsDTO> getOrdersRevenueWindowsByType(String orderType) {

        var revenueWindowsStore = getRevenueWindowsStore(orderType);

        var orderTypeEnum = mapOrderType(orderType);

        var revenueWindowsIterator = revenueWindowsStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(revenueWindowsIterator, 0);
        return StreamSupport.stream(spliterator, false)
                .map(windowedLongKeyValue -> {
                    printLocalDateTimes(windowedLongKeyValue.key, windowedLongKeyValue.value);
                    return new OrdersRevenuePerStoreByWindowsDTO(
                            windowedLongKeyValue.key.key(),
                            windowedLongKeyValue.value,
                            orderTypeEnum,
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().startTime(),
                                    ZoneId.of("GMT")),
                            LocalDateTime.ofInstant(windowedLongKeyValue.key.window().endTime(),
                                    ZoneId.of("GMT"))

                    );
                })
                .toList();
    }

    public ReadOnlyWindowStore<String, TotalRevenue> getRevenueWindowsStore(String orderType) {

        return switch (orderType) {
            case GENERAL_ORDERS -> orderStoreService.ordersWindowRevenueStore(GENERAL_ORDERS_REVENUE_WINDOWS);
            case RESTAURANT_ORDERS -> orderStoreService.ordersWindowRevenueStore(RESTAURANT_ORDERS_REVENUE_WINDOWS);
            default -> throw new IllegalStateException("Not a Valid Option");
        };
    }
}
