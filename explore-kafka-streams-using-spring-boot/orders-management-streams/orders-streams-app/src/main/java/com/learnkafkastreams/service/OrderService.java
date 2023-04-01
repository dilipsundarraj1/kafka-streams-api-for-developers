package com.learnkafkastreams.service;

import com.learnkafkastreams.client.OrdersServiceClient;
import com.learnkafkastreams.domain.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.net.InetAddress;
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
    MetaDataService metaDataService;

    @Value("${server.port}")
    private Integer port;

    OrdersServiceClient ordersServiceClient;

    public OrderService(OrderStoreService orderStoreService, MetaDataService metaDataService, OrdersServiceClient ordersServiceClient) {
        this.orderStoreService = orderStoreService;
        this.metaDataService = metaDataService;
        this.ordersServiceClient = ordersServiceClient;
    }

    public List<OrderCountPerStoreDTO> getOrdersCount(String orderType, String queryOtherHosts) {

        ReadOnlyKeyValueStore<String, Long> orderStore = getOrderStore(orderType);

        var orders = orderStore.all();
        var spliterator = Spliterators.spliteratorUnknownSize(orders, 0);
        var orderCountPerStoreDTOListCurrentInstance = StreamSupport.stream(spliterator, false)
                .map(keyValue ->
                        new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());
        log.info("orderCountPerStoreDTOListCurrentInstance : {}", orderCountPerStoreDTOListCurrentInstance);
        // fetch the data about other instances
        // make the restcall to get the data from other instance
        // make sure the other instance is not going to make any network calls to other instances
        // aggregate the data.

        var orderCountPerStoreDTOList = retrieveDataFromOtherInstances(orderType, Boolean.parseBoolean(queryOtherHosts));
        log.info("orderCountPerStoreDTOList : {}", orderCountPerStoreDTOList);
        return Stream.of(orderCountPerStoreDTOListCurrentInstance
                        , orderCountPerStoreDTOList
                )
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    private List<OrderCountPerStoreDTO> retrieveDataFromOtherInstances(String orderType, Boolean queryOtherHosts) {
        var otherHosts = otherHosts();
        log.info("otherHosts : {}, queryOtherHosts: {}  ", otherHosts, queryOtherHosts);
        if (queryOtherHosts && otherHosts != null) {
            return otherHosts
                    .stream()
                    .map(hostInfoDTO -> ordersServiceClient.retrieveOrdersCountByOrderType(hostInfoDTO, orderType))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        }
        return null;
    }

    private List<HostInfoDTO> otherHosts() {
        var hostInfoMetaData = metaDataService.getStreamsMetaData();
        try {
            //var currentMachinesHost = InetAddress.getLocalHost().getHostName();
            var currentMachineAddress = InetAddress.getLocalHost().getHostAddress();
            // var currentMachineAddress = "localhost";
            return hostInfoMetaData
                    .stream().filter(hostInfoDTO ->
                            !(Objects.equals(hostInfoDTO.host(), currentMachineAddress) && hostInfoDTO.port() == port))
                    .toList();
        } catch (Exception e) {
            log.error("Exception in otherHosts : {} ", e.getMessage(), e);
        }
        return null;

    }

    public List<OrderCountPerStoreDTO> buildRecordsFromStore(KeyValueIterator<String, Long> orderStore) {

        var spliterator = Spliterators.spliteratorUnknownSize(orderStore, 0);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue ->
                        new OrderCountPerStoreDTO(keyValue.key, keyValue.value))
                .collect(Collectors.toList());

    }

    public OrderCountPerStoreDTO getOrdersCountByLocationId(String orderType, String locationId) {
        var storeName = mapOrderCountStoreName(orderType);
        var hostMetaData = metaDataService.getStreamsMetaDataForLocationId(storeName, locationId);
        log.info("hostMetaData : {} ", hostMetaData);
        if (hostMetaData != null) {
            if (hostMetaData.port() == port) {
                log.info("Fetching the data from the current instance");
                ReadOnlyKeyValueStore<String, Long> orderStore = getOrderStore(orderType);
                var orderCount = orderStore.get(locationId);
                if (orderCount != null) {
                    return new OrderCountPerStoreDTO(locationId, orderCount);
                } else {
                    return null;
                }
            } else {
                log.info("Fetching the data from the remote instance");
                var orderCountPerStoreDTOList = ordersServiceClient.retrieveOrdersCountByOrderTypeAndLocationId(
                        new HostInfoDTO(hostMetaData.host(), hostMetaData.port()),
                        orderType, locationId);
                if (!CollectionUtils.isEmpty(orderCountPerStoreDTOList)) {
                    return orderCountPerStoreDTOList.get(0);
                }
                return null;

            }
        }

        return null;

    }

    public static String mapOrderCountStoreName(String orderType) {
        return switch (orderType) {
            case GENERAL_ORDERS -> GENERAL_ORDERS_COUNT;
            case RESTAURANT_ORDERS ->RESTAURANT_ORDERS_COUNT;
            default -> throw new IllegalStateException("Not a Valid Option");
        };
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
                getOrdersCount(GENERAL_ORDERS, "true")
                        .stream()
                        .map(orderCountPerStoreDTO -> mapper.apply(orderCountPerStoreDTO, OrderType.GENERAL))
                        .collect(Collectors.toList());

        var restaurantOrdersCount = getOrdersCount(RESTAURANT_ORDERS, "true")
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

        var revenueStoreByType = getRevenueStore(orderType);

        var revenueIterator = revenueStoreByType.all();
        var spliterator = Spliterators.spliteratorUnknownSize(revenueIterator, 0);
        return StreamSupport.stream(spliterator, false)
                .map(keyValue ->
                        new OrderRevenueDTO(keyValue.key, mapOrderType(orderType), keyValue.value))
                .collect(Collectors.toList());

    }

    public OrderRevenueDTO getRevenueByLocationId(String orderType, String locationId) {
        var revenueStoreByType = getRevenueStore(orderType);

        var totalRevenue = revenueStoreByType.get(locationId);
        if (totalRevenue != null) {
            return new OrderRevenueDTO(locationId, mapOrderType(orderType), totalRevenue);
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

        var generalOrdersRevenue = revenueByOrderType(GENERAL_ORDERS);
        var restaurantOrdersRevenue = revenueByOrderType(RESTAURANT_ORDERS);

        return Stream.of(generalOrdersRevenue, restaurantOrdersRevenue)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

    }


}
