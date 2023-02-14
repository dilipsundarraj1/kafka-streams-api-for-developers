package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.AllOrdersCountPerStore;
import com.learnkafkastreams.domain.AllOrdersCountPerStoreByWindows;
import com.learnkafkastreams.domain.OrderType;
import com.learnkafkastreams.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/v1/orders")
@Slf4j
public class OrdersController {

    private OrderService orderService;

    public OrdersController(OrderService orderService) {
        this.orderService = orderService;
    }

    @GetMapping("/count/{order_type}")
    public ResponseEntity<?> ordersCount(
            @PathVariable("order_type") String orderType,
            @RequestParam(value = "location_id", required = false) String locationId
    ) {
        if (StringUtils.hasLength(locationId)) {
            return ResponseEntity.ok(orderService.getOrdersCountByLocationId(orderType, locationId));
        } else {

            return ResponseEntity.ok(orderService.getOrdersCount(orderType));

        }
    }

    @GetMapping("/count")
    public List<AllOrdersCountPerStore> allOrdersCount(
    ) {
        return orderService.getAllOrdersCount();

    }

    @GetMapping("/windows")
    public List<AllOrdersCountPerStoreByWindows> getAllOrdersCountByWindows(
            @RequestParam(value = "from_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime fromTime,
            @RequestParam(value = "to_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime toTime
    ) {
        log.info("fromTime : {} , toTime : {}", fromTime, toTime);
        if(fromTime!=null && toTime!=null){
            return orderService.getAllOrdersCountByWindows(fromTime, toTime);
        }
        return orderService.getAllOrdersCountByWindows();

    }

    @GetMapping("/windows/{window_order_type}")
    public List<AllOrdersCountPerStoreByWindows> getAllOrdersCountByWindowsType(
            @PathVariable("window_order_type") String orderType
    ) {
        return orderService.getAllOrdersCountWindowsByType(orderType, OrderType.RESTAURANT);

    }

}
