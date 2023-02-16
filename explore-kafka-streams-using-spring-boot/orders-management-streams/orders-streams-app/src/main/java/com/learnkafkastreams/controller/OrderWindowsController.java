package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindows;
import com.learnkafkastreams.service.OrdersWindowService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping("/v1/orders")
@Slf4j
public class OrderWindowsController {

    private OrdersWindowService ordersWindowService;

    public OrderWindowsController(OrdersWindowService ordersWindowService) {
        this.ordersWindowService = ordersWindowService;
    }

    @GetMapping("/windows/count")
    public List<OrdersCountPerStoreByWindows> getAllOrdersCountByWindows(
            @RequestParam(value = "from_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime fromTime,
            @RequestParam(value = "to_time", required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME)
            LocalDateTime toTime
    ) {
        log.info("fromTime : {} , toTime : {}", fromTime, toTime);
        if (fromTime != null && toTime != null) {
            return ordersWindowService.getAllOrdersCountByWindows(fromTime, toTime);
        }
        return ordersWindowService.getAllOrdersCountByWindows();

    }

    @GetMapping("/windows/count/{window_order_type}")
    public List<OrdersCountPerStoreByWindows> getAllOrdersCountByWindowsType(
            @PathVariable("window_order_type") String orderType
    ) {
        return ordersWindowService.getAllOrdersCountWindowsByType(orderType);

    }

}
