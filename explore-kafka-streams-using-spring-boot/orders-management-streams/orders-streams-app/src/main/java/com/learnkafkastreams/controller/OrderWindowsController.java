package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.OrdersCountPerStoreByWindowsDTO;
import com.learnkafkastreams.domain.OrdersRevenuePerStoreByWindowsDTO;
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


    @GetMapping("/windows/count/{order_type}")
    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindowsType(
            @PathVariable("order_type") String orderType
    ) {
        return ordersWindowService.getOrdersCountWindowsByType(orderType);

    }

    @GetMapping("/windows/count")
    public List<OrdersCountPerStoreByWindowsDTO> getAllOrdersCountByWindows(
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

    @GetMapping("/windows/revenue/{order_type}")
    public List<OrdersRevenuePerStoreByWindowsDTO> getAllOrdersRevenueByWindowsType(
            @PathVariable("order_type") String orderType
    ) {
        return ordersWindowService.getOrdersRevenueWindowsByType(orderType);

    }



}
