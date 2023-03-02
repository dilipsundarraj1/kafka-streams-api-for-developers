package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.*;
import com.learnkafkastreams.service.OrderService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

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
    public List<AllOrdersCountPerStoreDTO> allOrdersCount(
    ) {
        return orderService.getAllOrdersCount();

    }

    @GetMapping("/revenue/{order_type}")
    public ResponseEntity<?> revenueByOrderType(
            @PathVariable("order_type") String orderType,
            @RequestParam(value = "location_id", required = false) String locationId
    ) {
        if (StringUtils.hasLength(locationId)) {
            return ResponseEntity.ok(orderService.getRevenueByLocationId(orderType, locationId));
        } else {

            return ResponseEntity.ok(orderService.revenueByOrderType(orderType));

        }
    }

    @GetMapping("/revenue")
    public List<OrderRevenueDTO> allRevenue() {
        return orderService.allRevenue();
    }



}
