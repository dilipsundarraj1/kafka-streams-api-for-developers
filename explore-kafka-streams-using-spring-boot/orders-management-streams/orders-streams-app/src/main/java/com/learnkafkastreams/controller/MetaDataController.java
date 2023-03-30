package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.domain.HostInfoDTOWithKey;
import com.learnkafkastreams.service.MetaDataService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

import static com.learnkafkastreams.service.OrderService.mapOrderCountStoreName;

@RestController
@RequestMapping("/v1/metadata")
public class MetaDataController {

    private MetaDataService metaDataService;

    public MetaDataController(MetaDataService metaDataService) {
        this.metaDataService = metaDataService;
    }

    @GetMapping("/all")
    public List<HostInfoDTO> getStreamsMetaData(){
        return metaDataService.getStreamsMetaData();
    }

    @GetMapping("{order_type}/{location_id}")
    public HostInfoDTOWithKey getStreamsMetaDataForKey(@PathVariable("order_type") String orderType
            ,@PathVariable("location_id") String locationId){

        return metaDataService.getStreamsMetaDataForLocationId(mapOrderCountStoreName(orderType), locationId);
    }
}
