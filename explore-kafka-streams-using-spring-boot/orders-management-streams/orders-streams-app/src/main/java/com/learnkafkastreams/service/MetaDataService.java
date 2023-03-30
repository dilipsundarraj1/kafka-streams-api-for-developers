package com.learnkafkastreams.service;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.domain.HostInfoDTOWithKey;
import org.apache.catalina.Host;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.learnkafkastreams.topology.OrdersTopology.GENERAL_ORDERS_COUNT;

@Service
public class MetaDataService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;

    public MetaDataService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public List<HostInfoDTO> getStreamsMetaData(){
        return Objects.requireNonNull(streamsBuilderFactoryBean
                        .getKafkaStreams())
                .metadataForAllStreamsClients()
                .stream().
                map(streamsMetadata -> {
                    var hostInfo = streamsMetadata.hostInfo();
                    return  new HostInfoDTO(hostInfo.host(), hostInfo.port());
                })
                .collect(Collectors.toList());

    }

    public HostInfoDTOWithKey getStreamsMetaDataForLocationId(String storeName, String locationId){
        var metaDataForKey= Objects.requireNonNull(streamsBuilderFactoryBean.getKafkaStreams())
                .queryMetadataForKey(storeName, locationId, Serdes.String().serializer());

        if(metaDataForKey!=null){
            var activeHost = metaDataForKey.activeHost();
            return new HostInfoDTOWithKey(activeHost.host(), activeHost.port(), locationId);
        }else{
            return null;
        }

    }
}
