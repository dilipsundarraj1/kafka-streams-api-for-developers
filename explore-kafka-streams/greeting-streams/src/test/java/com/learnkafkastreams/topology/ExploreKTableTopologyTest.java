package com.learnkafkastreams.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ExploreKTableTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, String> inputTopic = null;
    TestOutputTopic<String, String> outputTopic = null;

//    @BeforeEach
//    void setUp() {
//        topologyTestDriver = new TopologyTestDriver(ExploreKTableTopology.build());
//
//        inputTopic =
//                topologyTestDriver.
//                        createInputTopic(
//                                ExploreKTableTopology.WORDS, Serdes.String().serializer(),
//                                Serdes.String().serializer());
//
//        outputTopic =
//                topologyTestDriver
//                        .createOutputTopic(
//                                ExploreKTableTopology.WORDS_OUTPUT,
//                                Serdes.String().deserializer(),
//                                Serdes.String().deserializer());
//    }
//
//    @AfterEach
//    void tearDown() {
//        topologyTestDriver.close();
//    }



}
