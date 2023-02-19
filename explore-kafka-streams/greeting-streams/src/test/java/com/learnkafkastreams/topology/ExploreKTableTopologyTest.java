package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static com.learnkafkastreams.topology.GreetingsTopology.GREETINGS;
import static com.learnkafkastreams.topology.GreetingsTopology.GREETINGS_UPPERCASE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class ExploreKTableTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, String> inputTopic = null;
    TestOutputTopic<String, String> outputTopic = null;

    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(ExploreKTableTopology.build());

        inputTopic =
                topologyTestDriver.
                        createInputTopic(
                                ExploreKTableTopology.WORDS, Serdes.String().serializer(),
                                Serdes.String().serializer());

        outputTopic =
                topologyTestDriver
                        .createOutputTopic(
                                ExploreKTableTopology.WORDS_OUTPUT,
                                Serdes.String().deserializer(),
                                Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void buildTopology() {

        inputTopic.pipeInput("A", "Apple");
        inputTopic.pipeInput("B", "Baby");
        inputTopic.pipeInput("A", "Airplane");
        inputTopic.pipeInput("B", "Bus");

        var output = outputTopic.getQueueSize();
        assertEquals(4, output);

        var outputValueList = outputTopic.readKeyValuesToList();
        System.out.println("outputValueList : " + outputValueList );


    }
}
