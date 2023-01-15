package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import com.learnkafkastreams.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static com.learnkafkastreams.topology.GreetingsTopology.GREETINGS;
import static com.learnkafkastreams.topology.GreetingsTopology.GREETINGS_UPPERCASE;
import static org.junit.jupiter.api.Assertions.*;

class GreetingsTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, Greeting> inputTopic = null;
    TestOutputTopic<String, Greeting> outputTopic = null;
    static String INPUT_TOPIC = GREETINGS;
    static String OUTPUT_TOPIC = GREETINGS_UPPERCASE;


    @BeforeEach
    void setUp() {
        topologyTestDriver = new TopologyTestDriver(GreetingsTopology.buildTopology());

        inputTopic =
                topologyTestDriver.
                        createInputTopic(
                                INPUT_TOPIC, Serdes.String().serializer(), SerdesFactory.greetingSerdes().serializer());

        outputTopic =
                topologyTestDriver
                        .createOutputTopic(
                                OUTPUT_TOPIC,
                                Serdes.String().deserializer(),
                                SerdesFactory.greetingSerdes().deserializer());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
    }

    @Test
    void buildTopology() {

        inputTopic.pipeInput("GM", new Greeting("Good mrg!", LocalDateTime.now()));

        var output = outputTopic.getQueueSize();
        assertEquals(1, output);

        var outputValue = outputTopic.readKeyValue();
        assertEquals("GM",outputValue.key);
        assertEquals("GOOD MRG!",outputValue.value.getMessage());
        assertNotNull(outputValue.value.getTimeStamp());


    }

    @Test
    void buildTopology_multipleInput() {

        var greeting1 = KeyValue.pair( "GM", new Greeting("Good mrg!", LocalDateTime.now()));
        var greeting2 = KeyValue.pair( "GN", new Greeting("Good night!", LocalDateTime.now()));
        inputTopic.pipeKeyValueList(List.of(greeting1, greeting2));

        var output = outputTopic.getQueueSize();
        assertEquals(2, output);

        var outputValues = outputTopic.readKeyValuesToList();
        System.out.println("outputValues : " + outputValues);

        var firstGreeting = outputValues.get(0);
        assertEquals("GM",firstGreeting.key);
        assertEquals("GOOD MRG!",firstGreeting.value.getMessage());

        var secondGreeting = outputValues.get(1);
        assertEquals("GN",secondGreeting.key);
        assertEquals("GOOD NIGHT!",secondGreeting.value.getMessage());

    }

    @Test
    void buildTopology_Error() {

        inputTopic.pipeInput("GM", new Greeting("Transient Error", LocalDateTime.now()));

        var output = outputTopic.getQueueSize();
        assertEquals(0, output);

    }
}