package com.learnkafkastreams.topology;

import com.learnkafkastreams.domain.Greeting;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;

class GreetingsTopologyTest {

    TopologyTestDriver topologyTestDriver = null;
    TestInputTopic<String, Greeting> inputTopic = null;
    TestOutputTopic<String, Greeting> outputTopic = null;


}