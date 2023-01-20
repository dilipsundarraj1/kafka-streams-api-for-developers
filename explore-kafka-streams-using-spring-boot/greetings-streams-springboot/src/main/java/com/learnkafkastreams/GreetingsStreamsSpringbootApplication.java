package com.learnkafkastreams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@Slf4j
@SpringBootApplication
@EnableKafkaStreams
public class GreetingsStreamsSpringbootApplication {

	public static void main(String[] args) {
		log.info("String Serde : {} ", Serdes.StringSerde.class );
		SpringApplication.run(GreetingsStreamsSpringbootApplication.class, args);

	}

}
