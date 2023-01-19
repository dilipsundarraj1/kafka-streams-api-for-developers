package com.learnkafkastreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
//@EnableKafka
@EnableKafkaStreams
public class GreetingsStreamsSpringbootApplication {

	public static void main(String[] args) {
		SpringApplication.run(GreetingsStreamsSpringbootApplication.class, args);
	}

}
