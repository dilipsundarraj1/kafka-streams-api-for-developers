package com.learnkafkastreams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class OrdersManagementStreamsApplication {

	public static void main(String[] args) {
		SpringApplication.run(OrdersManagementStreamsApplication.class, args);
	}

}
