package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.charset.StandardCharsets;

@Slf4j
public class JsonSerializer<T> implements Serializer<T> {

  private final ObjectMapper objectMapper = new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

  @Override
  public byte[] serialize(String topic, T type) {
    try {
      return objectMapper.writeValueAsString(type).getBytes(StandardCharsets.UTF_8);
    } catch (JsonProcessingException e) {
      log.error("JsonProcessingException Serializing to JSON : {} ", e.getMessage(), e);
      throw new RuntimeException(e);
    }catch (Exception e){
      log.error("Exception Serializing, Message is  {}  ", e.getMessage(), e);
      throw e;
    }
  }

  @Override
  public void close() {}
}
