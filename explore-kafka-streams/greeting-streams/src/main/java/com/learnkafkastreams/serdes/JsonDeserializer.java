package com.learnkafkastreams.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

@Slf4j
public class JsonDeserializer<T> implements Deserializer<T> {

  private final ObjectMapper objectMapper = new ObjectMapper()
          .registerModule(new JavaTimeModule())
          .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);

  private Class<T> destinationClass;

  public JsonDeserializer(Class<T> destinationClass) {
    this.destinationClass = destinationClass;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    Deserializer.super.configure(configs, isKey);
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }
    try {
      return objectMapper.readValue(new String(data, StandardCharsets.UTF_8), destinationClass);
    } catch (JsonProcessingException e) {
      log.error("JsonProcessingException Deserializing to {} : {} ", destinationClass, e.getMessage(), e);
      throw new RuntimeException(e);
    }catch (Exception e){
      log.error("Exception Deserializing to {} : {} ", destinationClass, e.getMessage(), e);
      throw e;
    }
  }

  @Override
  public T deserialize(String topic, Headers headers, byte[] data) {
    return Deserializer.super.deserialize(topic, headers, data);
  }

  @Override
  public void close() {
    Deserializer.super.close();
  }
}
