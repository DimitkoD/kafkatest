package com.example.kafkatest;

import com.example.kafkatest.producer.CustomProducer;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

@SpringBootApplication
public class KafkaTestApplication implements CommandLineRunner {

    private final CustomProducer customProducer;

    public KafkaTestApplication(CustomProducer customProducer) {
        this.customProducer = customProducer;
    }

    static void main(String[] args) {
        SpringApplication.run(KafkaTestApplication.class, args);
    }

    @Override
    public void run(String... args) throws JsonProcessingException {
        Map<String, Object> order = customProducer.createOrder();
        String jsonOrder = customProducer.convertToJson(order);
        customProducer.produceMessage(jsonOrder);
    }

}
