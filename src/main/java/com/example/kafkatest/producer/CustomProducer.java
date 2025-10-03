package com.example.kafkatest.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

@Component
public class CustomProducer {

    private Producer<String, String> producer;

    @PostConstruct
    public void configureProducer() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producer = new KafkaProducer<>(properties);
    }

    public Map<String, Object> createOrder() {
        Map<String, Object> order = new HashMap<>();
        order.put("orderId", UUID.randomUUID());
        order.put("user", "Dimitar Dobrev");
        order.put("item", "phone");
        order.put("quantity", 1);
        return order;
    }

    public String convertToJson(Map<String, Object> order) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.writeValueAsString(order);
    }

    public void produceMessage(String value) {
        ProducerRecord<String, String> message = new ProducerRecord<>("orders", value);

        producer.send(message, (metadata, exception) -> {
            if (exception != null) {
                System.out.println("Delivery failed" + exception.getMessage());
            } else {
                System.out.printf("Delivered %s%n", value);
                System.out.printf("Delivered to %s : partition %d : offset %d%n", metadata.topic(),
                        metadata.partition(), metadata.offset());
            }
        });

        producer.flush();
    }

    @PreDestroy
    public void closeProducer() {
        if (producer != null) {
            producer.close();
        }
    }

}
