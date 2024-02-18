package com.risgaleva.kafkaconsumer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.risgaleva.kafkaconsumer.model.User;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class ConsumerService {


    private final ObjectMapper objectMapper;

    @Autowired
    public ConsumerService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "user_topic", groupId = "group_id")
    public void consume(String message) throws JsonProcessingException {
        processMessage(message);

    }

    public void processMessage(String message) throws JsonProcessingException {
        User user = objectMapper.readValue(message, User.class);
        if (user.getPosition().equals("middle")) {
            log.info("Работник с должностью middle: " + user.getName() + " и зарплатой " + user.getSalary());
        } else if (user.getPosition().equals("junior")) {
            log.info("Мифическое существо");
        } else if (user.getSalary() > 600000) {
            log.info("ВОТ ЭТО ОКЛАД: " + user.getSalary() + "!!! " + user.getName() + " явно devOps!");
        }
    }
}
