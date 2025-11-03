package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaTransactionListener.class);

    // Reads topic from config and supplies a default group id if none is configured
    @KafkaListener(
            topics = "${general.kafka-topic}",
            groupId = "${spring.kafka.consumer.group-id:midas-core-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handle(Transaction tx) {
        // Do not assume getters that do not exist on the model
        log.info("Received transaction: {}", tx);
    }
}