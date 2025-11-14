package com.jpmc.midascore.kafka;

import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.model.TransactionRecord;
import com.jpmc.midascore.model.User;
import com.jpmc.midascore.repository.TransactionRecordRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class KafkaTransactionListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaTransactionListener.class);

    private final UserRepository userRepository;
    private final TransactionRecordRepository transactionRecordRepository;

    public KafkaTransactionListener(UserRepository userRepository, TransactionRecordRepository transactionRecordRepository) {
        this.userRepository = userRepository;
        this.transactionRecordRepository = transactionRecordRepository;
    }

    @KafkaListener(
            topics = "${general.kafkaTopic}",
            groupId = "${spring.kafka.consumer.group-id:midas-core-group}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void handle(Transaction tx) {
        log.info("Received transaction: {}", tx);

        User sender = userRepository.findById(tx.getSenderId()).orElse(null);
        User recipient = userRepository.findById(tx.getRecipientId()).orElse(null);

        if (sender == null || recipient == null) {
            log.warn("Invalid transaction: sender or recipient not found.");
            return;
        }

        if (sender.getBalance() < tx.getAmount()) {
            log.warn("Invalid transaction: insufficient balance for sender.");
            return;
        }

        // Process the transaction
        sender.setBalance(sender.getBalance() - tx.getAmount());
        recipient.setBalance(recipient.getBalance() + tx.getAmount());
        userRepository.save(sender);
        userRepository.save(recipient);

        TransactionRecord record = new TransactionRecord();
        record.setSender(sender);
        record.setRecipient(recipient);
        record.setAmount(tx.getAmount());
        transactionRecordRepository.save(record);

        log.info("Transaction processed successfully: {}", tx);
    }
}