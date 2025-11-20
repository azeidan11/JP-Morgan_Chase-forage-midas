## JP Morgan Chase Software Engineering - Midas Financial Platform

This project implements the backend core of J.P. Morgan’s Midas financial platform, completed as part of the Forage JPMC Software Engineering Virtual Experience.  
It showcases real enterprise concepts including Kafka message processing, Spring Boot microservices, H2 database persistence, and transaction validation logic.

---

# Project Overview

Midas Core is a Spring Boot application that:

- Receives incoming transaction messages through Apache Kafka  
- Validates sender and recipient accounts  
- Performs balance checks  
- Records valid transactions into a relational database  
- Updates user balances  
- Stores transaction history in persistent entities  

This backend represents the financial ledger layer behind the Midas system.

---

# Kafka Transaction Flow

1. Frontend publishes transaction messages to topic `midas.transactions`  
2. `KafkaTransactionListener` receives each message  
3. Deserializes JSON into a `Transaction` object  
4. Validates:  
   - Sender exists  
   - Recipient exists  
   - Sender balance is sufficient  
5. If valid:  
   - Debit sender  
   - Credit recipient  
   - Save a `TransactionRecord` to the database  
6. If invalid:  
   - Log and ignore  

---

# Running the Project

### Start the Application
```bash
./mvnw spring-boot:run

Run Tests:
./mvnw test

 License

#This project was completed as part of the JPMorgan Chase & Co. Forage Software Engineering Simulation.
Code is modified from starter templates provided by JPMC and Forage.

