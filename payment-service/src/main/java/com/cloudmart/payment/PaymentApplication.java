package com.cloudmart.payment;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.web.bind.annotation.*;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.*;

@SpringBootApplication
@EnableDiscoveryClient
public class PaymentApplication {
    public static void main(String[] args) {
        SpringApplication.run(PaymentApplication.class, args);
    }
}

@Entity
@Table(name = "payment_transactions")
class Transaction {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long orderId;
    private Double amount;
    private String status;
    private String paymentMethod;
    private LocalDateTime timestamp;

    public Transaction() {}
    public Transaction(Long orderId, Double amount, String status, String paymentMethod) {
        this.orderId = orderId;
        this.amount = amount;
        this.status = status;
        this.paymentMethod = paymentMethod;
        this.timestamp = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public Long getOrderId() { return orderId; }
    public Double getAmount() { return amount; }
    public String getStatus() { return status; }
    public String getPaymentMethod() { return paymentMethod; }
    public LocalDateTime getTimestamp() { return timestamp; }
}

interface TransactionRepository extends JpaRepository<Transaction, Long> {}

@RestController
@RequestMapping("/api/payments")
class PaymentController {
    private final TransactionRepository repo;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public PaymentController(TransactionRepository repo, KafkaTemplate<String, Object> kafkaTemplate) {
        this.repo = repo;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping
    public List<Transaction> getAll() { return repo.findAll(); }

    // Day 1-3 & Day 4-5: Saga Step 2 - Charge payment or trigger compensation
    @KafkaListener(topics = "inventory-reserved-events", groupId = "payment-group")
    public void handleInventoryReserved(String eventPayload) {
        try {
            JsonNode root = objectMapper.readTree(eventPayload);
            Long orderId = root.get("orderId").asLong();
            Long productId = root.get("productId").asLong();
            int qty = root.get("quantity").asInt();
            Double amount = root.get("totalPrice").asDouble();

            // Simulation rule: Amount greater than 200,000 triggers payment failure to demonstrate rollback
            if (amount > 200000.0) {
                Transaction failedTxn = new Transaction(orderId, amount, "FAILED", "CARD");
                repo.save(failedTxn);

                Map<String, Object> failPayload = Map.of(
                        "orderId", orderId,
                        "productId", productId,
                        "quantity", qty,
                        "reason", "Transaction limit exceeded"
                );
                kafkaTemplate.send("payment-failed-events", String.valueOf(orderId), objectMapper.writeValueAsString(failPayload));
                System.err.println("[Saga Step 2 FAILED] Payment rejected for Order: " + orderId);
            } else {
                Transaction txn = new Transaction(orderId, amount, "SUCCESS", "CARD");
                Transaction saved = repo.save(txn);

                Map<String, Object> successPayload = Map.of(
                        "orderId", orderId,
                        "paymentId", saved.getId(),
                        "status", "SUCCESS"
                );
                kafkaTemplate.send("payment-processed-events", String.valueOf(orderId), objectMapper.writeValueAsString(successPayload));
                System.out.println("[Saga Step 2 SUCCESS] Payment processed for Order: " + orderId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
