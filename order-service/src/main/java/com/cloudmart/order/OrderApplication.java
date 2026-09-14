package com.cloudmart.order;

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
public class OrderApplication {
    public static void main(String[] args) {
        SpringApplication.run(OrderApplication.class, args);
    }
}

@Entity
@Table(name = "customer_orders")
class OrderRecord {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private Long productId;
    private String username;
    private Integer quantity;
    private Double totalPrice;
    private String status; // PENDING, CONFIRMED, CANCELLED
    private LocalDateTime createdAt;

    public OrderRecord() {}
    public OrderRecord(Long productId, String username, Integer quantity, Double totalPrice) {
        this.productId = productId;
        this.username = username;
        this.quantity = quantity;
        this.totalPrice = totalPrice;
        this.status = "PENDING";
        this.createdAt = LocalDateTime.now();
    }

    public Long getId() { return id; }
    public Long getProductId() { return productId; }
    public String getUsername() { return username; }
    public Integer getQuantity() { return quantity; }
    public Double getTotalPrice() { return totalPrice; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public LocalDateTime getCreatedAt() { return createdAt; }
}

interface OrderRepository extends JpaRepository<OrderRecord, Long> {
    List<OrderRecord> findByUsernameOrderByCreatedAtDesc(String username);
}

@RestController
@RequestMapping("/api/orders")
class OrderController {
    private final OrderRepository repo;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public OrderController(OrderRepository repo, KafkaTemplate<String, Object> kafkaTemplate) {
        this.repo = repo;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping
    public List<OrderRecord> getAll() { return repo.findAll(); }

    @GetMapping("/user/{username}")
    public List<OrderRecord> getUserOrders(@PathVariable String username) {
        return repo.findByUsernameOrderByCreatedAtDesc(username);
    }

    @PostMapping
    public OrderRecord createOrder(@RequestBody OrderRecord order) {
        order.setStatus("PENDING");
        OrderRecord saved = repo.save(order);

        try {
            Map<String, Object> event = Map.of(
                    "orderId", saved.getId(),
                    "productId", saved.getProductId(),
                    "username", saved.getUsername(),
                    "quantity", saved.getQuantity(),
                    "totalPrice", saved.getTotalPrice(),
                    "timestamp", System.currentTimeMillis()
            );
            kafkaTemplate.send("order-created-events", String.valueOf(saved.getId()), objectMapper.writeValueAsString(event));
            System.out.println("[Saga Started] Order initiated with ID: " + saved.getId());
        } catch (Exception e) {
            e.printStackTrace();
        }

        return saved;
    }

    // Day 1-3: Saga Step 3 - Finalize Order on Payment Success
    @KafkaListener(topics = "payment-processed-events", groupId = "order-finalizer-group")
    public void handlePaymentSuccess(String payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            Long orderId = root.get("orderId").asLong();
            repo.findById(orderId).ifPresent(o -> {
                o.setStatus("CONFIRMED");
                repo.save(o);
                System.out.println("[Saga Completed SUCCESS] Order " + orderId + " is CONFIRMED");
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Day 4-5: Saga Rollback - Cancel Order if Inventory or Payment Fails
    @KafkaListener(topics = {"inventory-failed-events", "payment-failed-events"}, groupId = "order-cancellation-group")
    public void handleSagaFailure(String payload) {
        try {
            JsonNode root = objectMapper.readTree(payload);
            Long orderId = root.get("orderId").asLong();
            String reason = root.has("reason") ? root.get("reason").asText() : "Saga Transaction Failed";
            repo.findById(orderId).ifPresent(o -> {
                o.setStatus("CANCELLED");
                repo.save(o);
                System.err.println("[Saga Rolled Back] Order " + orderId + " CANCELLED. Reason: " + reason);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
