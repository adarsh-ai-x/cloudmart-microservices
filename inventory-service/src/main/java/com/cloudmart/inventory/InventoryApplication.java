package com.cloudmart.inventory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.web.bind.annotation.*;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import jakarta.persistence.*;
import java.util.*;

@SpringBootApplication
@EnableDiscoveryClient
public class InventoryApplication {
    public static void main(String[] args) {
        SpringApplication.run(InventoryApplication.class, args);
    }

    @Bean
    CommandLineRunner initInventory(ProductRepository repo) {
        return args -> {
            if (repo.count() == 0) {
                repo.save(new Product("Gaming Laptop", "RTX 4060, 16GB RAM", 75000.0, 10));
                repo.save(new Product("Mechanical Keyboard", "RGB Custom Switches", 3500.0, 25));
                repo.save(new Product("Wireless Mouse", "16000 DPI Optical Sensor", 1800.0, 40));
            }
        };
    }
}

@Entity
@Table(name = "inventory_products")
class Product {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String name;
    private String description;
    private Double price;
    private Integer stock;

    public Product() {}
    public Product(String name, String description, Double price, Integer stock) {
        this.name = name;
        this.description = description;
        this.price = price;
        this.stock = stock;
    }

    public Long getId() { return id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }
    public Double getPrice() { return price; }
    public void setPrice(Double price) { this.price = price; }
    public Integer getStock() { return stock; }
    public void setStock(Integer stock) { this.stock = stock; }
}

interface ProductRepository extends JpaRepository<Product, Long> {}

@RestController
@RequestMapping("/api/inventory")
class InventoryController {
    private final ProductRepository repo;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public InventoryController(ProductRepository repo, KafkaTemplate<String, Object> kafkaTemplate) {
        this.repo = repo;
        this.kafkaTemplate = kafkaTemplate;
    }

    @GetMapping("/products")
    public List<Product> getAll() { return repo.findAll(); }

    @GetMapping("/products/{id}")
    public ResponseEntity<Product> getById(@PathVariable Long id) {
        return repo.findById(id).map(ResponseEntity::ok).orElse(ResponseEntity.notFound().build());
    }

    // Direct Synchronous Deduction Endpoint (Verification Fallback)
    @PostMapping("/products/{id}/deduct")
    public ResponseEntity<?> deductStock(@PathVariable Long id, @RequestParam(defaultValue = "1") int qty) {
        return repo.findById(id).map(p -> {
            if (p.getStock() >= qty) {
                p.setStock(p.getStock() - qty);
                return ResponseEntity.ok(repo.save(p));
            }
            return ResponseEntity.badRequest().body("Insufficient stock");
        }).orElse(ResponseEntity.notFound().build());
    }

    // Day 1-3: Saga Step 1 - Deduct Inventory or Emit Failure
    @KafkaListener(topics = "order-created-events", groupId = "inventory-group")
    public void handleOrderCreated(String eventPayload) {
        try {
            JsonNode root = objectMapper.readTree(eventPayload);
            Long orderId = root.get("orderId").asLong();
            Long productId = root.get("productId").asLong();
            int qty = root.get("quantity").asInt();
            double totalPrice = root.get("totalPrice").asDouble();

            Optional<Product> prodOpt = repo.findById(productId);
            if (prodOpt.isPresent() && prodOpt.get().getStock() >= qty) {
                Product p = prodOpt.get();
                p.setStock(p.getStock() - qty);
                repo.save(p);

                Map<String, Object> payload = Map.of(
                        "orderId", orderId,
                        "productId", productId,
                        "quantity", qty,
                        "totalPrice", totalPrice
                );
                kafkaTemplate.send("inventory-reserved-events", String.valueOf(orderId), objectMapper.writeValueAsString(payload));
            } else {
                Map<String, Object> failurePayload = Map.of(
                        "orderId", orderId,
                        "productId", productId,
                        "reason", "Insufficient stock"
                );
                kafkaTemplate.send("inventory-failed-events", String.valueOf(orderId), objectMapper.writeValueAsString(failurePayload));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Day 4-5: Compensating Transaction
    @KafkaListener(topics = "payment-failed-events", groupId = "inventory-rollback-group")
    public void handlePaymentFailed(String eventPayload) {
        try {
            JsonNode root = objectMapper.readTree(eventPayload);
            Long orderId = root.get("orderId").asLong();
            Long productId = root.get("productId").asLong();
            int qty = root.get("quantity").asInt();

            repo.findById(productId).ifPresent(p -> {
                p.setStock(p.getStock() + qty);
                repo.save(p);
            });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}