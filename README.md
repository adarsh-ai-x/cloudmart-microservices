# CloudMart Microservices Platform

An event-driven distributed e-commerce architecture built with Spring Boot, Apache Kafka, and Saga Choreography.

---

## System Architecture & Live Verification

### 1. Multi-Container Orchestration (Docker Desktop)
All microservices, databases, and message brokers running in containerized environments:
![Docker Containers](screenshots/docker-containers.png)

### 2. Service Discovery (Eureka Registry)
Core distributed services registered and verified active on Netflix Eureka Server:
![Eureka Dashboard](screenshots/eureka-dashboard.png)

### 3. API Execution & Order Creation (Restfox)
Submitting order requests and validating asynchronous processing:
![Restfox API](screenshots/restfox-order-flow.png)

### 4. Distributed Transaction Management (Saga Pattern)
Live database state confirming successful order flows and automated compensating transactions (Cancelled status) on limit violations:
![Order Lifecycle](screenshots/order-lifecycle-status.png)
