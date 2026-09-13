# CloudMart Microservices Architecture

An event-driven distributed e-commerce backend built with Spring Boot, Apache Kafka, and Saga Choreography.

## Services Overview
- **Discovery Server**: Netflix Eureka (Port 8761)
- **API Gateway**: Spring Cloud Gateway (Port 8080)
- **Order Service**: Order creation & Saga lifecycle (Port 8083)
- **Inventory Service**: Stock reservation & rollback
- **Payment Service**: Payment validation & failure compensation

## Tech Stack
- Spring Boot, Spring Cloud
- Apache Kafka & Zookeeper
- PostgreSQL & MongoDB
- Docker & Docker Compose
